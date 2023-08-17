(ns jepsen.readyset
  (:gen-class)
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.tools.logging :refer [error info warn]]
   [jepsen.cli :as cli]
   [jepsen.consul.db :as consul.db]
   [jepsen.control :as c]
   [jepsen.control.net :as net]
   [jepsen.control.util :as cu]
   [jepsen.core :as jepsen]
   [jepsen.db :as db]
   [jepsen.os.debian :as debian]
   [jepsen.os.ubuntu :as ubuntu]
   [jepsen.readyset.client :as rs]
   [jepsen.tests :as tests]
   [slingshot.slingshot :refer [try+]]))

(def pguser "postgres")
(def pgpassword "password")
(def pgdatabase "jepsen")

(defn- node->role
  [test]
  (zipmap
   (:nodes test)
   (concat [:node-role/load-balancer
            :node-role/consul
            :node-role/readyset-server
            :node-role/upstream]
           (repeat :node-role/readyset-adapter))))

(defn role->node
  [test]
  (into {} (map (comp vec reverse)) (node->role test)))

(defn- node-role
  "Given a test and a node in that test, returns what will be running on that
  node for the test, represented as a keyword in
  `#{:node-role/consul
     :node-role/readyset-adapter
     :node-role/load-balancer
     :node-role/readyset-server
     :node-role/upstream}`.

  Note that we must always have at least 5 nodes to run a test.

  Roles will be assigned to nodes in the following order:

    1. `:node-role/load-balancer`
    2. `:node-role/consul`
    3. `:node-role/readyset-server`
    4. `:node-role/upstream`
    5. ... and all remaining nodes will have `:node-role/readyset-adapter`"
  [test node]
  (assert
   (>= (count (:nodes test)) 5)
   "Must have at least 5 nodes to run a high-availability ReadySet cluster")
  (get (node->role test) node))

(defn- node-with-role
  "Returns the node with the given role in the given test"
  [test role]
  (get (role->node test) role))

(defn- adapter-nodes
  "Returns a sequence of adapter nodes in the given test"
  [test]
  (->> test :nodes (drop 4)))

(defn- num-adapters
  "Returns the number of adapter instances that will be running in the given
  test"
  [test]
  (- (count (:nodes test)) 4))

(defn- upstream-db-url
  "Returns the upstream DB URL for the given test"
  [test]
  (str "postgresql://"
       pguser ":" pgpassword
       "@" (node-with-role test :node-role/upstream)
       "/" pgdatabase))

(defn- readyset-datasource
  "Build a JDBC DataSource for connecting to the ReadySet cluster in the given
  test"
  [test]
  (rs/make-datasource
   {:dbtype "postgres"
    :dbname pgdatabase
    :user pguser
    :password pgpassword
    :host (name (node-with-role test :node-role/load-balancer))
    :port 5432}))

(defn- ensure-git-cloned
  "Ensure that a git repository `repo` is cloned at ref `ref` in dir `dir`"
  [repo ref dir]
  (debian/install ["git"])

  (when (and (cu/exists? dir)
             (not (cu/exists? (str dir "/.git"))))
    (c/exec :rm :-rf dir))
  (letfn [(git [& args] (apply c/exec :git :-C dir args))]
    (if (cu/exists? dir)
      (git :fetch :origin)
      (c/exec :git :clone repo dir))
    (git :checkout ref)))

(defn- compile-and-install-readyset-binary
  [node ref bin & [{:keys [force?] :or {force? false}}]]
  (if (and
       (cu/file? (str "/usr/local/bin/" bin))
       (not force?))
    (info node bin "already exists, not re-installing")
    (c/su
     (debian/install ["clang"
                      "libclang-dev"
                      "libssl-dev"
                      "liblz4-dev"
                      "build-essential"
                      "pkg-config"])
     (c/exec* "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y")
     (ensure-git-cloned
      "https://github.com/readysettech/readyset.git"
      ref
      "/opt/readyset")
     (c/exec* "~/.cargo/bin/rustup install $(</opt/readyset/rust-toolchain)")
     (info node "compiling" bin)
     (c/cd "/opt/readyset"
           (c/exec "~/.cargo/bin/cargo" "build" "--release" "--bin" bin)
           (c/exec "mv"
                   (str "target/release/" bin)
                   "/usr/local/bin/")))))

(defn- authority-address
  [test]
  (str (name (node-with-role test :node-role/consul))
       ":8500"))

(defn- append-to-file [s file]
  (c/exec "echo" s (c/lit ">>") file))

(defn db
  "ReadySet DB at a given git ref"
  [ref]
  (let [consul (consul.db/db "1.16.1")]
    (reify db/DB
      (setup! [_ test node]
        (if-let [role (node-role test node)]
          (do
            (info node "role is" role)
            (case role
              :node-role/load-balancer
              (do
                (debian/install ["haproxy"])
                (let [haproxy-servers
                      (->> test
                           adapter-nodes
                           (map #(str "server " (name %) " " (name %) ":5432 check"))
                           (str/join "\n"))
                      haproxy-cfg (-> (io/resource "haproxy.cfg.tpl")
                                      (slurp)
                                      (str/replace "${servers}" haproxy-servers))]
                  (c/su
                   (cu/write-file! haproxy-cfg "/etc/haproxy/haproxy.cfg")
                   (c/exec "systemctl" "restart" "haproxy")))
                (jepsen/synchronize test (* 60 30)))

              :node-role/consul
              (do
                (db/setup! consul test node)
                (jepsen/synchronize test (* 60 30)))

              :node-role/upstream
              (do
                (debian/install ["postgresql"])
                (info node "setting up postgresql database " pgdatabase)
                (c/sudo
                 "postgres"

                 (try+
                  (c/exec "psql" :-c (str "create database " pgdatabase))
                  (catch #(re-find #"database \".*?\" already exists"
                                   (:err %)) _))
                 (c/exec "psql" :-c (str "alter user " pguser
                                         " password '" pgpassword "'"))

                 (append-to-file
                  "listen_addresses = '*'\nwal_level = logical"
                  "/etc/postgresql/14/main/postgresql.conf")
                 (append-to-file
                  "host all all 0.0.0.0/0 scram-sha-256"
                  "/etc/postgresql/14/main/pg_hba.conf"))
                (c/su (c/exec "systemctl" "restart" "postgresql"))
                (jepsen/synchronize test (* 60 30)))

              :node-role/readyset-adapter
              (do
                (compile-and-install-readyset-binary
                 node
                 ref
                 "readyset"
                 {:force? (:force-install test)})

                ;; Don't try to start readyset processes until Consul is up
                (jepsen/synchronize test (* 60 30))
                (c/su
                 (cu/start-daemon!
                  {:logfile "/var/log/readyset.log"
                   :pidfile "/var/run/readyset.pid"
                   :chdir "/"}
                  "/usr/local/bin/readyset"
                  :--log-level (:log-level test "info")
                  :--deployment "jepsen"
                  :-a "0.0.0.0:5432"
                  :--external-address (net/ip (name node))
                  :--authority-address (authority-address test)
                  :--upstream-db-url (upstream-db-url test)
                  :--disable-upstream-ssl-verification
                  :--embedded-readers
                  :--reader-replicas (str (num-adapters test)))))

              :node-role/readyset-server
              (do
                (compile-and-install-readyset-binary
                 node
                 ref
                 "readyset-server"
                 {:force? (:force-install test)})

                ;; Don't try to start readyset processes until Consul is up
                (jepsen/synchronize test (* 60 30))
                (c/su
                 (c/exec :mkdir "/opt/readyset/data")
                 (cu/start-daemon!
                  {:logfile "/var/log/readyset-server.log"
                   :pidfile "/var/run/readyset-server.pid"
                   :chdir "/"}
                  "/usr/local/bin/readyset-server"
                  :--log-level (:log-level test "info")
                  :--deployment "jepsen"
                  :--db-dir "/opt/readyset/data"
                  :-a "0.0.0.0"
                  :--external-address (net/ip (name node))
                  :--authority-address (authority-address test)
                  :--upstream-db-url (upstream-db-url test)
                  :--disable-upstream-ssl-verification
                  :--no-readers
                  :--reader-replicas (str (num-adapters test))))

                (let [ds (readyset-datasource test)]
                  (rs/wait-for-snapshot-completed ds))
                (info "ReadySet is running"))))

          (error node "unknown role")))

      (teardown! [_ test node]
        (if-let [role (node-role test node)]
          (do
            (info node "tearing down for role" role)
            (case role
              :node-role/load-balancer nil
              :node-role/consul (db/teardown! consul test node)

              :node-role/upstream
              (try+
               (info node "dropping database" pgdatabase)
               (c/sudo "postgres" (c/exec "psql"
                                          :-c (str "drop database " pgdatabase)))
               (catch #(re-find #"psql: command not found" (:err %)) _)
               (catch #(re-find #"database \".*?\" does not exist" (:err %)) _)
               (catch
                   #(re-find #"database \".*?\" is used by an active logical"
                             (:err %))
                   e
                 (warn node "Could not drop database:" e)))

              :node-role/readyset-adapter
              (c/su
               (cu/stop-daemon! "/var/run/readyset.pid")
               (info node "ReadySet Adapter killed")
               (c/exec :rm :-rf
                       "/var/run/readyset.pid"
                       "/var/log/readyset.log"))

              :node-role/readyset-server
              (c/su
               (cu/stop-daemon! "/var/run/readyset-server.pid")
               (info node "ReadySet Server killed")
               (c/exec :rm :-rf
                       "/var/run/readyset-server.pid"
                       "/var/log/readyset-server.log"
                       "/opt/readyset/data"))))
          (error node "unknown role")))

      db/LogFiles
      (log-files [_ test node]
        (if-let [role (node-role test node)]
          (case role
            :node-role/load-balancer nil
            :node-role/consul (db/log-files consul test node)
            :node-role/upstream nil
            :node-role/readyset-adapter ["/var/log/readyset.log"]
            :node-role/readyset-server ["/var/log/readyset-server.log"])
          (error node "unknown role"))))))

(defn readyset-test
  [opts]
  (merge
   tests/noop-test
   opts
   {:name "ReadySet"
    :os ubuntu/os
    :db (db "1eebd43bd6befd8acc9104b4239a414d72a4bd55"  ; Needs at least this commit
            #_"refs/tags/beta-2023-07-26")
    :pure-generators true}))

(def opt-spec
  [[nil "--log-level LOG_LEVEL" "Log level for ReadySet processes"
    :default "info"]
   [nil "--force-install" "Force install readyset binaries"]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn readyset-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
