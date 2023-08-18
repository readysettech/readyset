(ns jepsen.readyset
  (:gen-class)
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.tools.logging :refer [error info warn]]
   [jepsen.checker :as checker]
   [jepsen.cli :as cli]
   [jepsen.consul.db :as consul.db]
   [jepsen.control :as c]
   [jepsen.control.net :as net]
   [jepsen.control.util :as cu]
   [jepsen.core :as jepsen]
   [jepsen.db :as db]
   [jepsen.generator :as gen]
   [jepsen.os.debian :as debian]
   [jepsen.os.ubuntu :as ubuntu]
   [jepsen.readyset.client :as rs]
   [jepsen.readyset.model :as rs.model]
   [jepsen.readyset.nodes :as nodes]
   [jepsen.tests :as tests]
   [slingshot.slingshot :refer [try+]]))

(defn- upstream-db-url
  "Returns the upstream DB URL for the given test"
  [test]
  (str "postgresql://"
       rs/pguser ":" rs/pgpassword
       "@" (nodes/node-with-role test :node-role/upstream)
       "/" rs/pgdatabase))

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
  (str (name (nodes/node-with-role test :node-role/consul))
       ":8500"))

(defn- append-to-file [s file]
  (c/exec "echo" s (c/lit ">>") file))

(defn db
  "ReadySet DB at a given git ref"
  [ref]
  (let [consul (consul.db/db "1.16.1")]
    (reify db/DB
      (setup! [_ test node]
        (if-let [role (nodes/node-role test node)]
          (do
            (info node "role is" role)
            (case role
              :node-role/load-balancer
              (do
                (debian/install ["haproxy"])
                (let [haproxy-servers
                      (->> test
                           nodes/adapter-nodes
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
                (info node "setting up postgresql database " rs/pgdatabase)
                (c/sudo
                 "postgres"

                 (try+
                  (c/exec "psql" :-c (str "create database " rs/pgdatabase))
                  (catch #(re-find #"database \".*?\" already exists"
                                   (:err %)) _))
                 (c/exec "psql" :-c (str "alter user " rs/pguser
                                         " password '" rs/pgpassword "'"))

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
                  :--controller-address "0.0.0.0"
                  :--external-address (net/ip (name node))
                  :--authority-address (authority-address test)
                  :--upstream-db-url (upstream-db-url test)
                  :--disable-upstream-ssl-verification
                  :--embedded-readers
                  :--reader-replicas (str (nodes/num-adapters test)))))

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
                  :--allow-full-materialization
                  :--disable-upstream-ssl-verification
                  :--no-readers
                  :--reader-replicas (str (nodes/num-adapters test))))

                (let [ds (rs/test-datasource test)]
                  (rs/wait-for-snapshot-completed ds))
                (info "ReadySet is running"))))

          (error node "unknown role")))

      (teardown! [_ test node]
        (if-let [role (nodes/node-role test node)]
          (do
            (info node "tearing down for role" role)
            (case role
              :node-role/load-balancer nil
              :node-role/consul (db/teardown! consul test node)

              :node-role/upstream
              (try+
               (info node "dropping database" rs/pgdatabase)
               (c/sudo "postgres" (c/exec "psql"
                                          :-c (str "drop database " rs/pgdatabase)))
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
        (if-let [role (nodes/node-role test node)]
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
    :client (rs/new-client)
    :checker (checker/linearizable
              {:model (rs.model/eventually-consistent-table)
               :algorithm :linear})
    :generator (->> (gen/mix [rs/r rs/w])
                    (gen/stagger 1)
                    (gen/nemesis nil)
                    (gen/time-limit 15))
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
