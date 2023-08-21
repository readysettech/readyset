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
   [jepsen.control.util :as cu]
   [jepsen.core :as jepsen]
   [jepsen.db :as db]
   [jepsen.generator :as gen]
   [jepsen.nemesis :as nemesis]
   [jepsen.os.debian :as debian]
   [jepsen.os.ubuntu :as ubuntu]
   [jepsen.readyset.automation :as rs.auto]
   [jepsen.readyset.client :as rs]
   [jepsen.readyset.model :as rs.model]
   [jepsen.readyset.nemesis :as rs.nemesis]
   [jepsen.readyset.nodes :as nodes]
   [jepsen.tests :as tests]
   [slingshot.slingshot :refer [try+]]))

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
                (rs.auto/compile-and-install-readyset-binary!
                 node
                 ref
                 "readyset"
                 {:force? (:force-install test)})

                ;; Don't try to start readyset processes until Consul is up
                (jepsen/synchronize test (* 60 30))
                (rs.auto/start-readyset-adapter! test node))

              :node-role/readyset-server
              (do
                (rs.auto/compile-and-install-readyset-binary!
                 node
                 ref
                 "readyset-server"
                 {:force? (:force-install test)})

                ;; Don't try to start readyset processes until Consul is up
                (jepsen/synchronize test (* 60 30))
                (rs.auto/start-readyset-server! test node)

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
          (error node "unknown role")))

      db/Kill
      (kill! [_ test node]
        (case (nodes/node-role test node)
          :node-role/load-balancer nil
          :node-role/consul nil
          :node-role/upstream nil
          :node-role/readyset-adapter (rs.auto/kill-readyset-adapter!)
          :node-role/readyset-server (rs.auto/kill-readyset-server!)))
      (start! [_ test node]
        (case (nodes/node-role test node)
          :node-role/load-balancer nil
          :node-role/consul nil
          :node-role/upstream nil
          :node-role/readyset-adapter (rs.auto/start-readyset-adapter! test node)
          :node-role/readyset-server (rs.auto/start-readyset-server! test node))))))

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
    :nemesis (nemesis/compose
              {{:kill-adapter :start
                :start-adapter :stop} (rs.nemesis/kill-adapters)
               {:kill-server :start
                :start-server :stop} (rs.nemesis/kill-server)})
    :checker (checker/linearizable
              {:model (rs.model/eventually-consistent-table)
               :algorithm :linear})
    :generator (gen/phases
                (->> (gen/mix [rs/r rs/w])
                     (gen/stagger (/ (:rate opts)))

                     (gen/nemesis
                      (->> (gen/mix
                            [(cycle
                              [{:type :info :f :kill-adapter}
                               {:type :info :f :start-adapter}])

                             (cycle
                              [{:type :info :f :kill-server}
                               {:type :info :f :start-server}])])
                           (gen/stagger 2)))
                     (gen/time-limit (:time-limit opts)))

                (gen/nemesis
                 (gen/phases
                  (gen/once {:type :info, :f :start-server})
                  (gen/once {:type :info, :f :start-adapter})))

                (gen/log "Waiting for dataflow to converge")
                (gen/sleep (:converge-time opts))

                (gen/synchronize
                 (gen/once rs/final-r)))
    :pure-generators true}))

(def opt-spec
  (let [validate-pos-number [#(and (number? %) (pos? %))
                             "Must be a positive number"]]
    [[nil "--log-level LOG_LEVEL" "Log level for ReadySet processes"
      :default "info"]
     [nil "--force-install" "Force install readyset binaries"]
     ["-r" "--rate HZ" "Approximate number of requests per second, per thread"
      :default 10
      :parse-fn read-string
      :validate validate-pos-number]
     [nil
      "--converge-time SECONDS"
      "Number of seconds to wait for dataflow to converge"
      :default 15
      :parse-fn read-string
      :validate validate-pos-number]]))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn readyset-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
