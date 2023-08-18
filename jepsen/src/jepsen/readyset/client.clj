(ns jepsen.readyset.client
  "Utilities for interacting with a ReadySet cluster"
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.tools.logging :refer [info warn]]
   [dom-top.core :refer [with-retry]]
   [jepsen.client :as client]
   [jepsen.readyset.nodes :as nodes]
   [knossos.model :as model]
   [next.jdbc :as jdbc]
   [slingshot.slingshot :refer [throw+ try+]])
  (:import
   (org.postgresql.util PSQLException)))

(def pguser "postgres")
(def pgpassword "password")
(def pgdatabase "jepsen")

(defn make-datasource
  "Make a new JDBC DataSource for connecting to ReadySet, using the options
  accepted by `jdbc/get-datasource`"
  [db-info]
  (jdbc/get-datasource
   (assoc db-info
          ;; Use simple queries where possible, so that we can run readyset
          ;; commands (to work around REA-2960
          :preferQueryMode "simple"
          ;; Avoid sending `SET extra_float_digits = 3` on connection
          ;; (see https://github.com/pgjdbc/pgjdbc/issues/168)
          :assumeMinServerVersion "9.0")))

(defn test-datasource
  "Build a JDBC DataSource for connecting to the ReadySet cluster in the given
  test"
  [test]
  (make-datasource
   {:dbtype "postgres"
    :dbname pgdatabase
    :user pguser
    :password pgpassword
    :host (name (nodes/node-with-role test :node-role/load-balancer))
    :port 5432}))

(defn wait-for-connection
  "Wait for a connection to the given ReadySet datasource (as creaated by
  `make-datasource`) to succeed"
  [ds]
  (info "Waiting for ReadySet to be connectable")
  (with-retry [attempts 5]
    (jdbc/execute! ds ["show readyset version"])
    (catch PSQLException e
      (if (pos? attempts)
        (do
          (warn "Error connecting to readyset adapter:" (ex-message e))
          (Thread/sleep 1000)
          (retry (dec attempts)))
        (throw+ {:error :retry-attempts-exceeded
                 :exception e})))))

(defn readyset-status
  "Return the status of the ReadySet cluster for the given data source,
  represented as a map giving the results of the `SHOW READYSET STATUS` command"
  [ds]
  (let [res (jdbc/execute! ds ["show readyset status"])]
    (set/rename-keys
     (into {} (map (juxt :name :value)) res)
     {"Snapshot Status" :snapshot-status
      "Maximum Replication Offset" :maximum-replication-offset
      "Minimum Replication Offset" :minimum-replication-offset
      "Last Started Controller" :last-started-controller
      "Last Completed Snapshot" :last-completed-snapshot
      "Last Started Replication" :last-started-replication})))

(defn wait-for-snapshot-completed
  "Wait for the ReadySet cluster at the given datasource to report that
  snapshotting has completed"
  [ds]
  (wait-for-connection ds)
  (info "Waiting for snapshot status = Completed")
  (with-retry [attempts 5]
    (let [{:keys [snapshot-status]} (readyset-status ds)]
      (when-not (= snapshot-status "Completed")
        (if (pos? attempts)
          (do
            (Thread/sleep 200)
            (retry (dec attempts)))
          (throw+ {:error :retry-attempts-exceeded}))))))

(comment
  (def --ds
    (make-datasource {:dbtype "postgres"
                      :dbname "jepsen"
                      :user "postgres"
                      :password "password"
                      :host "aspen-jepsen-n1"
                      :port 5432}))

  (wait-for-connection --ds)

  (readyset-status --ds)
  )

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn final-r [_ _] {:type :invoke, :f :final-read, :value nil})

(defrecord Client [conn table-created? tables]
  client/Client
  (open! [this test node]
    (assoc this :conn (test-datasource test)))

  (setup! [this test]
    (when (compare-and-set! table-created? false true)
      (jdbc/execute! conn ["drop table if exists t1;"])
      (jdbc/execute! conn ["create table t1 (x int)"])
      (with-retry [attempts 5]
        (jdbc/execute! conn ["create cache from select x from t1"])
        (catch PSQLException e
          (if (pos? attempts)
            (do
              (Thread/sleep 200)
              (retry (dec attempts)))
            (throw+ {:error :retry-attempts-exceeded
                     :exception e}))))))

  (invoke! [_ test op]
    (try+
     (case (:f op)
       (:read :final-read)
       (assoc op
              :type :ok
              :value
              (map (some-fn :t1/x :x)
                   (jdbc/execute! conn ["select x from t1"])))

       :write
       (do (jdbc/execute! conn ["insert into t1 (x) values (?)"
                                (:value op)])
           (assoc op :type :ok)))
     (catch PSQLException e
       (assoc op :type :fail :message (ex-message e)))))

  (teardown! [this test]
    (try
      (some-> conn (jdbc/execute! ["drop table if exists t1;"]))
      (catch PSQLException e
        (warn "Could not drop table:" e))))

  (close! [this test]
    (dissoc this :conn)))

(defn new-client []
  (map->Client {:table-created? (atom false)}))
