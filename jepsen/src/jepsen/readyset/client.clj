(ns jepsen.readyset.client
  "Utilities for interacting with a ReadySet cluster"
  (:require
   [clojure.tools.logging :refer [info warn]]
   [dom-top.core :refer [with-retry]]
   [next.jdbc :as jdbc]
   [slingshot.slingshot :refer [throw+]]
   [clojure.set :as set])
  (:import
   (org.postgresql.util PSQLException)))

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
