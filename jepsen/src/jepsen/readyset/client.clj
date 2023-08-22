(ns jepsen.readyset.client
  "Utilities for interacting with a ReadySet cluster"
  (:require
   [clojure.set :as set]
   [clojure.tools.logging :refer [info warn]]
   [dom-top.core :refer [with-retry]]
   [honey.sql :as sql]
   [jepsen.client :as client]
   [jepsen.readyset.nodes :as nodes]
   [next.jdbc :as jdbc]
   [slingshot.slingshot :refer [throw+ try+]]
   [jepsen.generator :as gen])
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

;;;

(defn recreate-table
  "Given a honeysql-compatible `:create-table` map, drops and recreates that
  table in the given db"
  [db {table-name :create-table :as create-table}]
  (jdbc/execute! db (sql/format {:drop-table [:if-exists table-name]}
                                {:dialect :ansi}))
  (jdbc/execute! db (sql/format create-table
                                {:dialect :ansi})))

(defn format-create-cache [_ n]
  [(cond-> "CREATE CACHE"
     (not= :_ n) (str " " (sql/format-entity n))
     true (str " FROM"))])
(sql/register-clause! :create-cache #'format-create-cache :select)

(defrecord Inserts [gen-insert rows]
  gen/Generator
  (op [this _test ctx]
    (when-let [insert (gen-insert rows)]
      [(gen/fill-in-op {:type :invoke :f :insert :value insert} ctx) this]))

  (update [this _test _ctx {:keys [type f value]}]
    (case [type f]
      [:ok :insert]
      (let [{:keys [table rows]} value]
        (update-in this [:rows table] into rows))

      this)))

(defn inserts
  "Given a function to generate insert statements, returns a `Generator` for
  generating insert ops for those statements

  The `gen-insert` function will be passed a map from table names, to a list of
  rows known to exist in that table. It should return a `honey.sql`-compatible
  `:insert-into` map, or `nil` if no more inserts should be generated"
  [gen-insert]
  (map->Inserts {:gen-insert gen-insert :rows {}}))

(defn query [q]
  {:type :invoke, :f :query, :value q})

(defn consistent-query [q]
  {:type :invoke, :f :consistent-query, :value q})

(defrecord Client [conn
                   tables
                   queries
                   tables-created?
                   retry-queries?]
  client/Client
  (open! [this test _node]
    (assoc this :conn (test-datasource test)))

  (setup! [_this _test]
    (when (compare-and-set! tables-created? false true)
      (doseq [table tables]
        (recreate-table conn table))

      (doseq [query (vals queries)]
        (with-retry [attempts 5]
          (jdbc/execute! conn (sql/format
                               (assoc query :create-cache :_)
                               {:dialect :ansi}))
          (catch PSQLException e
            (if (pos? attempts)
              (do
                (Thread/sleep 200)
                (retry (dec attempts)))
              (throw+ {:error :retry-attempts-exceeded
                       :exception e})))))))

  (invoke! [_ _test {:keys [f value] :as op}]
    (letfn [(maybe-retry-once [f]
              (with-retry [attempts (if retry-queries? 1 0)]
                (f)
                (catch PSQLException e
                  (if (pos? attempts)
                    (retry (dec attempts))
                    (throw e)))))]
      (try+
       (case f
         (:query :consistent-query)
         (if-let [q (get queries value)]
           (maybe-retry-once
            #(assoc op
                    :type :ok
                    :value
                    {:query-id value
                     :results
                     (jdbc/execute! conn (sql/format q {:dialect :ansi}))}))
           (throw+ {:error :unknown-query
                    :query-id value
                    :known-queries (into #{} (keys queries))}))

         :insert
         (maybe-retry-once
          #(assoc op
                  :type :ok
                  :value
                  {:table (:insert-into value)
                   :rows
                   (jdbc/execute!
                    conn
                    (-> value
                        (assoc :returning [:*])
                        (sql/format {:dialect :ansi}))) })))

       (catch PSQLException e
         (assoc op :type :fail :message (ex-message e))))))

  (teardown! [_this _test]
    (try
      (some-> conn (jdbc/execute! ["drop table if exists t1;"]))
      (catch PSQLException e
        (warn "Could not drop table:" e))))

  (close! [this _test]
    (dissoc this :conn)))

(defn new-client
  "Create a new jepsen Client for ReadySet

  Options supported:

  * `:retry-queries?` (optional) if set to true, all queries will be retried
    once before being considered failed
  * `:tables` (required) A list of tables, represented as HoneySQL
    `:create-table` maps, to install in the DB
  * `queries` (required) A map from query ID, which should be a keyword, to
    queries. represented as HoneySQL `:select` maps. The key in this map will be
    used to execute the query as part of the `query` op. Queries with parameters
    are not yet supported (TODO)"
  [opts]
  (-> opts
      (select-keys [:retry-queries?
                    :tables
                    :queries])
      (merge {:tables-created? (atom false)})
      map->Client))
