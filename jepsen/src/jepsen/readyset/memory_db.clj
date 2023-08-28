(ns jepsen.readyset.memory-db
  "Functions for tracking the in-memory state of the DB, responding to SQL
  statements expressed as HoneySQL maps.

  All functions which take a `db` argument in this `ns` also work on maps with a
  `:rows` key"
  (:require [clojure.core.match :refer [match]]))

(defrecord DB [rows])

(defn empty-db
  "Construct a new, empty DB"
  []
  (map->DB {:rows {}}))

(defn insert
  "Returns a new DB with the given rows inserted into the given table"
  [db table rows]
  (update-in db [:rows table] into rows))

(defn- where-clause->pred [table where-clause]
  (letfn [(get-col [col] (some-fn col
                                  (keyword (name table) (name col))))]
    (match where-clause
      [:= (col :guard keyword?) v]
      #(= v ((get-col col) %))

      [:in (col :guard keyword?) l]
      (comp (into #{} l) (get-col col))

      [:and pred1 pred2]
      (every-pred (where-clause->pred table pred1)
                  (where-clause->pred table pred2)))))

(defn delete-where
  "Delete all rows matching the given WHERE clause in the table"
  [db table where-clause]
  (update-in
   db
   [:rows table]
   (fn [rows]
     (filter (complement (where-clause->pred table where-clause)) rows))))

(defn- ->rows [rows table columns]
  (for [row rows]
    (condp #(%1 %2) row
      map? row
      vector? (zipmap (map #(keyword (name table) (name %)) columns) row))))

(defn apply-write
  "Apply the given write, which should be a HoneySQL map, to the given db"
  [db write]
  (match write
    {:insert-into table :columns columns :values rows}
    (insert db table (->rows rows table columns))

    {:insert-into table :values rows}
    (insert db table rows)

    {:delete-from table :where pred}
    (delete-where db table pred)))
