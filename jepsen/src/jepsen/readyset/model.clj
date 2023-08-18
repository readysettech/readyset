(ns jepsen.readyset.model
  "A Knossos Model for ReadySet eventual consistency"
  (:require
   [knossos.model :as model])
  (:import
   (knossos.model Model)))

(defrecord
    ^{:doc "A model for an eventually-consistent table with a single column.
    Supports :write ops to write new rows, and :read ops to read the list of
    rows out of the table in an eventually-consistent fashion. Also supports
    :final-read, to read a final set of rows out of the table, which *must* be
    consistent"}
    EventuallyConsistentTable [rows past-results]
  Model
  (step [this op]
    (case (:f op)
      :write (-> this
                 (update :rows conj (:value op))
                 (#(update % :past-results conj (sort (:rows %)))))
      :read (let [results (sort (:value op))]
              (if (or (nil? (:value op))
                      (contains? past-results results))
                this
                (model/inconsistent (str "can't read " results
                                         "; valid results: " past-results))))
      :final-read (let [results (sort (:value op))]
                    (if (= (sort rows) results)
                      this
                      (model/inconsistent (str "final read of " results
                                               " did not match " rows)))))))

(defn eventually-consistent-table []
  (map->EventuallyConsistentTable
   {:rows []
    :past-results #{[]}}))
