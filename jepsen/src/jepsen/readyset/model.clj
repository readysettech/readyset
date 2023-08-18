(ns jepsen.readyset.model
  "A Knossos Model for ReadySet eventual consistency"
  (:require
   [knossos.model :as model])
  (:import
   (knossos.model Model)))

(defrecord
    ^{:doc "A model for an eventually-consistent table with a single column.
    Supports :write ops to write new rows, and :read ops to read the list of
    rows out of the table"}
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
                                         "; valid results: " past-results)))))))

(defn eventually-consistent-table []
  (map->EventuallyConsistentTable
   {:rows []
    :past-results #{[]}}))
