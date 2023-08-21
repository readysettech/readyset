(ns jepsen.readyset.checker
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set :as set]
   [jepsen.checker :as checker]
   [jepsen.readyset.nodes :as nodes]))

(defn liveness
  "All queries should succeed if at least one adapter is alive"
  []
  (reify checker/Checker
    (check [_this test history _opts]
      (let [initial-live-adapters (into #{}
                                        (nodes/nodes-with-role
                                         test
                                         :node-role/readyset-adapter))]
        (reduce
         (fn [state {:keys [index type f value]}]
           (match [type f value]
             [:info :kill-adapter (_ :guard map?)]
             (update
              state
              :live-adapters
              #(set/difference %
                               ;; `value` looks like a map from node name to ""
                               (into #{} (keys value))))

             [:info :start-adapter (_ :guard map?)]
             (update state :live-adapters into (keys value))

             [:fail _ _]
             (if (seq (:live-adapters state))
               (reduced
                (assoc state
                       :valid? false
                       :failed-at index))
               state)

             :else state))
         {:valid? true
          :failed-at nil
          :live-adapters initial-live-adapters}
         history)))))

(defn commutative-eventual-consistency
  "A simpler, faster (than Knossos) checker for eventual consistency of a single
  table, which works if writes are commutative.

  * `{:f :write, :value x}` ops are inserted into the table
  * `{:f :read, :value rows}` ops must return the state of the table as of *some
    point* in the past
  * `{:f :final-read :value rows}` ops must return the *current* state of the
    table"
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (->
       (reduce
        (fn [{:keys [rows past-results] :as state}
             {:keys [index type f value]}]
          (case [type f]
            [:ok :write]
            (let [new-rows (-> rows (conj value) sort)]
              (-> state
                  (assoc :rows new-rows)
                  (update :past-results conj (sort new-rows))))

            [:ok :read]
            (if (contains? past-results (sort value))
              state
              (reduced
               (assoc state
                      :valid? false
                      :failed-at index)))

            state))
        {:valid? true
         :rows []
         :past-results #{[]}}
        history)
       ;; Don't include these keys in the final map; they get big and are
       ;; mostly noise
       (dissoc :rows :past-results)))))

(comment
  (require '[jepsen.store :as store])

  (def t (store/test -2))

  (checker/check
   (commutative-eventual-consistency)
   t
   (:history t)
   {}
   )
  )
