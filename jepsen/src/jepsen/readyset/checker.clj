(ns jepsen.readyset.checker
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set :as set]
   [jepsen.checker :as checker]
   [jepsen.readyset.nodes :as nodes]
   [jepsen.readyset.memory-db :as memory-db]))

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
  "A simpler, faster (than Knossos) checker for eventual consistency of a
  database, which works if writes are commutative.

  Passed a map from query-id, to function which takes a map from tables to rows
  known to exist in those tables, and returns the expected results for that
  query

  * `{:type :ok, :f :value, :value query}` ops are applied to the db
  * `{:f :query, :value {:query-id query-id :results results}}` ops must return
    the results for the query as of *some point* in the past
  * `{:f :consistent-query, :value {:query-id query-id :results results}}` ops
    must return the *current* results for the query"
  [expected-query-results]
  (reify checker/Checker
    (check [_this _test history _opts]
      (->
       (reduce
        (fn [{:keys [rows past-results] :as state}
             {:keys [index type f value]}]
          (case [type f]
            [:ok :write]
            (let [new-rows (:rows (memory-db/apply-write {:rows rows} value))]
              (-> state
                  (assoc :rows new-rows)
                  (update
                   :past-results
                   (fn [results]
                     (reduce-kv
                      (fn [results query-id compute-results]
                        (update results
                                query-id
                                conj
                                (compute-results new-rows)))
                      results
                      expected-query-results)))))

            [:ok :query]
            (let [{:keys [query-id results]} value
                  past-results (get past-results query-id)]
              (if (contains? past-results results)
                state
                (reduced
                 (assoc state
                        :valid? false
                        :failed-at index
                        :expected past-results
                        :got results))))

            state))
        {:valid? true
         :rows {}
         :past-results
         (into {}
               (map (fn [[k _]] [k #{[]}]))
               expected-query-results)}
        history)
       ;; Don't include these keys in the final map; they get big and are
       ;; mostly noise
       (dissoc :rows :past-results)))))

(comment
  (require '[jepsen.store :as store])

  (def t (store/latest))

  ;; Looking at consistency

  (->> t
       :history
       (filter (comp #{:query :insert :consistent-query} :f))
       (filter (comp #{:ok} :type)))

  (def example-result
    (->> t
         :history
         (filter (comp #{:query} :f))
         (filter (comp #{:ok} :type))
         first))

  (def rows
    (->> t
         :history
         (filter (comp #{:write} :f))
         (filter (comp #{:ok} :type))
         (map :values)
         (group-by :table)
         (map (fn [[k ops]] [k (mapcat :rows ops)]))
         (into {})))

  (require '[jepsen.readyset.workloads :as workloads])
  (def compute-votes
    (get-in workloads/votes [:queries :votes :expected-results]))

  (compute-votes rows)

  (def final-consistent-result
    (->> t
         :history
         (filter (comp #{:consistent-query} :f))
         (filter (comp #{:ok} :type))
         first
         :value
         :results))

  (assert
   (= final-consistent-result (compute-votes rows)))

  (assert
   (:valid?
    (checker/check
     (commutative-eventual-consistency
      {:votes compute-votes})
     t
     (:history t)
     {})))

  ;; looking at failures

  (def first-failure
    (->> t
         :history
         (filter (comp #{:fail} :type))
         first))

  (def upto-first-failure
    (->> t
         :history
         (take-while #(not= :fail (:type %)))))

  (def after-first-failure
    (second (split-at (:index first-failure) (:history t))))

  (filter (comp #{:kill-adapter} :f) upto-first-failure)
  )
