(ns jepsen.readyset.checker
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set :as set]
   [jepsen.checker :as checker]
   [jepsen.readyset.nodes :as nodes]
   [jepsen.readyset.memory-db :as memory-db]
   [jepsen.readyset.workloads :as workloads]))

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

  The argument is a map where the key is query-id, and the value is a function
  which takes a map from tables to rows known to exist in those tables, and
  returns the expected results for that query. For parameterized queries,
  expected results can be a *function* from params to expected results

  * `{:type :ok, :f :value, :value query}` ops are applied to the db
  * `{:f :query, :value {:query-id query-id :params params :results results}}`
    ops must return the results for the query as of *some point* in the past
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
                        (let [expected (compute-results new-rows)]
                          (update-in results
                                     [query-id
                                      (if (fn? expected)
                                        :expected-results/fns
                                        :expected-results/set)]
                                     conj
                                     expected)))
                      results
                      expected-query-results)))))

            [:ok :query]
            (let [{:keys [query-id params results]} value

                  {:expected-results/keys [set fns] :as expected}
                  (get past-results query-id)

                  in-set? (contains? set results)
                  fn-results (into #{} (map #(% params)) fns)]
              (if (or in-set? (some #{results} fn-results))
                state
                (reduced
                 (assoc state
                        :valid? false
                        :failed-at index
                        :query-id query-id
                        :expected (assoc expected
                                         :expected-results/fns
                                         fn-results)
                        :got results))))

            state))
        {:valid? true
         :rows {}
         :past-results
         (into {}
               (map (fn [[k _]] [k {:expected-results/set #{[]}}]))
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

  (def res (checker/check
            (commutative-eventual-consistency
             (->> workloads/votes
                  :queries
                  (map (fn [[k v]] [k (:expected-results v)]))
                  (into {})))
            t
            (:history t)
            {}))
  (assert (:valid? res))

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
