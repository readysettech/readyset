(ns jepsen.readyset.checker
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set :as set]
   [jepsen.checker :as checker]
   [jepsen.readyset.nodes :as nodes]
   [jepsen.readyset.memory-db :as memory-db]
   [jepsen.readyset.workloads :as workloads]
   [clojure.math.combinatorics :refer [subsets]]))

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
  query. For parameterized queries, expected results can be a *function* from
  params to expected results

  * `{:type :ok, :f :value, :value query}` ops are applied to the db
  * `{:f :query, :value {:query-id query-id :params params :results results}}`
    ops must return the results for the query as of *some point* in the past
  * `{:f :consistent-query, :value {:query-id query-id :results results}}` ops
    must return the *current* results for the query"
  [expected-query-results]

  (reify checker/Checker
    (check [_this _test history _opts]
      (let [update-past-results
            (fn [state]
              (update state :past-results
                      (fn [results]
                        (reduce-kv
                         (fn [results query-id compute-results]
                           (let [expected (compute-results (:rows state))]
                             (update-in results
                                        [query-id
                                         (if (fn? expected)
                                           :expected-results/fns
                                           :expected-results/set)]
                                        conj
                                        expected)))
                         results
                         expected-query-results))))

            validate-read
            (fn [{:keys [index] {:keys [query-id params results]} :value}
                 {:keys [past-results pending-writes] :as state}]
              (let [{:expected-results/keys [set fns]
                     :as expected} (get past-results query-id)
                    in-set? (contains? set results)
                    fn-results (map #(% params) fns)

                    ;; If there are more `:type :invoke, :f :write` ops than
                    ;; `:type :ok, :f write` ops, this result set might be
                    ;; seeing one of those. Step back through the last
                    ;; `pending-writes` write invokes and see if our result set
                    ;; is valid for a db that includes one of them
                    valid-for-pending-invoke?
                    (fn []
                      (let [pending-invokes (->> history
                                                 (take index)
                                                 (reverse)
                                                 (filter (every-pred
                                                          (comp #{:invoke} :type)
                                                          (comp #{:write} :f)))
                                                 (take pending-writes))
                            ;; any subset of the pending invokes might have
                            ;; completed
                            possible-completed-invokes
                            (filter seq (subsets pending-invokes))

                            ;; make a list of the possible dbs
                            possible-dbs
                            (for [invokes possible-completed-invokes]
                              (reduce memory-db/apply-write
                                      state
                                      (map :value invokes)))

                            compute-results (get expected-query-results query-id)]
                        (some (fn [{:keys [rows]}]
                                (let [expected (compute-results rows)
                                      expected (if (fn? expected)
                                                 (expected params)
                                                 expected)]
                                  (= expected results)))
                              possible-dbs)) )]

                (if (or in-set?
                        (some #{results} fn-results)
                        (valid-for-pending-invoke?))
                  {:valid? true}
                  {:valid? false
                   :query-id query-id
                   :expected (assoc expected :expected-results/fns fn-results)
                   :got results})))]
        (->
         (reduce
          (fn [state {:keys [index type f value] :as op}]
            (case [type f]
              [:invoke :write]
              (-> state (update :pending-writes inc))

              [:ok :write]
              (-> state
                  (update :pending-writes dec)
                  (memory-db/apply-write value)
                  (update-past-results))

              [:ok :query]
              (let [res (validate-read op state)]
                (if (:valid? res)
                  state
                  (reduced (merge state res {:failed-at index}))))

              state))
          {:valid? true
           :rows {}
           :pending-writes 0
           :past-results
           (into {}
                 (map (fn [[k _]] [k {:expected-results/set #{[]}}]))
                 expected-query-results)}
          history)
         ;; Don't include these keys in the final map; they get big and are
         ;; mostly noise
         (dissoc :rows
                 :past-results))))))

(comment
  (require '[jepsen.store :as store])
  (require '[jepsen.readyset.history :as hist])

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

  (->> res
       :expected
       :expected-results/set
       (filter #(= (into #{} (map :stories/id) %)
                   (into #{} (map :stories/id) (:got res)))))

  ;; GOT:
  [{:stories/id 1, :stories/title "story-1802525859", :vcount 3}
   {:stories/id 2, :stories/title "story-314842577", :vcount 1}
   {:stories/id 3, :stories/title "story-1420760905", :vcount 4}
   {:stories/id 13, :stories/title "story-2107023155", :vcount 3}
   {:stories/id 14, :stories/title "story-1557371865", :vcount 2}
   {:stories/id 15, :stories/title "story-2104640635", :vcount 1}
   {:stories/id 16, :stories/title "story-908649073", :vcount 2}
   {:stories/id 17, :stories/title "story-684800089", :vcount 2}
   {:stories/id 18, :stories/title "story-1704918784", :vcount 2}
   {:stories/id 20, :stories/title "story-1904643225", :vcount 1}
   {:stories/id 21, :stories/title "story-533861241", :vcount 1}
   {:stories/id 22, :stories/title "story-382932692", :vcount nil}
   {:stories/id 29, :stories/title "story-572695678", :vcount 1}
   {:stories/id 30, :stories/title "story-1484670588", :vcount 1}
   {:stories/id 31, :stories/title "story-302693351", :vcount 1}
   {:stories/id 33, :stories/title "story-1824543582", :vcount nil}
   {:stories/id 34, :stories/title "story-1583525566", :vcount nil}
   {:stories/id 35, :stories/title "story-734289220", :vcount nil}]

  ;; LATEST :ok
  (->> t :history
       (take (:failed-at res))
       (hist/apply-writes :ok)
       :rows
       (workloads/compute-votes))
  [{:stories/id 1, :stories/title "story-1802525859", :vcount 3}
   {:stories/id 2, :stories/title "story-314842577", :vcount 1}
   {:stories/id 3, :stories/title "story-1420760905", :vcount 4}
   {:stories/id 13, :stories/title "story-2107023155", :vcount 3}
   {:stories/id 14, :stories/title "story-1557371865", :vcount 2}
   {:stories/id 15, :stories/title "story-2104640635", :vcount 1}
   {:stories/id 16, :stories/title "story-908649073", :vcount 2}
   {:stories/id 17, :stories/title "story-684800089", :vcount 2}
   {:stories/id 18, :stories/title "story-1704918784", :vcount 2}
   {:stories/id 20, :stories/title "story-1904643225", :vcount 1}
   {:stories/id 21, :stories/title "story-533861241", :vcount 1}
   {:stories/id 22, :stories/title "story-382932692", :vcount nil}
   {:stories/id 29, :stories/title "story-572695678", :vcount 1}
   {:stories/id 30, :stories/title "story-1484670588", :vcount 1}
   {:stories/id 31, :stories/title "story-302693351", :vcount 1}
   {:stories/id 32, :stories/title "story-352417266", :vcount nil}
   {:stories/id 33, :stories/title "story-1824543582", :vcount nil}
   {:stories/id 34, :stories/title "story-1583525566", :vcount nil}
   {:stories/id 35, :stories/title "story-734289220", :vcount nil}
   ]

  ;; LATEST :invoke
  (->> t :history
       (take (:failed-at res))
       (hist/apply-writes :invoke)
       :rows
       (workloads/compute-votes))
  [{:stories/id 1, :stories/title "story-1802525859", :vcount 3}
   {:stories/id 2, :stories/title "story-314842577", :vcount 1}
   {:stories/id 3, :stories/title "story-1420760905", :vcount 4}
   {:stories/id 13, :stories/title "story-2107023155", :vcount 3}
   {:stories/id 14, :stories/title "story-1557371865", :vcount 2}
   {:stories/id 15, :stories/title "story-2104640635", :vcount 1}
   {:stories/id 16, :stories/title "story-908649073", :vcount 2}
   {:stories/id 17, :stories/title "story-684800089", :vcount 1}
   {:stories/id 18, :stories/title "story-1704918784", :vcount 2}
   {:stories/id 20, :stories/title "story-1904643225", :vcount 1}
   {:stories/id 21, :stories/title "story-533861241", :vcount 1}
   {:stories/id 22, :stories/title "story-382932692", :vcount nil}
   {:stories/id 29, :stories/title "story-572695678", :vcount 1}
   {:stories/id 30, :stories/title "story-1484670588", :vcount 1}
   {:stories/id 31, :stories/title "story-302693351", :vcount 1}
   {:stories/id 33, :stories/title "story-1824543582", :vcount nil}
   {:stories/id 34, :stories/title "story-1583525566", :vcount nil}
   {:stories/id 35, :stories/title "story-734289220", :vcount nil}
   ]


  (def db1
    (->> t :history
         (take (:failed-at res))
         (filter (every-pred (comp #{:ok} :type)
                             (comp #{:write} :f)))
         (map :value)
         (take 2)
         (reduce memory-db/apply-write (memory-db/empty-db))))

  (memory-db/apply-write
   db1
   {:delete-from :votes, :where [:and [:= :story-id 4] [:= :user-id 1313110714]]})

  ;; PENDING WRITES
  (def pending-invokes (->> t :history
                            (take (:failed-at res))
                            (hist/last-n-write-invokes (:pending-writes res))))

  ;;;

  (->> t :history (take (:failed-at res))
       (filter (every-pred (comp #{:invoke} :type)
                           (comp #{:write} :f)))
       (map :value)
       (reduce memory-db/apply-write (memory-db/empty-db)))

  (->> t :history (take (:failed-at res))
       (filter (every-pred (comp #{:invoke} :type)
                           (comp #{:write} :f)))
       (map :value)
       )


  (->> t :history (take (:failed-at res))
       (filter (every-pred (comp #{:invoke} :type)
                           (comp #{:write} :f)))
       (map :value))


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
