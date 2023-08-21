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
