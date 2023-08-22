(ns jepsen.readyset.workloads
  "ReadySet workloads.

  Workloads are represented as maps with the following structure:

  `{:tables ; list of honeysql :create-table maps
    :queries
    {:query-id-one
     {:query ; honeysql :select map
      :expected-results ; fn from rows map (from table kw to list of rows) to
                          expected results for this query
      :gen-insert ; fn from rows map to generated honeysql :insert-into map
     }}``")

(def votes
  {:tables
   [{:create-table :stories
     :with-columns [[:id :serial]
                    [:title :text]
                    [[:primary-key :id]]]}
    {:create-table :votes
     :with-columns [[:story-id :int]
                    [:user-id :int]]}]

   :queries
   {:votes
    {:query
     {:select [:id :title :vcount]
      :from [:stories]
      :left-join
      [[{:select [:story-id [:%count.* :vcount]]
         :from [:votes]
         :group-by :story-id}
        :vote-count]
       [:= :stories.id :vote-count.story-id]]
      :order-by [:stories.id]}
     :expected-results
     (fn compute-votes [rows]
       (let [vote-counts
             (->> rows
                  :votes
                  (group-by :votes/story-id)
                  (map
                   (fn [[story-id rows]]
                     [story-id (count rows)]))
                  (into {}))]
         (->> rows
              :stories
              (map #(select-keys % [:stories/id :stories/title]))
              (map (fn [{:stories/keys [id] :as story}]
                     (assoc story :vcount (get vote-counts id))))
              (sort-by :stories/id))))}}

   :gen-insert
   (fn [rows]
     (let [candidate-tables (if (contains? rows :stories)
                              [:stories :votes]
                              [:stories])]
       (case (rand-nth candidate-tables)
         :stories
         {:insert-into :stories
          :columns [:title]
          ;; generate between 1 and 5 stories
          :values (for [_ (range 0 (inc (rand-int 5)))]
                    [(str "story-" (rand-int (Integer/MAX_VALUE)))])}
         :votes
         (let [candidate-stories (->> rows :stories (map :stories/id))]
           {:insert-into :votes
            :columns [:story-id :user-id]
            ;; generate between 1 and 5 votes
            :values (for [_ (range 0 (inc (rand-int 5)))]
                      [(rand-nth candidate-stories)
                       (rand-int Integer/MAX_VALUE)])}))))})
