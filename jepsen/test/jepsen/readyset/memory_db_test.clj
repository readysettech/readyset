(ns jepsen.readyset.memory-db-test
  (:require [jepsen.readyset.memory-db :as sut]
            [clojure.test :refer [deftest is]]))

(deftest apply-writes-test
  (is (= {:t1 [{:t1/x 2} {:t1/x 1}]
          :t2 [{:t2/y 3 :t2/z 4}]}
         (:rows
          (reduce
           sut/apply-write
           (sut/empty-db)
           [{:insert-into :t1 :values [{:t1/x 1}]}
            {:insert-into :t1 :values [{:t1/x 2} {:t1/x 3}]}
            {:insert-into :t2 :values [{:t2/y 3 :t2/z 4}
                                       {:t2/y 3 :t2/z 5}
                                       {:t2/y 4 :t2/z 4}
                                       {:t2/y 5 :t2/z 6}]}
            {:delete-from :t1 :where [:= :x 3]}
            {:delete-from :t2 :where [:in :y [4 5]]}
            {:delete-from :t2 :where [:and
                                      [:= :y 3]
                                      [:= :z 5]]}])))))
