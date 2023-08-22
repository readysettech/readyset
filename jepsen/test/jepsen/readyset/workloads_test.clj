(ns jepsen.readyset.workloads-test
  (:require [jepsen.readyset.workloads :as sut]
            [clojure.test :refer [deftest is]]))


(deftest compute-votes-test
  (let [compute-votes
        (get-in sut/votes [:queries :votes :expected-results])]

    (is (= [{:stories/id 1 :stories/title "a" :vcount 2}
            {:stories/id 2 :stories/title "b" :vcount nil}]
           (compute-votes {:stories [#:stories{:id 1 :title "a"}
                                     #:stories{:id 2 :title "b"}]
                           :votes [#:votes{:story-id 1 :user-id 1}
                                   #:votes{:story-id 1 :user-id 2}]})))))
