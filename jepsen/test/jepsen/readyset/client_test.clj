(ns jepsen.readyset.client-test
  (:require
   [clojure.test :refer [deftest is]]
   [honey.sql :as sql]
   [jepsen.readyset.client]))

(deftest honeysql-create-cache-test
  (is (= ["CREATE CACHE FROM SELECT * FROM t"]
         (sql/format {:create-cache :_
                      :select [:*]
                      :from [:t]}))
      "Without a name")

  (is (= ["CREATE CACHE mycache FROM SELECT * FROM t"]
         (sql/format {:create-cache :mycache
                      :select [:*]
                      :from [:t]}))
      "With a name")

  (is (= ["CREATE CACHE my_cache FROM SELECT * FROM t"]
         (sql/format {:create-cache :my-cache
                      :select [:*]
                      :from [:t]}))
      "With a name requiring character replacing")

  (is (= ["CREATE CACHE \"table\" FROM SELECT * FROM \"t\""]
         (sql/format {:create-cache :table
                      :select [:*]
                      :from [:t]}
                     {:dialect :ansi}))
      "With a name requiring quoting"))
