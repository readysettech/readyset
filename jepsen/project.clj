(defproject jepsen.readyset "0.1.0-SNAPSHOT"
  :description "ReadySet Jepsen test"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.3-SNAPSHOT"]

                 [base64-clj "0.1.1"]
                 [cheshire "5.9.0"]
                 [clj-http "3.10.0"]
                 [com.github.seancorfield/next.jdbc "1.3.883"]
                 [org.postgresql/postgresql "42.6.0"]
                 [slingshot "0.12.2"]]
  :repl-options {:init-ns jepsen.readyset}
  :main jepsen.readyset
  :resource-paths ["resources"])
