(ns jepsen.readyset.nemesis
  (:require [jepsen.nemesis :as nemesis]
            [jepsen.readyset.nodes :as nodes]
            [jepsen.readyset.automation :as rs.auto]))

(defn- wrap-reflection [fs nemesis]
  (reify
    nemesis/Nemesis
    (setup! [_ test] (nemesis/setup! nemesis test))
    (invoke! [_ test op] (nemesis/invoke! nemesis test op))
    (teardown! [_ test] (nemesis/teardown! nemesis test))

    nemesis/Reflection
    (fs [_] fs)))

(defn kill-adapters
  "Nemesis which kills a random adapter at `{:f :start}`, and starts the adapter
  at `{:f :stop}`"
  []
  (wrap-reflection
   #{:start :stop}
   (nemesis/node-start-stopper
    (fn [test _nodes]
      [(rand-nth (nodes/nodes-with-role test :node-role/readyset-adapter))])
    rs.auto/kill-readyset-adapter!
    rs.auto/start-readyset-adapter!)))

(defn kill-server
  "Nemesis which kills the server at `{:f :start}`, and starts the server at
  `{:f :stop}`"
  []
  (wrap-reflection
   #{:start :stop}
   (nemesis/node-start-stopper
    (fn [test _nodes] [(nodes/node-with-role test :node-role/readyset-server)])
    rs.auto/kill-readyset-server!
    rs.auto/start-readyset-server!)))
