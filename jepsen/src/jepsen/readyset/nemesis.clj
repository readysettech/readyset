(ns jepsen.readyset.nemesis
  (:require
   [jepsen.control :as c]
   [jepsen.control.util :as cu]
   [jepsen.nemesis :as nemesis]
   [jepsen.readyset.automation :as rs.auto]
   [jepsen.readyset.nodes :as nodes]))

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

(defrecord BitflipRocksdb [bitflip]
  nemesis/Nemesis
  (setup! [this test]
    (assoc this :bitflip (nemesis/setup! bitflip test)))
  (invoke! [_ test op]
    (let [server (nodes/node-with-role test :node-role/readyset-server)
          rocksdb-dirs (c/on server (cu/ls-full rs.auto/deployment-dir))]
      (if (empty? rocksdb-dirs)
        (assoc op :value :no-dbs)
        (nemesis/invoke! bitflip
                         test
                         (assoc op
                                :value
                                {server {:file (rand-nth rocksdb-dirs)}})))))
  (teardown! [this test]
    (assoc this :bitflip (nemesis/teardown! bitflip test)))

  nemesis/Reflection
  (fs [_] (nemesis/fs bitflip)))

(defn bitflip-rocksdb
  "Returns a nemesis which flips bits in random files within the RocksDB
  database files on the ReadySet server"
  []
  (BitflipRocksdb. (nemesis/bitflip)))
