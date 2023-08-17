(ns jepsen.consul.db
  "Adapted from https://github.com/jepsen-io/jepsen/blob/main/consul/src/jepsen/consul/db.clj"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.consul.client :as client]
            [jepsen.core :as jepsen]
            [jepsen.db :as db]
            [jepsen.control :as c]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]))

(def dir "/opt")
(def binary "consul")
(def config-file "/opt/consul.json")
(def pidfile "/var/run/consul.pid")
(def logfile "/var/log/consul.log")
(def data-dir "/var/lib/consul")

(def retry-interval "5s")

(defn start-consul!
  [node]
  (info node "starting consul")
  (cu/start-daemon!
   {:logfile logfile
    :pidfile pidfile
    :chdir   dir}
   binary
   :agent
   :-server
   :-log-level "debug"
   :-client    "0.0.0.0"
   :-bind      (net/ip (name node))
   :-data-dir  data-dir
   :-node      (name node)
   :-retry-interval retry-interval
   :-bootstrap
   :>> logfile
   (c/lit "2>&1")))

(defn db
  "Install and cleanup a specific version of consul"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing consul" version)
      (c/su
       (let [url (str "https://releases.hashicorp.com/consul/"
                      version "/consul_" version "_linux_amd64.zip")]
         (cu/install-archive! url (str dir "/" binary)))

       (start-consul! node))

      (info "Waiting for cluster to converge")
      (client/await-cluster-ready node 1)

      #_(jepsen/synchronize test))

    (teardown! [_ _test node]
      (c/su
       (cu/stop-daemon! binary pidfile)
       (info node "consul killed")

       (c/exec :rm :-rf pidfile logfile data-dir (str dir "/" binary) config-file)
       (c/su
        (c/exec :rm :-rf binary)))
      (info node "consul nuked"))

    db/LogFiles
    (log-files [_ _test _node]
      [logfile])))
