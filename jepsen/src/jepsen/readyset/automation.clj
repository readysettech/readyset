(ns jepsen.readyset.automation
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.readyset.client :as rs]
            [jepsen.readyset.nodes :as nodes]))

(def storage-dir
  "The database directory used when running the readyset server"
  "/opt/readyset/data")

(def deployment
  "The name of the ReadySet deployment"
  "jepsen")

(def deployment-dir
  "The directory that will be used to store deployment data"
  (str storage-dir "/" deployment))

(defn ensure-git-cloned
  "Ensure that a git repository `repo` is cloned at ref `ref` in dir `dir`"
  [repo ref dir]
  (debian/install ["git"])

  (when (and (cu/exists? dir)
             (not (cu/exists? (str dir "/.git"))))
    (c/exec :rm :-rf dir))
  (letfn [(git [& args] (apply c/exec :git :-C dir args))]
    (if (cu/exists? dir)
      (git :fetch :origin)
      (c/exec :git :clone repo dir))
    (git :checkout ref)))

(defn compile-and-install-readyset-binary!
  [node ref bin & [{:keys [force?] :or {force? false}}]]
  (if (and
       (cu/file? (str "/usr/local/bin/" bin))
       (not force?))
    (info node bin "already exists, not re-installing")
    (c/su
     (debian/install ["clang"
                      "libclang-dev"
                      "libssl-dev"
                      "liblz4-dev"
                      "build-essential"
                      "pkg-config"])
     (c/exec* "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y")
     (ensure-git-cloned
      "https://github.com/readysettech/readyset.git"
      ref
      "/opt/readyset")
     (c/exec* "~/.cargo/bin/rustup install $(</opt/readyset/rust-toolchain)")
     (info node "compiling" bin)
     (c/cd "/opt/readyset"
           (c/exec "~/.cargo/bin/cargo" "build" "--release" "--bin" bin)
           (c/exec "mv"
                   (str "target/release/" bin)
                   "/usr/local/bin/")))))


(defn authority-address
  [test]
  (str (name (nodes/node-with-role test :node-role/consul))
       ":8500"))

(defn upstream-db-url
  "Returns the upstream DB URL for the given test"
  [test]
  (str "postgresql://"
       rs/pguser ":" rs/pgpassword
       "@" (nodes/node-with-role test :node-role/upstream)
       "/" rs/pgdatabase))

(defn start-readyset-adapter! [test node]
  (c/su
   (cu/start-daemon!
    {:logfile "/var/log/readyset.log"
     :pidfile "/var/run/readyset.pid"
     :chdir "/"}
    "/usr/local/bin/readyset"
    :--log-level (:log-level test "info")
    :--no-color
    :--deployment deployment
    :-a "0.0.0.0:5432"
    :--controller-address "0.0.0.0"
    :--external-address (net/ip (name node))
    :--authority-address (authority-address test)
    :--upstream-db-url (upstream-db-url test)
    :--disable-upstream-ssl-verification
    :--embedded-readers
    :--reader-replicas (str (nodes/num-adapters test)))))

(defn kill-readyset-adapter!
  "Kill the readyset adapter process running on the current node."
  [& [_test _node]]
  (c/su
   (cu/stop-daemon! "/var/run/readyset.pid")))

(defn start-readyset-server! [test node]
  (c/su
   (c/exec :mkdir :-p storage-dir)
   (cu/start-daemon!
    {:logfile "/var/log/readyset-server.log"
     :pidfile "/var/run/readyset-server.pid"
     :chdir "/"}
    "/usr/local/bin/readyset-server"
    :--log-level (:log-level test "info")
    :--no-color
    :--deployment deployment
    :--storage-dir storage-dir
    :-a "0.0.0.0"
    :--external-address (net/ip (name node))
    :--authority-address (authority-address test)
    :--upstream-db-url (upstream-db-url test)
    :--allow-full-materialization
    :--disable-upstream-ssl-verification
    :--no-readers
    :--reader-replicas (str (nodes/num-adapters test)))))

(defn kill-readyset-server!
  "Kill the readyset server process running on the current node."
  [& [_test _node]]
  (c/su
   (cu/stop-daemon! "/var/run/readyset-server.pid")))
