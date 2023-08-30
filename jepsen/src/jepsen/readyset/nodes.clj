(ns jepsen.readyset.nodes)

(defn node->role
  [test]
  (zipmap
   (:nodes test)
   (concat [:node-role/load-balancer
            :node-role/consul
            :node-role/readyset-server
            :node-role/upstream]
           (repeat :node-role/readyset-adapter))))

(defn role->node
  [test]
  (into {} (map (comp vec reverse)) (node->role test)))

(defn node-role
  "Given a test and a node in that test, returns what will be running on that
  node for the test, represented as a keyword in
  `#{:node-role/consul
     :node-role/readyset-adapter
     :node-role/load-balancer
     :node-role/readyset-server
     :node-role/upstream}`.

  Note that we must always have at least 5 nodes to run a test.

  Roles will be assigned to nodes in the following order:

    1. `:node-role/load-balancer`
    2. `:node-role/consul`
    3. `:node-role/readyset-server`
    4. `:node-role/upstream`
    5. ... and all remaining nodes will have `:node-role/readyset-adapter`"
  [test node]
  (assert
   (>= (count (:nodes test)) 5)
   "Must have at least 5 nodes to run a high-availability ReadySet cluster")
  (get (node->role test) node))

(defn node-with-role
  "Returns the first node with the given role in the given test"
  [test role]
  (get (role->node test) role))

(defn nodes-with-role
  "Returns a list of the nodes with the given role in the given test"
  [test role]
  (->> test
       node->role
       (filter (comp #{role} val))
       (map key)))

(defn adapter-nodes
  "Returns a sequence of adapter nodes in the given test"
  [test]
  (->> test :nodes (drop 4)))

(defn num-adapters
  "Returns the number of adapter instances that will be running in the given
  test"
  [test]
  (- (count (:nodes test)) 4))

(defn grudge
  "Converts a grudge in terms of node roles to a grudge that Jepsen understands

  In Jepsen, grudges are maps from nodes to collections of nodes they should
  reject messages from. This function takes that map in terms of node roles, and
  returns a function of the kind accepted by eg `jepsen.nemesis/partitioner`
  from a list of nodes to a Jepsen grudge."
  [grudge]
  (fn [nodes]
    (let [test {:nodes nodes}]
      (into
       {}
       (mapcat (fn [[node frenemies]]
                 (for [ns (nodes-with-role test node)]
                   [ns (mapcat (partial nodes-with-role test) frenemies)])))
       grudge))))

(comment
  (def example-nodes (map #(str "node-" %) (range 10)))
  (def example-test {:nodes example-nodes})

  (nodes-with-role example-test :node-role/readyset-adapter)

  ((grudge {:node-role/readyset-server [:node-role/readyset-adapter]})
   example-nodes)
  )
