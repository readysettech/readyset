# Migrations

`noria/server/src/controller/migrate` orchestrates any changes to the running data-flow. This includes drawing domain boundaries, setting up sharding, and
deciding what nodes should have materialized state. This planning process is split into many files in the same directory, but the primary entry point is `Migration::commit`, which may be worth reading top-to-bottom.

## Sharding 
Sharder traverses graph for new nodes and figures out how to shard them. (e.g. if aggregate node, then shard by column you're aggregating over, etc). 

`need_sharding` is a list of ways in which a domain should be sharded. 

Given constraints, sharder figures out how to actually shard the domain — this could get awkward, e.g. when doing a join between two views sharded in diff ways. 

The `reshard` function makes sure edges are sharded correctly such that it matches the destination sharding (i.e. injects sharder, shard merger, and does the wiring in between). In general, the fall back is to force everything to be unsharded. By default, this code might do some unnecessary shuffles, so there's some cleanup code that makes sure this doesn't happen. The `validate` function then checks to make sure that none of the sharding is illegal. 

After figuring out sharding, `commit` then assigns domains. In general, we want to keep nodes in the same domain as their parents to the greatest extent possible, although sometimes you do want to put a child node in a different domain so that you have more work units (more cores, machines, etc). 

`assign` figures out if you should put a node into an existing domain or a new one. 
- Shard mergers need to be in their own domain because they're merging shards and so they can't be placed in the same domain in all of their ancestors which are sharded. 
- Readers can be put in their domains always (very little cost to doing so). 
- Highly interconnected queries often are in one domain — this can be bad. Current manual workaround is if the name of the view starts with `boundary`, you can indicate that there's supposed to be a thread boundary there. 
- If parents are in different domains, which do we choose or do we make a new one? Heuristic currently tries to pick up on common patterns like if child of sharder can't be in same domain. Also tries to make sure we don't have an A-B-A type of path so we don't bounce back and forth between domains. Favors minimal number of domains bc of joins - inputs to join need to be materialized in same domain as a join. Want to avoid duplicating state by materializing join table in one domain rather than two. Rule of thumb: place domain boundaries above aggregations, avoid them above joins. 

`routing` sends up ingress/egress nodes to domains.

`augmentation` is the process of telling thread domains that are already running about changes to the set of nodes they already have (e.g. new nodes, new incoming outgoing edge, etc) 

`connect` tells all egress nodes the addresses to use for destinations `UpdateEgress` messages, `add` adds all of the ingress/egress nodes. Let's say we have a thread domain that's sharded —> egress in thread domain has child in other thread domain. Both sharded in same domain. Want to make sure that egress of shard 1 sends to ingress of shard 1 in child domain. 

`materialization/mod.rs` and `materialization/plan.rs` sets up all upquery paths and indexes. 

`Materializations` struct keeps track of current materializations in the system (+ index info). 

`commit` says we're done adding new nodes and applies changing to materializations. This includes setting up replay paths, adding new indices to existing materializations, and populating existing materializations. 

`FrontierStrategy` suffix of dataflow graph indicates that we materialize but evict the moment the read has happened.

(WIP)