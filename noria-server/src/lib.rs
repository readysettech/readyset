//! Hello! Welcome to Noria.
//!
//! Noria is a database built to provide incrementally maintained materialized views for a known
//! set of queries. This can improve application performance drastically, as all computation is
//! performed at write-time, and reads are equivalent to cache reads. This is the server side of
//! the Noria codebase. If you are looking for the client bindings for Noria, you want the
//! [`noria`](https://crates.io/crates/noria) crate instead.
//!
//! This page describes the inner workings of the Noria server at a relatively high level of
//! abstraction. If you have not done so already, you should go read the [Noria
//! paper](https://jon.tsp.io/papers/osdi18-noria.pdf) before you continue.
//!
//! # Interfacing with Noria
//!
//! In Noria, a user provides only the *base types* that can arrive into the system (i.e., the
//! possible writes Noria will observe; see `noria::Input`), and the queries the application cares
//! about over those writes. Each such query is called a *view*. Queries are expressed through
//! relational SQL statements that operate over base tables and other views.
//!
//! Ultimately, the base tables, intermediate operators, and views are assembled into a single
//! directed, acyclic data flow graph. The leaf nodes of the graph are the views (see
//! `dataflow::node::special::Reader`), and the root nodes are the base types (see
//! `dataflow::node::special::Base`). Updates flow along the edges, and represent changes to the
//! output of the source operator over time. The conversion from the SQL-like queries specified by
//! the user to this internal graph representation takes place through a number of optimization
//! steps, which are (or will be) described in the `mir` crate.
//!
//! # Processing client writes
//!
//! When Noria receives a write, it initially sends it to its base table node. This node eventually
//! (see `dataflow/src/group_commit.rs`) injects a batch of these writes as an update into the
//! data-flow. As the update flows through operators on its way to the leaf views (i.e., the
//! application's query results), the nodes modify the update by applying their respective
//! operators. We call this *feed-forward propagation*. For example, an aggregation (see
//! `dataflow::ops::grouped`) queries its current state, updates that state based on the received
//! record, and then forwards an update that contains the updated state. Operators like joins may
//! also query other views in order to compute derived records. For example, a two-way join of `A`
//! and `B` will conceptually query back to `B` upon receiving an `A` record to construct the
//! resulting output set; we call such a query an *upquery*.
//!
//! # Stateful operators
//!
//! Nodes in the data-flow can be *materialized*, which indicates that Noria should keep the
//! current state of those nodes in memory so that their state can be efficiently queried. The
//! leaves of the data-flow are always materialized so that the application can efficiently against
//! them (see `dataflow::backlog`). Nodes that hold operators that must query their own state
//! (e.g., aggregations) are also materialized, as are any nodes that have children that issue
//! upqueries (see `dataflow::state`). Noria automatically derives how node state should be indexed
//! using operator semantics (the group by field for an aggregation is a good candidate for
//! example).
//!
//! # Data-flow execution
//!
//! The data-flow is logically divided into *thread domains* (see `dataflow::Domain`). Domains are
//! always processed atomically -- two threads can never operate on the same shard of one domain at
//! the same time. Specifically, each Noria worker runs a [`tokio`](https://docs.rs/tokio) thread
//! pool, and each domain is placed in its own "task" (see `Replica` in `src/controller/mod.rs`).
//! Noria sends updates across domain boundaries using TCP if the source and destination domains
//! are hosted by different workers, and using in-memory channels otherwise (see `noria::channel`).
//!
//! When a domain receives an update, it processes that update to completion before it progresses
//! to the next update. This approach has a number of advantages compared to the "one thread per
//! node" model:
//!
//!  - Within a domain, no locks are needed on internal materialized state. This significantly
//!    speeds up operators that need to query state, such as joins, if they are co-located with the
//!    nodes whose state they have to query. If a join had to take a lock on its ancestors' state
//!    every time it received a new record, performance would suffer.
//!  - Domains provide a natural machine boundary, with network connections being used in place of
//!    edges that cross domains.
//!  - If there are more nodes in the graph than there are cores, we can now be smarter about how
//!    computation is split among cores, rather than have all nodes compete equally for
//!    computational resources.
//!
//! However, it also has some drawbacks:
//!
//!  - We no longer get thread specialization, and thus may reduce cache utilization efficiency.
//!    In the case where there are more nodes than threads, this would likely have been an issue
//!    regardless.
//!  - If multiple domains need to issue upqueries to some shared ancestor, they now need to
//!    locally re-materialize that state, which causes duplication, and thus memory overhead.
//!
//! # Core components
//!
//! If you're going to hack on Noria, there are some files, modules, and types that you should
//! know:
//!
//!  - `ControllerInner` in `src/controller/inner.rs`, which is held by a *single* worker, and
//!    "owns" the currently executing data-flow graph. If a client wishes to add or modify queries,
//!    create new handles to base tables or views, or inspect the current data-flow, they end up
//!    interfacing with a `ControllerInner`.
//!  - `Migration` in `src/controller/migrate/mod.rs`, which orchestrates any changes to the
//!    running data-flow. This includes drawing domain boundaries, setting up sharding, and
//!    deciding what nodes should have materialized state. This planning process is split into many
//!    files in the same directory, but the primary entry point is `Migration::commit`, which may
//!    be worth reading top-to-bottom.
//!  - `Packet` in `dataflow/src/payload.rs`, which holds all the possible messages that a domain
//!    may receive. Of particular note are `Packet::Message`, the packet type for inter-domain
//!    updates; `Packet::Input`, the packet type for client writes to base tables (see also
//!    `noria::Input`); and `Packet::ReplayPiece`, the packet type for upquery responses.
//!  - `Domain` in `dataflow/src/domain/mod.rs`, which contains all the logic used to execute
//!    cliques of Noria operators that are contained in the same domain. Its primary entry point is
//!    `Domain::on_event`, which gets called whenever there are new `Packets` for the domain. You
//!    may also want to look at `Replica` in `src/controller/mod.rs`, which is responsible for
//!    managing of all of a domain's inputs and outputs.
//!  - `Node::process` in `dataflow/src/node/process.rs`, which contains all the logic for how an
//!    update is executed at a single operator. Most of the operator implementations are in
//!    `dataflow/src/ops/`, though some are in `dataflow/src/node/special/`.
//!
//! This crate also provides `LocalAuthority`, which allows you to _embed_ a `noriad` worker, and
//! not bother with setting up ZooKeeper or multiple workers. This provides no fault-tolerance and
//! no multi-machine operations, but can be a convenient way to set things up for development and
//! testing. See `Builder::build_local` or the `basic-recipe` example for details.
//!
//! # I'm a visual learner
//!
//! To provide some holistic insight into how the system works, an instructive exercise is to
//! trace through what happens internally in the system between when a write comes in and a
//! subsequent read is executed. For this example, we will be using the following base types and
//! views:
//!
//!  - `Article` (or `a`), a base table with two fields, `id` and `title`.
//!  - `Vote` (or `v`), another base table two fields, `user` and `id`.
//!  - `VoteCount` (or `vc`), a view equivalent to:
//!
//!     ```sql
//!     SELECT id, COUNT(user) AS votes FROM Vote GROUP BY id
//!     ```
//!
//!  - `ArticleWithVoteCount` (or `awvc`), a view equivalent to:
//!
//!     ```sql
//!     SELECT a.id, a.title, vc.votes
//!     FROM a JOIN vc ON (a.id = vc.id)
//!     ```
//!
//! Together, these form a data flow graph that looks like this:
//!
//! ```text
//! (a)      (v)
//!  |        |
//!  |        +--> [vc]
//!  |              |
//!  |              |
//!  +--> [awvc] <--+
//! ```
//!
//! In fact, this is almost the exact graph used by the `votes` test in `src/integration.rs`, so
//! you can go look at that if you want to see the code. Its data-flow construction looks roughly
//! like this (modified for clarity).
//!
//! ```no_run
//! # fn main() {
//! # // we don't have access to any internal noria-server types :'(
//! # struct ControllerInner;
//! # impl ControllerInner { fn migrate<F>(&self, _: F) where F: FnOnce(M) { } }
//! # struct M;
//! # impl M {
//! #  fn add_ingredient<A, B, C>(&self, _: A, _: B, _: C) -> () {}
//! #  fn maintain<A, B>(&self, _: A, _: B) -> () {}
//! # }
//! # #[derive(Default)]
//! # struct Base;
//! # enum Aggregation { COUNT }
//! # impl Aggregation { fn over<A, B, C>(&self, _: A, _: B, _: C) -> () { } }
//! # #[derive(Default)]
//! # struct Join;
//! # impl Join { fn new<A, B, C, D>(_: A, _: B, _: C, _: D) -> Join { Join } }
//! # enum JoinType { Inner }
//! # enum JoinSource { B(usize, usize), L(usize), R(usize) };
//! # let g = ControllerInner;
//! // assume we have some g that is a ControllerInner
//! g.migrate(|mig| {
//!     // base types
//!     let article = mig.add_ingredient("article", &["id", "title"], Base::default());
//!     let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());
//!
//!     // vote count is an aggregation over vote where we group by the second field ([1])
//!     let vc = mig.add_ingredient("vc", &["id", "votes"], Aggregation::COUNT.over(vote, 0, &[1]));
//!
//!     // add final join using first field from article and first from vc.
//!     // joins are trickier because you need to specify what to join on. the B(0, 0) here
//!     // signifies that the first field of article and vc should be equal,
//!     // and the second field can be whatever.
//!     let j = Join::new(article, vc, JoinType::Inner, vec![
//!                JoinSource::B(0, 0),
//!                JoinSource::L(1),
//!                JoinSource::R(1)
//!             ]);
//!     let awvc = mig.add_ingredient("end", &["id", "title", "votes"], j);
//!
//!     // we want to be able to query awvc_q using "id"
//!     let awvc_q = mig.maintain(awvc, 0);
//!
//!     // returning will commit the migration and start the data flow graph
//! });
//! # }
//! ```
//!
//! This may look daunting, but reading through you should quickly recognize the queries from
//! above. Note that we didn't specify any domains in this migration, so Noria will automatically
//! put each node in a separate domain. Normally, clients will just provide Noria with the
//! equivalent SQL statements, and `mir` will automatically construct the appropriate data-flow
//! program.
//!
//! ## Tracing a write
//!
//! Let us see what happens when a new `Article` write enters the system. This happens by passing
//! the new record to the put function on a mutator obtained for article.
//!
//! ```no_run
//! # extern crate noria;
//! # extern crate tokio;
//! # extern crate futures;
//! use tokio::prelude::*;
//! # fn main() {
//! # let zookeeper_addr = "";
//! let mut rt = tokio::runtime::Runtime::new().unwrap();
//! let mut db = noria::SyncControllerHandle::from_zk(zookeeper_addr, rt.executor()).unwrap();
//! let mut article = db.table("article").unwrap();
//! article.insert(vec![noria::DataType::from(1), "Hello world".into()]).wait();
//! # }
//! ```
//!
//! The `.into()` calls here turn the given values into Noria's internal `DataType`. Noria records
//! are always represented as vectors of `DataType` things, where the `n`th element corresponds to
//! the value of the `n`th column of that record's view. Internally in the data flow graph, they
//! are also wrapped in the `Record` type to indicate if they are "positive" or "negative" (we'll
//! get to that later), and again in the `Packet` type to allow meta-data updates to propagate
//! through the system too.
//!
//! Our write (now a `Packet`) next arrives at the `article` base table in the data-flow. Or, more
//! specifically, it is received by the domain that contains `article`. `Domain::on_event` checks
//! that the update shouldn't be held for batching (see `dataflow/src/group_commit.rs`), and
//! then calls into `Domain::dispatch`, which again calls
//! `dataflow::node::special::Base::on_input`. This does pretty much nothing since we don't have
//! any altered columns in this base table.
//!
//! Once `article` has returned the update, Noria must then forward the update to all of its
//! children. In this case, the only child is the join. Since joins require their inputs to be
//! materialized so that they can be efficiently queried when a record arrives from the other side
//! of the join, `article`'s data-flow node is materialized, so the update is also added to that
//! materialization before it reaches the join.
//!
//! Following the same chain as above, we end up at the `on_input` method of the `Join` type in
//! `dataflow/src/ops/join.rs`. It's a little complicated, but trust that it does basically what
//! you'd expect a join to do:
//!
//!  - query the other side of the join by looking up the join key in the materialization we
//!    have for that other ancestor (this is an upquery).
//!  - look for anything that matches the join column(s) on the current record.
//!  - emit the carthesian product of those records with the one we received.
//!
//! It also sorts the batch of updates, like most Noria operators do, so that it only performs one
//! lookup per key. In this particular case, the join finds no records in `vc`, and so no records
//! are emitted. If this were a `LEFT JOIN`, we would instead get a row where the vote count is 0.
//!
//! Since we asked `Migration` to "maintain" the output of `awvc`, `awvc` has a single child node
//! which is a `Reader`. `Reader` keeps materialized state that can be accessed by applications by
//! issuing reads on a `noria::View`. This materialized state uses
//! [`evmap`](https://github.com/jonhoo/evmap), which is optimized for concurrent reads and writs.
//! Since `awvc` produced no updates this time around, no changes are made to the `Reader`. When
//! control returns to the domain, it observes that `awvc` has no further descendants, and does not
//! propagate the (empty) update any further.
//!
//! ## Let's Vote
//!
//! Let's next trace what happens when a `Vote` is introduced into the system using
//!
//! ```no_run
//! # extern crate noria;
//! # extern crate tokio;
//! # extern crate futures;
//! use tokio::prelude::*;
//! # fn main() {
//! # let zookeeper_addr = "";
//! let mut rt = tokio::runtime::Runtime::new().unwrap();
//! let mut db = noria::SyncControllerHandle::from_zk(zookeeper_addr, rt.executor()).unwrap();
//! let mut vote = db.table("vote").unwrap();
//! vote.insert(vec![noria::DataType::from(1000), 1.into()]).wait();
//! # }
//! ```
//!
//! We will skip the parts related to the `vote` base table, since they are equivalent to the
//! `article` flow. The output of the `vote` node arrives at `vc`, an aggregation. This ends up
//! calling `GroupedOperator::on_input` in `dataflow/src/ops/grouped/mod.rs`. If you squint at it,
//! you can see that it first queries its own materialized state for the current value for the
//! `GROUP BY` key of the incoming record, and then uses `GroupedOperation` to compute the new,
//! updated value. In our case, this ends up calling `Aggregator::to_diff` and `Aggregator::apply`
//! in `ops/grouped/aggregate.rs`. As expected, these functions add one to the current count. The
//! grouped operator then, as expected, emits a record with the new count. However, it also does
//! looks to do something slightly weird --- it first emits a *negative* record. Why..?
//!
//! Negative records are Noria's way of signaling that already materialized state has changed. They
//! indicate to descendant views that a past record is no longer valid, and should be discarded. In
//! the case of our vote, we would get the output:
//!
//! ```diff
//! - [id=1, votes=0]
//! + [id=1, votes=1]
//! ```
//!
//! Since these are sent within a single `Packet`, the descendants know the vote count was
//! incremented (and not, say, removed, and then re-appearing with a value of one). The negative
//! records are also observed by `dataflow::Node::process`, which deletes the old materialized
//! result row (because of the `-`), and then inserts the new materialized result row (because of
//! the `+`). Note that in this *particular* example, since there were no votes to article 1 prior
//! to the vote we inserted, the aggregation will *not* produce the negative. But it's worth
//! knowing about anyway!
//!
//! After the aggregation, the update proceeds to the join, which does an upquery into `article`,
//! finds the matching article, and produces a single joined record, which it then sends on to
//! the `Reader` leaf node. The `Reader` then applies that update to its state so that the entry
//! for article 1 becomes visible.
//!
//! ## What about joins and negatives?
//!
//! What if the aggregation *did* produce a negative and a positive because the vote count changed
//! from some previous value to a new one? In that case, the update contains two records, and the
//! join performs the join *twice*, once for the negative, and again for the positive. Why is that
//! necessary? Consider the case where the system has been running for a while, and our article has
//! received many votes. After the previous vote, `awvc` emitted a record containing
//!
//! ```diff
//! + [id=1, title=Hello world, votes=42]
//! ```
//!
//! If we simply ignored the negative we received from `vc`, and performed the join for the
//! positive, we'd emit another row saying
//!
//! ```diff
//! + [id=1, title=Hello world, votes=43]
//! ```
//!
//! In the absence of any further information, the leaf materialization would then insert a
//! *second* row (well, a 43rd row) in the materialized table for our article. This would mean that
//! if someone queried for it, they would get a lot of results. In this particular example, the
//! negative from `vc` contains enough information to delete the correct output row (`id=1`), but
//! this is not always the case. We therefore have to perform the *full* join for the negative as
//! well, to produce an exact replica of the row that we are "revoking". In fact, since this is a
//! join, a single negative may produce *multiple* negative outputs, each one revoking a single
//! output record.
//!
//! So, from the update received from `vc`, `awvc` would perform two joins, and eventually produce
//! a new update with the records
//!
//! ```text
//! - [id=1, title=Hello world, votes=42]
//! + [id=1, title=Hello world, votes=43]
//! ```
#![feature(allow_fail)]
#![feature(optin_builtin_traits)]
#![feature(box_patterns)]
#![feature(box_syntax)]
#![feature(nll)]
#![feature(try_blocks)]
#![feature(fnbox)]
#![feature(vec_remove_item)]
#![feature(crate_visibility_modifier)]
#![deny(missing_docs)]
#![deny(unused_extern_crates)]
//#![deny(unreachable_pub)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate nom;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;

mod builder;
mod controller;
mod coordination;
mod handle;
mod startup;
mod worker;

#[cfg(test)]
mod integration;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum ReuseConfigType {
    Finkelstein,
    Relaxed,
    Full,
    NoReuse,
}

pub use crate::builder::Builder;
pub use crate::handle::{Handle, SyncHandle};
pub use dataflow::{DurabilityMode, PersistenceParameters};
pub use noria::consensus::LocalAuthority;
pub use noria::*;
pub use petgraph::graph::NodeIndex;

use dataflow::DomainConfig;
use std::time;

pub(crate) fn block_on<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    use tokio::prelude::*;
    use tokio_threadpool::blocking;
    let mut wrap = Some(f);
    future::poll_fn(|| blocking(wrap.take().unwrap()))
        .wait()
        .unwrap()
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
crate struct Config {
    crate sharding: Option<usize>,
    crate partial_enabled: bool,
    crate domain_config: DomainConfig,
    crate persistence: PersistenceParameters,
    crate heartbeat_every: time::Duration,
    crate healthcheck_every: time::Duration,
    crate quorum: usize,
    crate reuse: ReuseConfigType,
    crate threads: Option<usize>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            sharding: Some(2),
            #[cfg(not(test))]
            sharding: None,
            partial_enabled: true,
            domain_config: DomainConfig {
                concurrent_replays: 512,
                replay_batch_timeout: time::Duration::new(0, 10_000),
            },
            persistence: Default::default(),
            heartbeat_every: time::Duration::from_secs(1),
            healthcheck_every: time::Duration::from_secs(10),
            quorum: 1,
            reuse: ReuseConfigType::Finkelstein,
            #[cfg(any(debug_assertions, test))]
            threads: Some(2),
            #[cfg(not(any(debug_assertions, test)))]
            threads: None,
        }
    }
}

/// Just give me a damn terminal logger
pub fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

#[cfg(test)]
mod tests {
    use super::*;
    use noria::consensus::ZookeeperAuthority;
    use std::{sync::Arc, thread, time};
    use tokio::prelude::*;

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    #[allow_fail]
    fn it_works_default() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_default").unwrap();
        {
            tokio::runtime::current_thread::block_on_all(
                Builder::default().start(Arc::new(authority)),
            )
            .unwrap();
            thread::sleep(time::Duration::from_millis(100));
        }
        thread::sleep(time::Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    #[allow_fail]
    fn it_works_blender_with_migration() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        use rand::Rng;
        let zk = format!(
            "127.0.0.1:2181/it_works_blender_with_migration_{}",
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(8)
                .collect::<String>()
        );
        let authority = ZookeeperAuthority::new(&zk).unwrap();
        tokio::runtime::current_thread::block_on_all(
            Builder::default()
                .start(Arc::new(authority))
                .and_then(|mut c| c.install_recipe(r_txt)),
        )
        .unwrap();
    }

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    fn it_works_default_local() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        {
            let _c = Builder::default().start_simple().unwrap();
            thread::sleep(time::Duration::from_millis(100));
        }
        thread::sleep(time::Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration_local() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = Builder::default().start_simple().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
    }
}
