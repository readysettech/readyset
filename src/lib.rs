//! Welcome to Soup.
//!
//! Soup is a database built to provide incrementally maintained materialized views for a known set
//! of queries. This can improve application performance drastically, as all computation is
//! performed at write-time, and reads are equivalent to cache reads.
//!
//! In Soup, a user provides only the *base types* that can arrive into the system (i.e., the
//! possible writes Soup will observe), and the queries the application cares about over those
//! writes. Each such query is called a *view*. Queries can be expressed solely in terms of the
//! base write types, or they can read from other views, producing *derived views*.
//!
//! Ultimately, the views and base types are assembled into a single directed, acyclic data flow
//! graph. The nodes of the graph are the views (the root nodes are the base types), and the edges
//! are channels along which records are forwarded as new values are computed.
//!
//! When a new write arrives into the system, it is initially sent to its base type's node. This
//! node will forward that new record to all views that query that base type. These views will
//! perform some computation on the new record before forwarding any *derived* records to views
//! that query each of them in turn. We call this *feed-forward propagation*. For example, an
//! aggregation will query its current state, update that state based on the received record, and
//! then forward a record with its updated state. Nodes such as joins may also query other views in
//! order to compute derived records (e.g. a two-way join of `A` and `B` will *query back* to `B`
//! upon receiving an `A` record to construct the resulting output set).
//!
//! Nodes in the graph can be marked as *materialized*, indicating that Soup should keep the
//! current state of those nodes for efficient querying. The leaves of the DAG will generally be
//! materialized, but internal nodes that need to query their own state (e.g., aggregations) could
//! also be for performance reasons. If a node is materialized, any record it forwards is kept in
//! an in-memory data structure (managed by [`shortcut`](https://github.com/jonhoo/shortcut)). Soup
//! will also use the semantics of the given view's computation to determine what indices should be
//! added to the materialization (the group by field for an aggregation is a good candidate for
//! example).
//!
//! # Code structure
//!
//! At a high level, Soup consists of two main components that function relatively independently:
//!
//!  - `flow::FlowGraph`, which constructs the data flow graph, manages the propagation of records
//!    between different views, and faciliates views querying other views. Much of the code in
//!    `FlowGraph` is there to carefully manage the concurrency in the data flow computation such
//!    that views don't see inconsistent results when observing other views.
//!  - `ops::*`, which provides the different view operations provided by Soup, as well as some
//!    shared functionality like materialization (see the `Node` type in `ops/mod.rs`).
//!
//! There are also a number of important secondary components that the components above depend on,
//! but aren't immediately useful on their own:
//!
//!  - `DataType` in `query/`, which encapsulates the data types provided by Soup to end-users.
//!  - `Update` and `Record` in `ops/mod.rs`, which are used pervasively in the code to move
//!    records between views.
//!  - `Query` in `query/`, which is used to filter and project records when sent from one view to
//!    another. `Query` objects are currently view-local -- that is, each node knows what queries
//!    to use when querying their ancestors, but this information is not available to other nodes.
//!    Thus, queries are not applied to records that are fed forward unless a view decides to do
//!    so.
//!  - `BufferedStore` in `backlog/`, which provides a thin layer atop `shortcut` giving access to
//!    short-term time-travel queries.
//!
//! # Dependencies
//!
//! Soup critically depends on a few libraries that should be understood before delving into
//! internal Soup development. **Users** of Soup generally need not familiarize themselves with
//! these libraries. The documentation for these libraries is generally pretty good, so you should
//! start there if you want to know more.
//!
//!  - [`shortcut`](https://github.com/jonhoo/shortcut) provides storage and query support for
//!    the materialized views.
//!  - [`petgraph`](https://github.com/bluss/petgraph) provides graph construction and traversal
//!    mechanisms that are used by `FlowGraph` to maintain the data flow graph.
//!  - [`clocked-dispatch`](https://github.com/jonhoo/clocked-dispatch) provides a serialization
//!    mechanism that assigns records that are sent across disparate channels (i.e., to different
//!    base types) monotonically increasing global timestamps.
//!
//! # Data flow
//!
//! To provide some holistic insight into how the system works, an instructive exercise is to
//! trace through what happens internally in the system between when a write comes in and a
//! subsequent read is executed. For this example, we will be using the following base types and
//! views:
//!
//!  - `Article` (or `a`), a base type with two fields, `id` and `title`.
//!  - `Vote` (or `v`), another base type two fields, `user` and `id`.
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
//! In fact, this is almost the exact graph used by the `votes` test in `tests/lib.rs`, so you can
//! go look at that if you want to see the code. It looks roughly like this (some details omitted
//! for clarity)
//!
//! ```rust,ignore
//! let mut g = FlowGraph::new();
//!
//! // base types
//! let a = g.incorporate(&["id", "title"], Base {});
//! let v = g.incorporate(&["user", "id"], Base {});
//!
//! // vote count is pretty straightforward
//! let vc = g.incorporate(&["id", "votes"], Aggregation::COUNT.over(v, 0));
//!
//! // joins are tricky because you need to specify what to join on. we use the following structure
//! // where columns with the same non-zero values across ancestors are joined by equality.
//! let join = hashmap!{
//!   article => vec![1, 0],
//!   vc => vec![1, 0],
//! };
//!
//! // emit first and second field from article (id + title)
//! // and second field from right (votes)
//! let emit = vec![(article, 0), (article, 1), (vc, 1)];
//!
//! // make the join already!
//! let awvc = g.incorporate(&["id", "title", "votes"], Joiner::new(emit, join));
//!
//! // and start the data flow graph
//! let (put, get) = g.run(10);
//! ```
//!
//! This may look daunting, but reading through you should quickly recognize the queries from
//! above.
//!
//! When you `run` the `FlowGraph`, it will set up a bunch of data structures used for bookkeping,
//! set up channels for the different views to talk to each other, and then spin up a thread for
//! every node in the graph. The main loop for every node is in `FlowGraph::inner`. It reads
//! incoming updates, does some bookkeping to keep the system consistent (we'll get to that later),
//! processes the updates using the inner operation of the node (this is where the `ops::*`
//! components come into play), and then forwards the resulting update to any descendant views.
//!
//! ## Tracing a write
//!
//! Let us see what happens when a new `Article` write enters the system. This happens by sending
//! the write on one of the channels in the `put` map returned by `FlowGraph::run`
//!
//! ```rust,ignore
//! put[&article].send(vec![1.into(), "Hello world".into()]);
//! ```
//!
//! The `.into()` calls here turn the given values into Soup's internal `DataType`. Soup records
//! are always represented as vectors of `DataType` things, where the `n`th element corresponds to
//! the value of the `n`th column of that record's view. Internally in the data flow graph, they
//! are also wrapped in the `Record` type to indicate if they are "positive" or "negative" (we'll
//! get to that later), and again in the `Update` type to allow meta-data updates to propagate
//! through the system too (though that's not currently used). That wrapping is taken care of by
//! dedicated threads that `FlowGraph::run` spins up.
//!
//! Records that enter the system are first assigned a unique timestamp by `clocked_dispatch`. This
//! timestamp is later used to avoid inconsitencies in the face of concurrency in the system. The
//! write (now an `Update`) then arrives at the `Article` node in the graph. `FlowGraph::inner`
//! checks that the update shouldn't be held back (we'll get back to that later -- are you noticing
//! a pattern?), and then calls `inner.process`.
//!
//! This method, defined on the `View` trait in `flow/mod.rs`, dictates the feed-forward behavior
//! of a node. All Soup views currently hold nodes of the enum type `NodeType` (defined in
//! `ops/mod.rs`), which can be any of the operation types supported by Soup (see `ops/*.rs`).
//! `ops/mod.rs` implements the `View` trait for `NodeType`, and specifically implements the
//! `process` method called by `FlowGraph::inner` above. It simply dispatches the call to the
//! `.process` method of *its* `.inner`, which is one of the concrete Soup operation types. In this
//! particular case, a `Base`, since `Article` is a base type.
//!
//! `Base` nodes do little more than juggle the timestamps around a little to ensure that they are
//! also propagated inside a record. The original update is quickly returned, at which point the
//! `NodeType::process` method continues. Since the node is materialized (which all base nodes are),
//! the record is persisted through `BufferedStore` (think of it as simple row-based storage for
//! now). The original update is then returned again, which causes control to return all the way
//! back to `FlowGraph::inner`. It then finds any descendants of `Article` in the view graph
//! (`awvc` in our example), and forwards `Article`'s output record there.
//!
//! ## Going deeper
//!
//! The next thing that happens is that `awvc` receives the new `Article`. Since the base node
//! didn't modify it beyond copying a timestamp, we receive essentially the same record as
//! `Article` did above. Following the same chain as above, we end up at the `process` method of
//! the `Joiner` type in `ops/join.rs`. It's a little complicated, but trust that it does basically
//! what you'd expect a join to do --- query the other side of the join for anything that matches
//! the join column(s) on the current record, and emit the carthesian product of those records with
//! the one we received. In this particular case, we get no records, and so no records are emitted.
//!
//! > Hearing this, you may hesitate, and protest "but shouldn't we get an article with `votes =
//! > 0`?". Well, yes and no. That would be a `LEFT JOIN`, which Soup doesn't currently support.
//! > However, it would be a *really* nice feature to have, so maybe you should look into it?
//!
//! While `awvc` is materialized, `NodeType::process` will see that no records were produced, and
//! so no new rows will be added to the materialized state of `awvc`. When control finally returns
//! to `FlowGraph::process`, it will observe that `awvc` has no descendants, and will not propagate
//! the (empty) update any further.
//!
//! ## Let's Vote
//!
//! Let's next trace what happens when a `Vote` is introduced into the system using
//!
//! ```rust,ignore
//! put[&vote].send(vec![1000.into(), 1.into()]);
//! ```
//!
//! We will skip the parts related to the `Vote` base node, since they are equivalent to the
//! `Article` flow. The output of the `Vote` node arrives at `vc`, an aggregation. This ends up
//! calling `Aggregator::process` in `ops/aggregate.rs`. If you squint at it, you can see that it
//! queries its own materialized output for the aggregated count for the `GROUP BY` (deemed to be
//! every column except the one being aggregated), updates the value based on the operator and the
//! received record(s), and then emits a record with the new count. However, the aggregation does
//! something slightly weird --- it first emits a *negative* record. How weird...
//!
//! Negative records are Soup's way of modifying materialized state. They indicate to descendant
//! views that a past record is no longer valid, and should be discarded. In the case of our vote,
//! we would get the output:
//!
//! ```text
//! - [id=1, votes=0]
//! + [id=1, votes=1]
//! ```
//!
//! Since these are sent within a single `Update`, the descendants know the vote count was
//! incremented (and not, say, removed, and then re-appearing with a value of one). The negative
//! records are also observed by `NodeType::process`, which will delete the old materialized result
//! row (because of the `-`), and then insert the new materialized result row (because of the `+`).
//! Finally, `FlowGraph::inner` will send the `Update` with both the negative and the positive to
//! `awvc`.
//!
//! ## All together now
//!
//! For the final piece of the puzzle, let us now see what happens when the above `Update` arrives
//! at `awvc`. It contains two records, and the `Joiner` will perform the join *twice*, once for
//! the negative, and again for the positive. Why is that necessary? Consider the case where the
//! system has been running for a while, and our article has received many votes. After the
//! previous vote, `awvc` emitted a record containing
//!
//! ```text
//! [id=1, title=Hello world, votes=42]
//! ```
//!
//! If we simply ignored the negative we received from `vc`, and performed the join for the
//! positive, we'd emit another row saying
//!
//! ```text
//! [id=1, title=Hello world, votes=43]
//! ```
//!
//! In the absence of any further information, the materializer in `NodeType::process` would then
//! insert a *second* row (well, a 43rd row) in the materialized table for our article, which means
//! that if someone queried for it, they would get a lot of results. In this particular example,
//! the negative from `vc` contains enough information to delete the correct output row (`id=1`),
//! but this is not always the case. We therefore have to perform the *full* join for the negative
//! as well, to produce an exact replica of the row that we are "revoking". In fact, since this is
//! a join, a single negative may produce *multiple* negative outputs, each one revoking a single
//! output record.
//!
//! So, from the `Update` received from `vc`, `awvc` will perform two joins, eventually producing a
//! new update with the records
//!
//! ```text
//! - [id=1, title=Hello world, votes=0]
//! + [id=1, title=Hello world, votes=1]
//! ```
//!
//! The materialized state will be updated by `NodeType::process`, and `FlowGraph::inner` will stop
//! propagating the `Update` since there are no descendant views.
//!
//! ## Who doesn't love a good data race
//!
//! At this point, it is worth pausing to see what happens when we consider that there may be
//! concurrent writes to the system. While there is a serializer assigning timestamps at the "top"
//! of the graph, different nodes still process updates in parallel, leading to possible races, and
//! thus data inconsistencies) in the data flow graph. Many of the mechanisms we've skipped along
//! the way (have you been keeping track?) are there precisely to tackle this problem.
//!
//! Figuring out all the things that can go wrong is left as an exercise to the reader, but let us
//! explore at least one issue that can easily arise in the graph we have constructed in this
//! (rather long) example. Consider the case where a user votes on an article *immediately* after
//! it is added. The article enters the system right before the vote, but because of scheduling,
//! the vote is handled by `vc` before the article propagates down the flow graph. Our article now
//! starts to propagate, and arrives at `awvc`, which queries `vc` for matching records. It will
//! find a match with a vote count of one, and output a single record with that value. But then,
//! the record emitted by `vc` arrives at `awvc`, it also causes a query to go to `Article`, which
//! will (obviously) return our article, causing `awvc` to emit *another* row with a vote count of
//! one. Subsequent queries to this view will now yield two identical rows!
//!
//! This may not seem so bad --- surely downstream nodes can just prune out duplicates? That seems
//! fine, until you realize that this can also happen with negative records, where the results are
//! potentially more damaging. Consider a different view graph in which some part of the graph
//! looks like this:
//!
//! ```text
//! :       :
//! A       B
//! |       |
//! +---+---+
//!     |
//!     C
//! ```
//! Say that the system is initially in a steady state where `A` and `B` both contain one record
//! each (`a` and `b` respectively). `C` has just output the record `c = a JOIN b`. Then, a new
//! negative `A` record, `-a`, and a negative `B` record, `-b`, both come in simultaneously. The
//! `-a` and `-b` records are handled by their respective nodes immediately, both of them deleting
//! the corresponding record from their materialized outputs. The `-a` then arrives at `C`. `C` now
//! has to query `B` in order to compute what negative records to produce in response to the `-a`.
//! Specifically, it must produce `-c`. However, `C`'s query against `B` will not include `b` since
//! `B` has already deleted it, and thus, `C` will not produce `-c`. Similarly, when the `-b` later
//! arrives, this too will produce no resulting negative records. Uh oh.
//!
//! ## Time is of the essence
//!
//! Soup fixes the problem outlined above, as well as a host of related problems, by enforcing two
//! invariants:
//!
//!  - A node will only process an update with timestamp `ts` if it will not in the future process
//!    any update with `ts' < ts`.
//!  - When querying another view as a result of a record with timestamp `ts`, the query will be
//!    run *as if* it was run right after all updates up to `ts` had been processed by the target
//!    view.
//!
//! The first property is the source of most of the code in `FlowGraph::inner`. Incoming updates
//! are buffered until a higher timestamp has been seen from *every* parent of the current node.
//! When the minimum across all parents is updated to `ts`, all buffered updates with `ts' < ts`
//! are processed *in order of their timestamps*.
//!
//! The second property is maintained by two independent pieces, one to ensure that the target
//! view's node is *sufficiently* up to date, the other to ensure that it's not *too* up to date.
//! The first part is trivial to enforce. Since we process an update at time `ts` only after we
//! have seen `ts' > ts` from all ancestors, we already know that all ancestors are at least as up
//! to date as they need to be.
//!
//! The second part is a bit more involved, because it requires being able to query "back in time".
//! We *could* have all nodes move in lock-step to ensure that they are never too up-to-date, but
//! this would artificially slow down the system. Instead, we buffer changes to a view's
//! materialized state for a short time, and only absorb those buffered changes into the backing
//! `shortcut` storage once we know that none of our ancestors will want to execute a query from
//! before those updates happened. Queries to materialized views are thus executed in a two-step
//! algorithm. First, `shortcut` is (efficiently) queried for all matches in the absorbed state.
//! Then, all buffered updates before the query's time are scanned, one at the time, to see if any
//! of them change the returned state. Since this scan would be slow with many buffered updates, it
//! is important that Soup absorbs fairly frequently. All of this is handled by the `BufferedStore`
//! in `backlog/mod.rs`. The logic for when it is safe to absorb a given timestamp can be seen in
//! the bottom of `FlowGraph::inner`.
//!
#![feature(optin_builtin_traits)]
#![feature(proc_macro)]
#![deny(missing_docs)]

#[cfg(feature="b_netsoup")]
#[macro_use]
extern crate serde_derive;

extern crate clocked_dispatch;
extern crate petgraph;
extern crate shortcut;

#[cfg(feature="web")]
extern crate rustc_serialize;

#[macro_use]
#[cfg(feature="web")]
extern crate rustful;

#[macro_use]
#[cfg(feature="b_netsoup")]
extern crate tarpc;

mod flow;
mod query;
mod ops;
mod backlog;

pub use flow::{FlowGraph, NodeIndex, FreshnessProbe};
pub use ops::new;
pub use ops::NodeType;
pub use ops::base::Base;
pub use ops::grouped::aggregate::{Aggregator, Aggregation};
pub use ops::grouped::concat::{GroupConcat, TextComponent};
pub use ops::join::Builder as JoinBuilder;
pub use ops::union::Union;
pub use ops::latest::Latest;
pub use query::Query;
pub use query::DataType;

#[cfg(feature="web")]
/// web provides a simple REST HTTP server for reading from and writing to the data flow graph.
pub mod web;

#[cfg(feature="b_netsoup")]
/// srv provides a networked RPC server for accessing the data flow graph.
pub mod srv;
