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
//! are paths along which records are forwarded as new values are computed. The conversion from
//! the SQL-like queries specified by the user to this internal graph representation takes place
//! through a number of optimization steps, which are described elsewhere.
//!
//! When a new write arrives into the system, it is initially sent to its base type's node. This
//! node will forward that new record to all views that query that base type. These views will
//! perform some computation on the new record before forwarding any *derived* records to views
//! that query each of them in turn. We call this *feed-forward propagation*. For example, an
//! aggregation will query its current state, update that state based on the received record, and
//! then forward a record with its updated state. Nodes such as joins may also query other views in
//! order to compute derived records (e.g. a two-way join of `A` and `B` will conceptually query
//! back to `B` upon receiving an `A` record to construct the resulting output set).
//!
//! Nodes in the graph can be *materialized*, indicating that Soup should keep the current state of
//! those nodes to allow efficient querying of that state. The leaves of the DAG will generally be
//! materialized to enable application queries against them, but internal nodes that need to query
//! their own state (e.g., aggregations) could also be for performance reasons. If a node is
//! materialized, any record it forwards is kept in an in-memory data structure. Soup will also use
//! the semantics of the given view's computation to determine what indices should be added to the
//! materialization (the group by field for an aggregation is a good candidate for example).
//!
//! The data flow graph is logically divided into *domains*. Each domain is handled by a single
//! computational entity (currently threads, but eventually different domains could be run by
//! different computers entirely). Each record that arrives to a domain is processed to completion
//! before the next update is processed. This approach has a number of advantages compared to the
//! "one thread per node" model:
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
//!  - If multiple domains need to access the state of some shared ancestor, they now need to
//!    locally re-materialize that state, which causes duplication, and thus memory overhead.
//!
//! # Code structure
//!
//! At a high level, Soup consists of a couple of main components that can generally be understood
//! in isolation, even though they interact heavily during standard operation.
//!
//!  - `flow::Blender`, which "owns" the data flow graph, and provides methods for inspecting it.
//!    `Blender` is principally used to start a `Migration` that adds new queries to the system, or
//!    removes old ones.
//!  - `flow::Migration`, which handles all the plumbing needed to hook in new queries into an
//!    existing Soup graph. This includes spinning up new `Domain`s where appropriate, and to set
//!    up channels between different domains when the data flow graph has inter-domain
//!    dependencies.
//!  - `flow::domain::*`, which sets up and runs the internal machinery of each domain. For each
//!    update that arrives `flow::domain::single` is used to determine what should be done with it,
//!    and then to process it to completion before forwarding it to any child nodes in other
//!    domains. The buffering required to implement atomic transactions also lives mainly within
//!    each domain.
//!  - `flow::Ingredient` and `ops::*`, which provide the interface for and implementation of the
//!    various computational operators supported by Soup. The most important among these is the
//!    `on_input` method, which specifies what a node does when it receives a new record, and how
//!    its output changes.
//!
//! There are also a number of important secondary components that the components above depend on,
//! but aren't immediately useful on their own:
//!
//!  - `DataType` in `query/`, which encapsulates the data types provided by Soup to end-users.
//!  - `Update` and `Record` in `ops/mod.rs`, which are used pervasively in the code to move
//!    records between views.
//!  - `BufferedStore` in `backlog/`, which provides an eventually consistent `HashMap` where
//!    readers and a single writer can operate concurrently. This is used to maintain any state
//!    that application queries can reach out and read.
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
//! ```rust
//! # use distributary::{Blender, Base, Aggregation, JoinBuilder};
//! // set up graph
//! let mut g = Blender::new();
//! let mut mig = g.start_migration();
//!
//! // base types
//! let article = mig.add_ingredient("article", &["id", "title"], Base::default());
//! let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());
//!
//! // vote count is an aggregation over vote where we group by the second field ([1])
//! let vc = mig.add_ingredient("vc", &["id", "votes"], Aggregation::COUNT.over(vote, 0, &[1]));
//!
//! // add final join using first field from article and first from vc.
//! // joins are trickier because you need to specify what to join on. the vec![1, 0] here
//! // signifies that the first field of article and vc should be equal, and the second field can
//! // be whatever.
//! let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
//!     .from(article, vec![1, 0])
//!     .join(vc, vec![1, 0]);
//! let awvc = mig.add_ingredient("end", &["id", "title", "votes"], j);
//!
//! // we want to be able to query awvc_q using "id"
//! let awvc_q = mig.maintain(awvc, 0);
//! # drop(awvc_q);
//!
//! // start the data flow graph
//! mig.commit();
//! ```
//!
//! This may look daunting, but reading through you should quickly recognize the queries from
//! above. Note that we didn't specify any domains in this migration, so Soup will automatically
//! put each node in a separate domain.
//!
//! When you `commit` the `Migration`, it will set up a bunch of data structures used for
//! bookkeping, set up channels for the different domains to talk to each other, and then spin up a
//! thread for every new domain in the graph (which is all of them). The main loop for every node
//! is in `Domain::boot`. It reads incoming updates, does some transactional bookkeping, processes
//! the updates using the inner operation of the node (this is where the `domain::single` component
//! comes into play), and then forwards the resulting update to any descendant views.
//!
//! ## Tracing a write
//!
//! Let us see what happens when a new `Article` write enters the system. This happens by passing
//! the new record to the put function on a mutator obtained for article.
//!
//! ```rust
//! # use distributary::{Blender, Base};
//! # let mut g = Blender::new();
//! # let article = {
//! # let mut mig = g.start_migration();
//! # let article = mig.add_ingredient("article", &["id", "title"], Base::default());
//! # mig.commit();
//! # article
//! # };
//! let muta = g.get_mutator(article);
//! muta.put(vec![1.into(), "Hello world".into()]);
//! ```
//!
//! The `.into()` calls here turn the given values into Soup's internal `DataType`. Soup records
//! are always represented as vectors of `DataType` things, where the `n`th element corresponds to
//! the value of the `n`th column of that record's view. Internally in the data flow graph, they
//! are also wrapped in the `Record` type to indicate if they are "positive" or "negative" (we'll
//! get to that later), and again in the `Update` type to allow meta-data updates to propagate
//! through the system too (though that's not currently used). That wrapping is taken care of by
//! the functions `Migration::commit` creates and puts in the returned `HashMap` though, so we can
//! simply put in a `Vec<DataType>`.
//!
//! We'll talk only about non-transactional reads and writes for now, and defer discussion
//! transactions until later. Our write (now an `Update`) next arrives at the `Article` node in the
//! graph. Or, more specifically, it is received by the domain that contains `Article` (and only
//! `Article`). `Domain::boot` checks that the update shouldn't be held back (we'll get back to
//! that when we talk about transactions), and then calls into `domain::single::process`, which
//! again calls `ops::Base::on_input`, which, well, does nothing. `Base` nodes simply forward the
//! updates they get in, and do no further processing, unlike most other node types.
//!
//! Once `Article` has returned, we reach a domain boundary, and thus need to use the cross-domain
//! channels set up by `Migration::commit` to forward that update to children of `Article`. In this
//! case, the only child is our join, so the domain holding that node will receive the article we
//! inserted. If you're following along with the code, the `Egress` type nodes are the ones that
//! keep the information about outgoing cross-domain connections.
//!
//! Since joins require their inputs to be materialized (so that they can be efficiently queried
//! when a record arrives from the other side of the join), the incoming record is first persisted
//! in a `domain::local::State`.
//!
//! Following the same chain as above, we end up at the `on_input` method of the `Joiner` type in
//! `ops/join.rs`. It's a little complicated, but trust that it does basically what you'd expect a
//! join to do:
//!
//!  - query the other side of the join by looking up the join key in the `domain::local::State` we
//!    have for that other ancestor.
//!  - look for anything that matches the join column(s) on the current record.
//!  - emit the carthesian product of those records with the one we received.
//!
//! In this particular case, we get no records, and so no records are emitted. If this were a `LEFT
//! JOIN`, we would, of course, instead get a row where the vote count is 0.
//!
//! Since we asked `Migration` to maintain the output of `awvc`, `awvc` has a single child node
//! which is of type `flow::node::Reader`. `Reader` keeps materialized state that can be accesses
//! by applications by calling the function returned from `Migration::maintain`. However, since
//! `awvc` produced no updates this time around, no changes are made to the `Reader`.
//! When control finally returns to the domain it will observe that `awvc` has no descendants, and
//! will not propagate the (empty) update any further.
//!
//! ## Let's Vote
//!
//! Let's next trace what happens when a `Vote` is introduced into the system using
//!
//! ```rust
//! # use distributary::{Blender, Base};
//! # let mut g = Blender::new();
//! # let vote = {
//! # let mut mig = g.start_migration();
//! # let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());
//! # mig.commit();
//! # vote
//! # };
//! let mutv = g.get_mutator(vote);
//! mutv.put(vec![1000.into(), 1.into()]);
//! ```
//!
//! We will skip the parts related to the `Vote` base node, since they are equivalent to the
//! `Article` flow. The output of the `Vote` node arrives at `vc`, an aggregation. This ends up
//! calling `GroupedOperator::on_input` in `ops/grouped/mod.rs`. If you squint at it, you can see
//! that it first queries its own materialized output for the current value for the `GROUP BY` key
//! of the incoming record, and then uses `GroupedOperation` to compute the new, updated value. In
//! our case, this ends up calling `Aggregator::to_diff` and `Aggregator::apply` in
//! `ops/grouped/aggregate.rs`. As expected, these functions jointly just adds one to the current
//! count. The grouped operator then, as expected, emits a record with the new count. However, it
//! also does something slightly weird --- it first emits a *negative* record. Why..?
//!
//! Negative records are Soup's way of signaling that already materialized state has changed. They
//! indicate to descendant views that a past record is no longer valid, and should be discarded. In
//! the case of our vote, we would get the output:
//!
//! ```text
//! - [id=1, votes=0]
//! + [id=1, votes=1]
//! ```
//!
//! Since these are sent within a single `Update`, the descendants know the vote count was
//! incremented (and not, say, removed, and then re-appearing with a value of one). The negative
//! records are also observed by `domain::single::process`, which will delete the old materialized
//! result row (because of the `-`), and then insert the new materialized result row (because of
//! the `+`). Finally, the domain will call down the graph to `awvc` with this `Update` containing
//! both the negative and the positive (or rather, since it crosses a domain boundary, send it on a
//! channel).
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
//! In the absence of any further information, the materialization in `single::process` would then
//! insert a *second* row (well, a 43rd row) in the materialized table for our article. This would
//! mean that if someone queried for it, they would get a lot of results. In this particular
//! example, the negative from `vc` contains enough information to delete the correct output row
//! (`id=1`), but this is not always the case. We therefore have to perform the *full* join for the
//! negative as well, to produce an exact replica of the row that we are "revoking". In fact, since
//! this is a join, a single negative may produce *multiple* negative outputs, each one revoking a
//! single output record.
//!
//! So, from the `Update` received from `vc`, `awvc` will perform two joins, eventually producing a
//! new update with the records
//!
//! ```text
//! - [id=1, title=Hello world, votes=0]
//! + [id=1, title=Hello world, votes=1]
//! ```
//!
//! The materialized state will be updated by `single::process`, and `Domain::boot` will stop
//! propagating the `Update` since there are no descendant views.
//!
#![feature(optin_builtin_traits)]
#![feature(mpsc_select)]
#![feature(pub_restricted)]
#![feature(conservative_impl_trait)]
#![feature(try_from)]
#![deny(missing_docs)]
#![cfg_attr(feature="b_netsoup", feature(plugin))]
#![cfg_attr(feature="b_netsoup", plugin(tarpc_plugins))]

#[cfg(feature="b_netsoup")]
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate slog;
extern crate slog_term;

extern crate fnv;
extern crate evmap;
extern crate arccstr;

extern crate itertools;
extern crate petgraph;
extern crate regex;
extern crate nom_sql;
extern crate timekeeper;

#[cfg(feature="web")]
extern crate rustc_serialize;

#[macro_use]
#[cfg(feature="web")]
extern crate rustful;

#[macro_use]
#[cfg(feature="b_netsoup")]
extern crate tarpc;
#[cfg(feature="b_netsoup")]
extern crate futures;
#[cfg(feature="b_netsoup")]
extern crate tokio_core;

mod checktable;
mod flow;
mod ops;
mod backlog;
mod recipe;

pub use checktable::{Token, TransactionResult};
pub use flow::{Blender, Migration, Mutator};
pub use flow::core::{NodeAddress, DataType, Datas};
pub use flow::node::StreamUpdate;
pub use flow::sql_to_flow::{SqlIncorporator, ToFlowParts};
pub use flow::domain::Index;
pub use ops::base::Base;
pub use ops::grouped::aggregate::{Aggregator, Aggregation};
pub use ops::grouped::concat::{GroupConcat, TextComponent};
pub use ops::grouped::extremum::{Extremum, ExtremumOperator};
pub use ops::identity::Identity;
pub use ops::permute::Permute;
pub use ops::join::Builder as JoinBuilder;
pub use ops::union::Union;
pub use ops::latest::Latest;
pub use ops::filter::{Operator, Filter};
pub use ops::topk::TopK;
pub use recipe::Recipe;

#[cfg(feature="web")]
/// web provides a simple REST HTTP server for reading from and writing to the data flow graph.
pub mod web;

#[cfg(feature="b_netsoup")]
/// srv provides a networked RPC server for accessing the data flow graph.
pub mod srv;
