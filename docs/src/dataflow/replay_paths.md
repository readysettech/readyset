# Replay Paths
<sub>Updated 2022-03-21 by Griffin</sub>

For every [reader node][], and every non-[base][] node in the dataflow graph
that is *materialized*, the dataflow execution needs to be able to build the
state of that node by querying upstream materializations (including base nodes).
For fully materialized nodes, this happens immediately after those nodes are
constructed, and for partially materialized nodes this happens on-demand as
users query readers for new keys. To accomplish this, for each [index][] into a
materialization, we need to know the *provenance* of the columns in that index,
so that we know *which* upstream materialization we can query in order to
construct that state. We calculate this provenance at **migration** time (when
adding a new query), and save the information for this provenance, including the
nodes to upquery and the full path of nodes that the replay will pass through in
response, in a data structure called a **Replay Path**.

[reader node]: http://docs/rustdoc/noria_dataflow/node/special/struct.Reader.html
[base]: http://docs/rustdoc/noria_dataflow/node/special/struct.Base.html
[index]: http://docs/rustdoc/noria_dataflow/prelude/struct.Index.html

## Provenance Tracing

Provenance tracing, which is also described in §3.2.1 of the [thesis][],
involves tracing the set of key columns for an index (either in a reader or in
an internal node) "up" the graph, towards a *single* materialization which can
resolve all of those columns. Note the word *single* - since we have to query
the source materialization for *all* rows matching a lookup key for an index,
all columns must resolve to one and only one target materialization at the
source. If this doesn't happen, we need to either remap the column to split the
replay path, or force a full materialization at the source - both are described
in more detail below.

The actual implementation of provenance tracing is currently located entirely in
[`noria_server::controller::keys`][]. See that code for more detail about how it
works, but at a high level what happens is we start at the target of a replay
path (a materialization we want to be able to replay to), and trace recursively
in reverse topographical order ("up" the graph), each step of the way calling
the [`column_source`][] method on the node, then proceeding forward based on the
return value of that method.

[thesis]: https://jon.thesquareplanet.com/papers/phd-thesis.pdf
[`noria_server::controller::keys`]: http://docs/rustdoc/noria_server/controller/keys/index.html
[`column_source`]: http://docs/rustdoc/noria_dataflow/processing/trait.Ingredient.html#tymethod.column_source

### Direct provenance

In the simple case, all the columns for a particular index can be traced all the
way up the graph directly to one upstream materialization. Consider, for
example, the following query:

```sql
SELECT id, name FROM users WHERE deleted_at IS NULL AND id = ?
```

Which could create the following graph:

![Simple direct provenance](/images/simple-direct-provenance.png)

(see [Interpreting graphviz](/debugging.md#interpreting-graphviz) for more
information on how to read the above graph)

In that query, the `HashMap([0])` index in the reader node, with columns `[0]`,
can be traced verbatim all the way up the graph to the base node unchanged. In
interest of being extra-explicit for our first example, let's go through why
this is the case step-by-step, noting that tracing happens in reverse
topographical order ("up" the graph):

1. Ingress and egress nodes are both classes of "special"
   dataflow-execution-specific nodes that pass-through all rows unchanged, so by
   construction any columns resolve through nodes 5 and 6 completely unchanged
2. The project node (node 3) projects column 0 in its input to column 0 in its
   output, so we can trace the key `[0]` to `[0]`. This corresponds to a return
   value of [`ColumnSource::ExactCopy`][] from the [`column_source`][] method
3. Filter nodes (node 2) similarly pass-through all rows that aren't filtered
   out unchanged, so we can also resolve the key `[0]` to `[0]` in this node.
4. We've reached a materialization (the base table, node 1, in this case), so we
   can stop tracing and create our replay path starting at node 1, then going
   through nodes 2, 3, 6, and 5, and finally terminating at the reader, node 4.

Direct provenance can also work in more complex scenarios, such as queries
involving a compound index on more than one column, queries with joins, or
queries where the columns are reordered - as long as all the columns resolve to
one and only one parent, we still have direct provenance and can create a replay
path through the join to that parent. For example, consider the following query:

```sql
SELECT posts.id, posts.title, users.name
FROM users
JOIN posts ON users.id = posts.author_id
WHERE users.deleted_at IS NULL
AND posts.id = ?
AND posts.title = ?
```

Which could create the following graph:

![Complex direct provenance](/images/complex-direct-provenance.png)

In that query, we can still use direct provenance, since the lookup key on
columns `[1, 0]` in the reader traces directly to one and only one node (in this
case columns `[1, 0]` in node 1, the base node for the "posts" table).

[`ColumnSource::ExactCopy`]: http://docs/rustdoc/noria_dataflow/prelude/enum.ColumnSource.html#variant.ExactCopy

### Generated columns

We can't always track every column in an index all the way to an existing
materialization, however. One of the two scenarios this can happen in is if a
node "generates" columns which can't be directly traced back to a column in
their input. Consider this query (which could be more succinctly written using
`HAVING`, which ReadySet doesn't support yet):

```sql
CREATE VIEW post_count AS
SELECT count(*) as num_posts, author_id FROM posts GROUP BY author_id;

SELECT author_id, num_posts
FROM post_count
WHERE num_posts = ?
```

Which could create this graph:

![having-like, with generated columns](/images/having-like.png)

In that query, the key for the reader for the second query (node 8) traces to
the *result* column for the `COUNT` aggregate in the view (column 1 in node 2),
but then we get stuck - there isn't a parent of that aggregate node that
contains the result of the aggregate unchanged! This corresponds to a return
value of [`ColumnSource::RequiresFullReplay`][] from the [`column_source`][]
method. Currently, this situation *forces* node 2 to be fully materialized, as
that's the only way we currently know to query a node for all rows matching a
column which that node generates.

[`ColumnSource::RequiresFullReplay`]: http://docs/rustdoc/noria_dataflow/prelude/enum.ColumnSource.html#variant.RequiresFullReplay

#### Generated columns, but remapped

We'd like to avoid making fully materialized queries as often as we can,
however, since the whole raison d'être of Noria's invention is the ability to
use partial materialization to only store the individual results of a query in
memory that users have actually asked for. As an optimization, it's possible for
a node to request that an upquery to it for a particular set of columns be
remapped into an upquery for a *different* set of columns. For example, consider
the `Paginate` operator, which groups rows by a set of columns, orders each of
those groups by a different set of columns, then emits a "page number" column
indicating the page number within each group up to a configured page size. For
example, if we have a Paginate operator that groups by columns `[0]`, orders by
columns `[1]` ascending, and has a configured page size of 3, an input of:

```
["a", 1]
["a", 3]
["a", 2]
["a", 4]
["b", 2]
["b", 3]
["b", 5]
```

Would produce the following output (the page number column is always last):

```
["a", 1, 0]
["a", 3, 0]
["a", 2, 0]
["a", 4, 1]
["b", 2, 0]
["b", 3, 0]
["b", 5, 0]
```

Similarly to the aggregate node example above, column index 2 (the page number
column) in the output of the paginate node is "generated" by that node - there's
no direct way of querying any of the parents of the node to determine all of the
rows that would go into page 0, for example. However, we can get a little bit
closer - if instead we consider an index on both the group *and* the page (which
is likely to be the actual lookup key for a real-world query), we can
*approximate* an upquery for that index by making an upquery for *only* the
group - we'll load (potentially many) more rows than we need, but at least we'll
be better than fully materialized! Concretely, today, if you call
[`column_source`][] on a `Paginate` node with a set of columns containing both
the group columns and the page number, the return value will contain
[`ColumnSource::GeneratedFromColumns`][] with a column reference to *only* the
group, on the *node itself*. This semantically indicates that the page number
column is "generated" from the group column. In other words, the node is saying
"if you want only one page of a group, you can get that by replaying *all* rows
in the group through me, and then I'll be able to satisfy a lookup of only one
page".

[`ColumnSource::GeneratedFromColumns`]: http://docs/rustdoc/noria_dataflow/prelude/enum.ColumnSource.html#variant.GeneratedFromColumns

### Straddled joins

Coming Soon!
