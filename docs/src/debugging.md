# Debugging

There are a few standard Rust debugging tools, in addition to some
ReadySet-specific internal debugging tools, that are valuable when tracking down
issues with Noria.

## ReadySet REPL

Because the readyset adapter has different code paths for prepared statements
and queries executed ad-hoc, it's often desirable to prepare and execute
statements when testing. The official `mysql` and `psql` clients, however, don't
directly support preparing and executing statements. There's a tool in the
workspace called `readyset-repl` that can be used for this purpose, in addition
to just as a regular database client for either PostgreSQL or MySQL. To run it,
run the following command from the root of the repository:

```
$ cargo run --bin readyset-repl -- <database-url>
```

Where `<database-url>` is a URL for a database, starting with either `mysql://`
or `postgresql://`. For example, the following is the URL to connect to the
mysql database currently run by the `docker-compose.yml` in the root of the
repository:

```
mysql://root:noria@127.1/test
```

Once you run the REPL binary, you'll be given a prompt to execute queries. The
REPL also supports a `help` command for providing built-in help.

To prepare a statement with the REPL, prefix the statement with `prepare `. That
will output the ID of the prepared statement, which can be prefixed with
`execute` along with a comma-separated list of parameters to execute the query.
For example:

```
Welcome to the ReadySet REPL!

Type `help` for help.

[mysql://root@127.1/test] ❯ prepare select * from t1 where uid = ?;
Prepared statement id: 0

[mysql://root@127.1/test] ❯ execute 0 1;

 1 | 4
```

## Graphviz

Noria has the ability to emit a [graphviz][] representation of both the dataflow
graph and the MIR graph, which can be passed to the graphviz `dot` program to
generate PNG images of the dataflow graph. Here's an example of what a graphviz
graph of the query in the [noria-server module documentation][awvc] looks like:

![Graphviz of `article_with_vote_counts` query](./images/awvc-graphviz.png)

These graphs are *hugely useful* - they're almost always the first step when
debugging any query issues. This section documents how to get noria-server to
output the requisite information, how to generate the images, and how to
interpret them.

[graphviz]: https://graphviz.org/
[awvc]: /rustdoc/noria_server/index.html#im-a-visual-learner

### Outputting graphviz

There are a couple of ways that we usually get Noria to output raw graphviz dot
source:

- Use the `graphviz` command in a [logictest][]

- Call the [`graphviz`][] method on a `ControllerHandle`, for example in a
  noria-server integration test in `//noria/server/src/integration.rs`, and
  print the resulting string to stderr, eg:

  ```rust
  eprintln!("{}", g.graphviz().await.unwrap())
  ```

- Call the [`graphviz`][graphviz-function] function directly inside of
  noria-server, and print the resulting string to stderr

- Run `EXPLAIN GRAPHVIZ` via an adapter, either manually in a mysql or
  postgresql shell or via a connection in an integration test, eg:

  ```rust
  # MySQL
  eprintln!(
      "{}",
      conn.query_first::<(String,), _>("EXPLAIN GRAPHVIZ")
          .unwrap()
          .unwrap()
          .0
  );

  # PostgreSQL
  eprintln!(
      "{}",
      conn.query_one("EXPLAIN GRAPHVIZ", &[])
          .unwrap()
          .get::<_, String>(0)
  );
  ```

- Make an HTTP GET request to the `/graph` RPC endpoint in `noria-server`:

  ```rust
  curl localhost:6033/graph
  ```

All of these methods emit output in the [graphviz `dot` format][dot], and look
like this (this is the source of the above graph):

```dot
digraph {{
    node [shape=record, fontsize=10]
    n0 [style="filled", fillcolor=white, label="(source)"]
    n1 [style="filled", fillcolor="/set312/1", label="{ { 1 / l0 / article | B | ● } | 0: id, \n1: title | unsharded }"]
    n2 [style="filled", fillcolor="/set312/2", label="{ { 2 / l0 / vote | B | ● } | 0: user_id, \n1: article_id | unsharded }"]
    n3 [style="filled", fillcolor="/set312/2", label="{{ 3 / l1 / q_a6f0d135deb2f983_n0 | \|*\| γ[1] | ● } | 0: article_id, \n1: votes | unsharded}"]
    n4 [style="filled", fillcolor="/set312/2", label="{{ 4 / l2 / q_a6f0d135deb2f983_n1 | π[0, 1, lit: 0] | ● } | 0: article_id, \n1: votes, \n2: bogokey | unsharded}"]
    n5 [style="filled", fillcolor="/set312/3", label="{ { 5 / l0 / vote_count | ● } | (reader / ⚷: HashMap([2])) | unsharded }"]
    n6 [style="filled", fillcolor="/set312/3", label="{ { 6 / l1  } | (ingress) | unsharded }"]
    n7 [style="filled", fillcolor="/set312/2", label="{ 7 / l3 | (egress) | unsharded }"]
    n8 [style="filled", fillcolor="/set312/2", label="{{ 8 / l4 / q_9a2271d3e3cc2573_n0 | [11:0, 11:1, 4:1, 4:2] 11:(0) ⋈ 4:(0)  } | 0: id, \n1: title, \n2: votes, \n3: bogokey | unsharded}"]
    n9 [style="filled", fillcolor="/set312/2", label="{{ 9 / l5 / q_9a2271d3e3cc2573_n1 | π[0, 1, 2]  } | 0: id, \n1: title, \n2: votes | unsharded}"]
    n10 [style="filled", fillcolor="/set312/4", label="{ { 10 / l0 / article_with_vote_count | ◕ } | (reader / ⚷: HashMap([0])) | unsharded }"]
    n11 [style="filled", fillcolor="/set312/2", label="{ { 11 / l6 | ◕ } | (ingress) | unsharded }"]
    n12 [style="filled", fillcolor="/set312/1", label="{ 12 / l1 | (egress) | unsharded }"]
    n13 [style="filled", fillcolor="/set312/4", label="{ { 13 / l1  } | (ingress) | unsharded }"]
    n14 [style="filled", fillcolor="/set312/2", label="{ 14 / l7 | (egress) | unsharded }"]
    n0 -> n1 [ style=invis ]
    n0 -> n2 [ style=invis ]
    n2 -> n3 [  ]
    n3 -> n4 [  ]
    n4 -> n7 [  ]
    n6 -> n5 [  ]
    n7 -> n6 [ color="#CCCCCC" ]
    n1 -> n12 [  ]
    n4 -> n8 [  ]
    n8 -> n9 [  ]
    n9 -> n14 [  ]
    n11 -> n8 [  ]
    n12 -> n11 [ color="#CCCCCC" ]
    n13 -> n10 [  ]
    n14 -> n13 [ color="#CCCCCC" ]
}}
```

[logictest]: /dev-testing.html#logictests
[`graphviz`]: /rustdoc/noria/controller/struct.ControllerHandle.html#method.graphviz
[graphviz-function]: /rustdoc/noria_server/controller/state/fn.graphviz.html
[dot]: https://graphviz.org/doc/info/lang.html

### Generating images

Once you've generated graphviz source for your graph, you can feed it to the
`dot` program on standard input to generate a PNG image. For example:

```shell-session
# On linux, if the graphviz is in your clipboard:
$ xclip -out -selection clipboard | dot -Tpng > graph.png

# On MacOS, if the graphviz is in your clipboard
$ pbpaste | dot -Tpng > graph.png
```

On Linux, it's possible to feed this directly into an image viewer like
[`feh`][] to open the image immediately:

```shell-session
$ xclip -out -selection clipboard | dot -Tpng | feh -
```

Some engineers create a script with the above command and put it on their
`PATH`, so that it can be easily run from a regular desktop program launcher

[`feh`]: https://feh.finalrewind.org/

### Interpreting graphviz

In a dataflow graphviz diagram:

- Each *box* is a [node][] in the dataflow graph
- Each *different color* is a different [domain][]
- Edges in the graph represent connections between nodes, with writes flowing
  downward in the direction of the arrow
- The nodes themselves have the following information:
  - First, the title:
    - The title of the node always starts with the node's [index][], then its
      [local index][] prefixed by the letter "l". For example, in the above
      graph, the top left node `1 / l0 / article` has node index `1` and local
      node index (the index within its domain) `0`.
    - For reader or non-[special][] nodes, the title then includes the [name][] of
      the node
    - For [internal][] nodes, there is then a [description][] of the node's
      [ingredient][]. This varies depending on the ingredient in question, but
      always at least contains a symbol taken from [relational algebra][]
      representing the operation. Some examples of descriptions:
      - `|*| γ[1]` is a `COUNT(*)` operation, grouping (mnemonic gamma = "group")
        by the second column (column index 1)
      - `π[0, 1, lit: 0]` is a project node (mnemonic pi = "project") emitting the
        first and second column from its parent, followed by the literal value 0
      - `[11:0, 11:1, 4:1, 4:2] 11:(0) ⋈ 4:(0)` is an inner join (the `⋈`) using
        column 0 from node 11 and column 0 from node 4 as its join key, and
        emitting columns 0 and 1 from node 11, followed by columns 1 and 2 from
        node 4
    - For [base table][base] nodes, there is the letter "B" in place of the
      ingredient description, which is short for "Base"
    - Finally in the title section is an optional representation of the
      *statefulness* of the node. Fully materialized nodes have a filled circle
      ("●"), whereas partially materialized nodes have a three-quarters-filled
      circle ("◕"). Note that base nodes are by definition always fully
      materialized
  - Following the title, for [internal][] and [base][] nodes, is a list of the
    column indices and names in that node.
  - In the place of columns, [ingress][] and [egress][] nodes contain just the
    strings "(ingress)" or "(egress)" respectively. Both these types of nodes
    only ever emit their input rows unchanged, so their columns are the same as
    their parent.
  - In the place of columns, [reader][] nodes have the word "reader", followed
    by the [index type][] and lookup key columns for that reader

[node]: /rustdoc/noria_dataflow/node/struct.Node.html
[domain]: /rustdoc/noria_dataflow/struct.Domain.html
[index]: /rustdoc/noria_dataflow/prelude/struct.NodeIndex.html
[local index]: /rustdoc/noria_dataflow/prelude/struct.LocalNodeIndex.html
[special]: /rustdoc/noria_dataflow/node/special/index.html'
[name]: /rustdoc/noria_dataflow/node/struct.Node.html#structfield.name
[internal]: /rustdoc/noria_dataflow/node/enum.NodeType.html#variant.Internal
[description]: /rustdoc/noria_dataflow/processing/trait.Ingredient.html#tymethod.description
[ingredient]: /rustdoc/noria_dataflow/processing/trait.Ingredient.html
[relational algebra]: https://en.wikipedia.org/wiki/Relational_algebra
[base]: /rustdoc/noria_dataflow/node/special/base/struct.Base.html
[ingress]: /rustdoc/noria_dataflow/node/special/struct.Ingress.html
[egress]: /rustdoc/noria_dataflow/node/special/struct.Egress.html
[reader]: /rustdoc/noria_dataflow/node/special/reader/struct.Reader.html
[index type]: /rustdoc/noria_dataflow/prelude/enum.IndexType.html

#### Sharding

Introducing sharding to Noria adds a number of additional pieces of notation to
the graphviz diagrams - since currently sharding is known-broken and
unsupported, we omit sharding here. If we later bring sharding back into scope,
this page should be expanded to document all of the parts of graphviz that
pertain to sharding.

## [rr][] and GDB

TODO

[rr]: https://rr-project.org/
