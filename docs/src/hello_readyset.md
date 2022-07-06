# Hello, ReadySet!

This exercise will walk you through making your first code change, running tests against it, and then putting it up for code review.

We'll be adding a print statement on connection to the ReadySet adapter! The ReadySet adapter supports connections from MySQL and Postgres clients, and
converts queries issued on those connections to ReadySet queries that can be sent to the ReadySet servers.

## Making and testing code changes
1. **Making our code change**

    Modify `//noria-client/adapter/src/lib.rs` to print out `Hello, ReadySet!` when a new client connects
    as in the example below:

    ```rust
    //noria-client/adapter/src/lib.rs
    pub fn run(&mut self, options: Options) -> anyhow::Result<()> {
      // ... lots of code
      while let Some(Ok(s)) = rt.block_on(listener.next()) {
        println!("Hello, ReadySet!");

        // Creates a connection to Noria and the upstream database.
      }
    }
    ```

2. **Verify the unittests succeed**

    While this certainly didn't break anything, let's verify that `noria-client-adapter` still passes unittests.
    ```
    cargo test -p noria-client-adapter
    ```

3. **Build ReadySet**

    ```
    cargo build --bin noria-server --bin noria-mysql
    ```

    > The `--release` flag may be used with many `cargo` operations to compile with performance optimizations.


    Now that we have a version of ReadySet compiled with our new fancy print statement, let's see it in action.

4. **Start Consul and MySQL via the docker-compose**

   Use `docker-compose` to copy over the docker-compose overrides and spin up the external resources.
   ```
   cp docker-compose.override.yml.example docker-compose.override.yml
   docker-compose up -d
   ```

5. **Start the ReadySet server**

    We'll need to run both ReadySet server and ReadySet adapter to manually test our change. This can be done using two terminals,
    or a terminal multiplexer such as `tmux`.

    ```
    cargo run --bin noria-server -- --replication-url mysql://root:noria@127.0.0.1:3306/test --deployment my-first-deployment
    ```

    > **Command line arguments:**
    >
    > * `--replication-url`: The database that we are replicating updates from.
    > * `--deployment`: The ReadySet deployment key.
    >
    > See `--help` for the complete set of arguments and their documentation.


6. **Start the ReadySet adapter**

    ```
    cargo run --bin noria-mysql -- --upstream-db-url mysql://root:noria@127.0.0.1:3306/test --allow-unauthenticated-connections --address 127.0.0.1:3307 --deployment my-first-deployment
    ```

    > **Command line arguments:**
    >
    > * `--upstrteam-db-url`: The database to send writes and queries that cannot be run on noria-server to.
    > * `--allow-unauthenticated-connections`: When connecting via the mysql client, a username and password are not used.
    > * `--address`: The address and port to use for the MySQL server.
    > * `--deployment`: The ReadySet deployment key.
    >
    > See `--help` for the complete set of arguments and their documentation.

7. **Connect via a MySQL client**

    ```
    mysql -h 127.0.0.1 --port 3307
    ```
    > You can use this connection just like any other MySQL connection to:
    >  * Create a table, `t1` with two integer columns `c1` and `c2`: `CREATE TABLE t1 (c1 int, c2 int);`.
    >  * Insert data into the table, `t1`: `INSERT INTO t1 VALUES (4,5), (5,6), (6,7);`.
    >  * Retrieve the data from the table, `t1`: `SELECT * FROM t1 WHERE c1 = 5;`.


🎉 If all went well you should now see `Hello, ReadySet` printed in the ReadySet adapter (noria-mysql) logs.

## Moving from Git to code review to submission.

> We have several commit message lints to keep in mind, see [//scripts/commit_lint.sh](https://gerrit.readyset.name/plugins/gitiles/readyset/+/refs/heads/main/scripts/commit_lint.sh):
>  * The subject must not be longer than 80 characters.
>  * The second line should be empty.
>  * The commit message must contain a body, that must be longer than 80 characters and wrapped at 80 characters.
>    Use this to describe the purpose of the change.
>
> Additionally, we follow certain other conventions that are not currently enforced via linter.
> These conventions are semi-arbitrary, but are chosen to match common standards in the wider
> development community, and make commit messages easier to read by making them more uniform:
>  * The subject should be capitalized, along with sentences in the body.
>  * The subject should not end in a period, but sentences in the body should.
>  * The subject should use the imperative mood (i.e. "Add code to do X", not "Adding code to do
>    X" or "Added code to do X"). This restriction need not apply to the body. Note this is the
>    standard used by Git itself for builtin commit messages like "Merge X to Y" or "Revert Z",
>    so while it may seem arbitrary, there's a reason we chose this style beyond just personal
>    preference 🙂
>  * (Optional) Prior to the capitalized subject, we commonly include the component to which the
>    change applies. The component is typically the lowercase directory name of the code that
>    we're modifying, or occasionally might refer to a more general area of functionality. Some
>    hypothetical examples:
>    ```
>    docs: Add documentation on commit message conventions
>    Logging: Refactor log formatting code across several components
>    ```

1. **Creating a commit on a local branch**

    Let's create a Git branch for this change and commit our change:
    ```bash
    git checkout -b hello-readyset
    git add noria-client/adapter/src/lib.rs
    git commit
    ```

2. **Going through code review**

    Once you have a commit to put up for review, you can push the change to Gerrit. A commit that has been pushed
    to Gerrit is called a **changelist** or **CL**. At ReadySet, this expression is more commonly used instead of commit.
    ```
    git push origin HEAD:refs/for/main
    ```

    🎉 You should be able to see your commit in the [Gerrit code review UI](https://gerrit.readyset.name/dashboard/self).

    > By default every commit uploaded to Gerrit is set as *Active* and will appear in Slack's `#prs` channel. (request
    > access if you don't have it)
    > These changes can instead by set as *Work-In-Progress* on upload through
    > [`Gerrit > Settings > Preferences`](https://gerrit.readyset.name/settings/#Preferences).

3. **Going through CI**

   Anytime a CL or a change to a CL is uploaded, we automatically run it through our testing pipeline, often referred to
   as **CI** (continuous integration) or our **CI pipeline**.

   You'll see **Buildkite CI** added as a reviewer to your change and the following comment on your change:
   ```
   "Build of patchset 1 running at https://buildkite.com/readyset/readyset/builds/<build number>"
   ```

   The buildkite link can be used to see what tests succeeded and failed. Every CI must successfully run the tests
   in the testing pipeline before being merged.

4. **Code Review**

   Before you can submit your change to the codebase, the code has to reviewed and approved by at
   least one other engineer. The list of engineers expected to take action on a CL is called the
   "attention set"; initially the attention set consists of the assigned reviewers, but if a
   reviewer responds or the CI fails, the attention set may switch back to the author.
   A reviewer can be added to your CL via the `Change Metadata` in the top left of the Gerrit CL page or the
   `Reply/Start Review` button in the center.

   > See [Gerrit Review UI](https://gerrit.readyset.name/Documentation/user-review-ui.html)
   > for a more in depth overview of the review UI.
   >
   > It can be helpful to look at the [list of changes up for review](https://gerrit.readyset.name/q/status:open+-is:wip)
   > for your intended reviewer to make sure they have a reasonable review load before adding more!

   > Instead of simply approving or rejecting a CL, reviewers must rank a CL on a scale from `-2` to
   > `+2`. At ReadySet, we require CL's to receive at least one `+2` before it should be
   > accepted. `+1` reviews are also fine, but generally indicate that more review is needed.

5. **Making Updates / Responding to Comments**

   Updating a CL is accomplished by making changes to your commit locally and pushing to Gerrit.
   ```
   # ... changes to files
   git add .
   git commit --amend
   git push origin HEAD:refs/for/main
   ```

   This will create a new **Patchset** for your CL, a patchset is an iteration of a commit that is up for
   review. Pushing a new patchset to Gerrit will trigger the CI pipeline.

   > We have configured Gerrit to require that all review comments are marked resolved before a CL
   > can be accepted. Note that "resolved" specifically means checking the "resolved" checkbox on
   > a comment; this distinction is import to be mindful of, as there are some common gotchas that
   > can result:
   > * It is possible for a commenter to mark their own comment as resolved if it is
   >   merely an observation which requires no changes. If they do not do so, then merging may be
   >   unintentionally blocked.
   > * Top-level comments (i.e. comments that are made on the CL itself rather than on any
   >   particular line of code) are marked "resolved" by default, and must be explicitly changed to
   >   "unresolved" by the commenter if they are requesting changes.
   >
   > There are two buttons that can be used to respond to a comment and mark it resolved with a
   > single click: "Done" and "Ack".
   > * "Done" means "I have implemented this feedback".
   > * "Ack" means "I understand your comment but have not made any corresponding change".
   >
   > "Ack" is less frequently used, but can be useful when responding to comments that start with
   > "nit"; "nit" is short for "nitpick", and is used when giving feedback of relatively low
   > importance, which may not be strictly necessary to address (such as subjective stylistic
   > suggestions, minor grammatical suggestions in a comment, etc.).

   For further helpful gerrit guidance, please see [Gerrit
   Documentation](./gerrit.md)

6. **Submitting your CL**

   Once all code review comments have been responded to and your code has been approved by another engineer,
   assigned a `+2`, and passed Buildkite. It can be submitted.

   We won't do this for the Hello, ReadySet! example as we do not need to greet ourselves on every client connection
   in production. But for any other change:

   🎉 Hit that **Submit** button in the top right of the Gerrit Review UI!
