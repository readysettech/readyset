# Getting Started
<sub>Updated 2022-05-26</sub>

Welcome to ReadySet! This guide is meant to give you the tools needed to work in our code base.
<!-- toc -->
## Getting access to internal resources via Tailscale
We use [Tailscale](https://tailscale.com/) as a [virtual private network](https://en.wikipedia.org/wiki/Virtual_private_network) (VPN)
for accessing internal resources such as code review and EC2 instances.

> **NOTE:**  Tailscale is not currently configured to route public traffic and will only route traffic to internal resources.

1. **Installing Tailscale**

    See [Installing Tailscale](https://tailscale.com/download) for instructions for your operating system.

2. **Running Tailscale**

    **macOS**
    Run the Tailscale GUI app, and use the menu bar icon in the upper right to log in and connect.

    **Linux**
    Run Tailscale with root privileges and authenticate with your @readyset.io email address
    ```bash
    sudo tailscale up --accept-dns
    ```

## Code management and review via Gerrit
Gerrit is a code review and project management tool for Git projects. Once you have access to Tailscale,
you can access the Gerrit UI via [https://gerrit.readyset.name/](https://gerrit.readyset.name/), using your
@readyset.io email address to sign in.

> If you're coming from other Git platforms such as GitHub, there are some things about Gerrit that
> may seem strange or counterintuitive, as the workflow is a bit different from other Git tools.
> We'll discuss some norms and conventions related to ReadySet's use of Gerrit later in this
> document, but for a good general introduction to Gerrit, check out either of these guides from the
> official documentation:
> * [Basic Gerrit Walkthrough](https://gerrit.readyset.name/Documentation/intro-gerrit-walkthrough.html)
> * [Basic Gerrit Walkthrough - For GitHub Users](https://gerrit.readyset.name/Documentation/intro-gerrit-walkthrough-github.html)

### Checking out the code
All ReadySet code lives in the [ReadySet monorepo](https://gerrit.readyset.name/admin/repos/readyset), a single repository that contains all our projects.

1. **Configure a username via the Gerrit UI [`Settings > Profile`](https://gerrit.readyset.name/settings/#Profile)**

    > ❗ This is extremely hard to change after the fact, so make your username choice count.

2. **Add an SSH key to Gerrit via the Gerrit UI [`Settings > SSH Keys`](https://gerrit.readyset.name/settings/#SSHKeys)**

    > If you do not have an ssh key, you may generate one with
    > ```
    > ssh-keygen -t ed25519 -C "your_email@readyset.io"
    > ```
    >
    > If you already have an rsa key, we prefer you generate a new ed25519 key given
    > rsa's impending deprecation.

3. **Clone the ReadySet repo and Gerrit commit hook with `git`.**
    ```
    git clone "ssh://<username>@gerrit:29418/readyset" && scp -p -P 29418 <username>@gerrit:hooks/commit-msg "readyset/.git/hooks/"
    ```

    > Gerrit requires a commit hook that adds a globally unique `Change-Id` to each commit message.
    > The `Change-Id` is used by Gerrit to track commits across cherry-picks and rebases.

### Getting rust and dependencies

1. **Install ReadySet dependencies.**

   **macOS with [homebrew](https://brew.sh/)**
   ```bash
   brew install lz4
   brew install openssl@1.1
   ```

   **Ubuntu**

   ```bash
   sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config llvm clang liblz4-dev
   sudo apt-get -y install cmake
   ```

   **Arch**

   ```bash
   sudo pacman -S base-devel clang lz4
   ```

   **Nix**
   ```bash
   nix-shell
   ```

2. **Install rust via [rustup.rs](https://rustup.rs/)**.

     > It can be helpful to install a couple other rust tools and make several changes to configure rust.
     >
     > **mdbook:** Our general docs exist as an mdbook, a book of markdown documents.
     > ```
     > cargo install mdbook
     > ```
     >
     > **rust-language-server:** Many IDEs and editors use the rust language server to support functionality
     > like goto definition, symbol search, reformatting, code completion derived from the compiler.
     > ```
     > rustup component add rls
     > ```
     >
     > **rust-analyzer:** A rust-language-server alternative that uses semantic analysis. See [rust-analyzer](https://rust-analyzer.github.io/).
     >
     > **mold linker:** Mold is a significantly faster drop-in replacement for UNIX linkers for recent linux distributions.
     >                  See [mold](https://github.com/rui314/mold).

     Once rust is installed you should be able to use the rust build system and package manager, `cargo`, to build and
     test ReadySet. See [cargo book](https://doc.rust-lang.org/cargo/) for an in-depth resource on using `cargo`. Below is a
     small subset of commands that are typically used. Each command supports many more options than listed.

     ```bash
     # Building a binary or package
     cargo build [--bin <binary> | -p <package]

     # Lint and check for common rust mistakes
     cargo clippy [--bin <binary> | -p <package>]

     # Running a binary
     cargo run --bin <binary> -- <args>

     # Running tests for a package
     cargo test -p <package> <filter>
     ```

     > ❗Running tests may require increasing file descriptor limits via running `ulimit -Sn 65535`. 

3. **Install docker via [Get Docker](https://docs.docker.com/get-docker/) and docker-compose via [Install Docker Compose](https://docs.docker.com/compose/install/).**

   ReadySet has several external system components, Consul: leader election and failure detection, MySQL or Postgres: the database we are
   performing caching for. We create these resources for local dev through `docker-compose`.

   Once you have installed `docker` and `docker-compose`, you may use the following commands to create the external resources for local dev.

   ```
   # Go to the root of the monorepo.
   cd readyset

   # Set the docker overrides file to open up docker ports locally.
   cp docker-compose.override.yml.example docker-compose.override.yml

   # Create the docker resources: consul, postgres, zookeeper, and mysql.
   docker-compose up -d

   # Terminate the docker resources and remove the images.
   docker-compose down
   ```

   > **NOTE**: On Linux you may want to manage docker as the non-root user, see [Post-installation steps for Linux](https://docs.docker.com/engine/install/linux-postinstall/).

## Hello, ReadySet!
You now have all the tools to make code changes! We'll walk you through making your first code change, running tests against it, and then putting it up for code review.

We'll be adding a print statement on connection to the ReadySet adapter! The ReadySet adapter supports connections from MySQL and Postgres clients, and
converts queries issued on those connections to ReadySet queries that can be sent to the ReadySet servers.

#### Making and testing code changes
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


2. **Verify that unittests succeed**

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
    >  * Create a table, `t1` with two integer columns `c1` and `c2`: `CREATE TABLE t1 (c1 int, c2 int)`.
    >  * Insert data into the table, `t1`: `INSERT INTO t1 VALUES (4,5), (5,6), (6,7)`.
    >  * Retrieve the data from the table, `t1`: `SELECT * FROM t1 WHERE c1 = 5;`.


🎉 If all went well you should now see `Hello, ReadySet` printed in the ReadySet adapter (noria-mysql) logs.

#### From Git to code review to submission.

1. **Creating a commit on a local branch**

    Let's create a Git branch for this change and commit our change:
    ```bash
    git checkout -b hello-readyset
    git add noria-client/adapter/src/lib.rs
    git commit
    ```

    > We have several commit message lints to keep in mind, see `//scripts/commit_lint.sh`:
    >  * The subject should not be longer than 80 characters.
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

2. **Going through code review**

    Once you have a commit to put up for review, you can push the change to Gerrit. A commit that has been pushed
    to Gerrit is called a **changelist** or **CL**, this expression is commonly used instead of commit.
    ```
    git push origin HEAD:refs/for/main
    ```

    🎉 You should be able to see your commit in the [Gerrit code review UI](https://gerrit.readyset.name/dashboard/self).

    > By default every commit uploaded to Gerrit is set as *Active* and will appear in Slack's `#prs` channel.
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

6. **Submitting your CL**

   Once all code review comments have been responded to and your code has been approved by another engineer,
   assigned a `+2`, and passed Buildkite. It can be submitted.

   We won't do this for the Hello, ReadySet! example as we do not need to greet ourselves on every client connection
   in production. But for any other change:

   🎉 Hit that **Submit** button in the top right of the Gerrit Review UI!
