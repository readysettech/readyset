# Getting Started
<sub>Updated 2022-06-07</sub>

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
> For a general introduction to Gerrit, check out either of these guides from the
> official Gerrit documentation:
> * [Basic Gerrit Walkthrough](https://gerrit.readyset.name/Documentation/intro-gerrit-walkthrough.html)
> * [Basic Gerrit Walkthrough - For GitHub Users](https://gerrit.readyset.name/Documentation/intro-gerrit-walkthrough-github.html)
>
> At Readyset we maintain some of our own [social conventions](./gerrit.md) which will help you in adapting to our workflow.

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
    > ❗ If you're using openssh >= 8.7.0, you will need to add `-O # O as in Oscar` to the `scp`
    > command below, due to openssh using the SFTP protocol for `scp` starting in version 8.7.0.
    > See [the first bullet](https://www.openssh.com/txt/release-8.7) under "New Features" or
    > [this gerrit bug](https://bugs.chromium.org/p/gerrit/issues/detail?id=15944) for more
    > information.

    ```
    echo -n 'Username: ' && read RS_USER && \
    git clone "ssh://${RS_USER}@gerrit:29418/readyset" && \
    scp -p -P 29418 "${RS_USER}"@gerrit:hooks/commit-msg "readyset/.git/hooks/"
    ```

    > Gerrit requires a commit hook that adds a globally unique `Change-Id` to each commit message.
    > The `Change-Id` is used by Gerrit to track commits across cherry-picks and rebases.

### Getting rust and dependencies

1. **Install ReadySet dependencies.**

   **macOS with [homebrew](https://brew.sh/)**

   ```bash
   brew install lz4 openssl@1.1 rocksdb
   ```

   Add the following to your [cargo config](https://doc.rust-lang.org/cargo/reference/config.html)
   to make it discover `lz4`:

    ```toml
    [target.aarch64-apple-darwin.env]
    LIBRARY_PATH = "/opt/homebrew/lib"

    [target.x86_64-apple-darwin.env]
    LIBRARY_PATH = "/opt/homebrew/lib"
    ```

   **Ubuntu**

   ```bash
   sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config llvm clang liblz4-dev librocksdb-dev
   sudo apt-get -y install cmake
   ```

   **Arch**

   ```bash
   sudo pacman -S base-devel clang lz4 rocksdb-static
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

4. **[Optional] Configure linking to system library dependencies for faster builds**

  ReadySet has a few system dependencies that have a long build time. Instead
  of re-compiling them each time `cargo build` is invoked, we can instead make
  sure the system libraries are installed and set some environmental variables
  so that cargo can link with the system libraries directly.

  - **librocksdb-sys**

    librocksdb-sys also uses lz4 and has a separate flag that will pick it up
    when running its build.rs script.

    > **NOTE** adjust the lib directory based on your system--the path below is
    > the default for MacOS/homebrew

    **Fish**
    ```
    set -Ux ROCKSDB_LIB_DIR "/opt/homebrew/lib/"
    set -Ux LZ4_LIB_DIR "/opt/homebrew/lib/"
    ```

    **Bash**
    ```
    export ROCKSDB_LIB_DIR="/opt/homebrew/lib/"
    export LZ4_LIB_DIR="/opt/homebrew/lib/"
    ```
  - **openssl-sys**

    openssl-sys will automatically pick up the system dependency _unless_ the
    "vendored" feature is set. We set this feature in our installer crate (for
    good reason), but if we know the system library will work for us and are
    re-compiling the installer crate ourselves, we can remove that feature flag
    locally to speed up build times. This only applies if compiling the
    installer crate itself.

    Modify installer/Cargo.toml to remove the "vendored" feature flag. Update
    the appropriate target for your dev environment.

    ```toml
    [target.aarch64-apple-darwin.dependencies]
    openssl = { version = "*" }
    ```

With all of the tools you'll need installed, you're ready to tackle the [Hello ReadySet!](./hello_readyset.md) project! 🎉🎉🎉
