# Framework testing

#### Running the tests

To run the tests, there are seven environment variables you need to set:  `RS_HOST`, `RS_PORT`, `RS_USERNAME`, `RS_PASSWORD`, `RS_DATABASE`, `RS_NUM_SHARDS`, and `RS_DIALECT`.
* `RS_HOST` is an IP address or port to access the ReadySet/MySQL database you want to run the tests against
* `RS_PORT` is the port number on which the database server is listening; `3306` is probably what you'll want to set this to
* `RS_USERNAME` is any username with access to the database you want to run tests against
* `RS_PASSWORD` is the password for that user
* `RS_DATABASE` is the name of the database/keyspace you want to access
* `RS_NUM_SHARDS` is the number of shards that are being used for the test.
    Setting this to 1 means that we are running an unsharded test (or
    pedantically, a singly sharded test, since a shard is just another name for
    a database).
* `RS_DIALECT` is the MySQL version we're targeting - currently supported options are `mysql57` and `mysql80`.

##### Example

Using a simple MySQL container as an example:
```bash
docker run --name=mysql -d -e MYSQL_DATABASE=test -e MYSQL_ROOT_PASSWORD=testpassword mysql:8.0
# ... wait for container to start ...
# Note that simply doing a port forward with -p on the `docker run` line doesn't
#    allow us to use `localhost` here, because the framework runs the tests in
#    their own containers.  For that reason, we get the IP address for the MySQL
#    container and use that.
export RS_HOST="$(docker inspect mysql | jq -r '.[].NetworkSettings.IPAddress')"
export RS_PORT=3306
export RS_USERNAME=root
export RS_PASSWORD=testpassword
export RS_DATABASE=test
export RS_NUM_SHARDS=1
export RS_DIALECT=mysql80
./run.sh run_test rust/mysql_async # Runs a single test, the Rust mysql_async client
./alltests.sh # Runs the entire test suite
```

#### Testing local changes

To test local changes to a test, there are two steps - build the test's container image, and run it.
* To build, simply run `./run.sh build_image "$dialect" "$language/$framework"`, e.g. `./run.sh build_image mysql rust/mysql_async`
* To run, follow the instructions under "Running the tests", above.

##### Example

Using noria as an example:
```bash
docker run --name zookeeper --restart always -p 2181:2181 -d zookeeper
cargo r --release --bin noria-server -- --deployment myapp --no-reuse --address 127.0.0.1 --shards 0
cargo run --release --bin noria-mysql -- --deployment myapp --no-require-authentication  -a 127.0.0.1:3333
export RS_HOST="127.0.0.1"
export RS_PORT=3333
export RS_USERNAME=
export RS_PASSWORD=password
export RS_DATABASE=test
export RS_NUM_SHARDS=0
export RS_DIALECT=mysql80
./run.sh run_test ruby/rails6
./alltests.sh
```

#### Introducing a new framework

New frameworks, languages, or tools can get added for testing by introducing the following directory structure:

- __readyset\-framework\-testing__
   - frameworks/__language__
     - __framework__
       - src/main.go (source code)
       - Dockerfile (dockerfile)
       - build (entrypoint)

Provided there is a `Dockerfile`, tests will be run automatically. The guidelines for an individual test are:
* The test is run in a container.
* Most of the work should be done at container build time, whenever possible. For example:
  * Any run-time dependencies should all be installed at build time.
  * For compiled languages, the code should be compiled and the binary placed in the image, except when database interaction at compile time is a feature of the framework. Rust `sqlx` is an example of such case.
* The container's `ENTRYPOINT` is responsible for everything related to test setup, run, and teardown _EXCEPT_ running the database itself; the database will be provided by CI, and, for local test runs, is expected to be provided by the user. For example:
  * Preferably, the framework should be used for "simple" programs that just run to completion and exit. In this case, the programs would create any tables they needs, run through code that exercises their database features (e.g. runs queries), drop the tables, and then exit.
  * If the framework is strictly used to setup a server, such as a web service, the container entrypoint should start the service as a background process, then interact with it in ways that exercise the database features.
* The container's entrypoint should exit with a status code of 0 when the test succeeds, or nonzero if it fails in any way.
* In cases when the test container fails prematurely, the framework will take care of cleaning up tables after each test.

## Automated test workflows (high-level)
Because the logic around CI and periodic runs is...let's politely say _complicated_, we have it documented here.

Definitions:
* Framework - a database client or client library; examples:
  * rust/diesel
  * mysqldump/import
  * ruby/rails6
* Dialect - a SQL dialect; currently two are supported:
  * MySQL
    * 8.0
  * Postgres
    * 13

Workflows:
* Periodically (currently every 12 hours)
  * For each $framework in the test harness
    * For each $dialect with a test for $framework
      1. Build a test container image for $framework on $dialect
      1. Test $framework against $dialect's reference implementation
      1. If successful, test $framework against ReadySet's implementation of $dialect
* On every CL
  * For each $framework whose test was changed in _this CL_
    * For each $dialect with a test for $framework
      1. Build a test container image for $framework on $dialect
        * Hard failure - if building the image doesn't work, we can't consider the test working
      1. Test $framework against $dialect's reference implementation
        * Hard failure - if the test doesn't work against a vanilla database, we can't consider the test working
      1. If successful, test $framework against ReadySet's implementation of $dialect
        * Soft failure - we know ReadySet will currently fail in lots of cases; expanding coverage is fine, even if we know it doesn't work
