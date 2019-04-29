# msql-srv-rs

[![Crates.io](https://img.shields.io/crates/v/msql_srv.svg)](https://crates.io/crates/msql_srv)
[![Documentation](https://docs.rs/msql-srv/badge.svg)](https://docs.rs/msql-srv/)
[![Build Status](https://travis-ci.org/jonhoo/msql-srv.svg?branch=master)](https://travis-ci.org/jonhoo/msql-srv)
[![Codecov](https://codecov.io/github/jonhoo/msql-srv/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/msql-srv)

Bindings for emulating a MySQL/MariaDB server.

When developing new databases or caching layers, it can be immensely useful to test your system
using existing applications. However, this often requires significant work modifying
applications to use your database over the existing ones. This crate solves that problem by
acting as a MySQL server, and delegating operations such as querying and query execution to
user-defined logic.

To start, implement `MysqlShim` for your backend, and create a `MysqlIntermediary` over an
instance of your backend and a connection stream. The appropriate methods will be called on
your backend whenever a client issues a `QUERY`, `PREPARE`, or `EXECUTE` command, and you will
have a chance to respond appropriately. For example, to write a shim that always responds to
all commands with a "no results" reply:

```rust
extern crate mysql;
use msql_srv::*;

struct Backend;
impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }
    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(0, 0)
    }
    fn on_close(&mut self, _: u32) {}

    fn on_init(&mut self, _: &str, writer: InitWriter<W>) -> io::Result<()> { Ok(()) }

    fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let cols = [
            Column {
                table: "foo".to_string(),
                column: "a".to_string(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "foo".to_string(),
                column: "b".to_string(),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            },
        ];

        let mut rw = results.start(&cols)?;
        rw.write_col(42)?;
        rw.write_col("b's value")?;
        rw.finish()
    }
}

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            MysqlIntermediary::run_on_tcp(Backend, s).unwrap();
        }
    });

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 1);
    drop(db);
    jh.join().unwrap();
}
```
