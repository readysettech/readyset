extern crate chrono;
extern crate futures;
extern crate msql_srv;
extern crate mysql_async;
extern crate mysql_common as myc;
extern crate nom;
extern crate tokio_core;

use futures::{Future, IntoFuture};
use mysql_async::prelude::*;
use std::io;
use std::net;
use std::thread;
use tokio_core::reactor::Core;

use msql_srv::{
    Column, ErrorKind, MysqlIntermediary, MysqlShim, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};

struct TestingShim<Q, P, E> {
    columns: Vec<Column>,
    params: Vec<Column>,
    on_q: Q,
    on_p: P,
    on_e: E,
}

impl<Q, P, E> MysqlShim<net::TcpStream> for TestingShim<Q, P, E>
where
    Q: FnMut(&str, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
    P: FnMut(&str) -> u32,
    E: FnMut(u32, Vec<msql_srv::ParamValue>, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
{
    type Error = io::Error;

    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<net::TcpStream>,
    ) -> io::Result<()> {
        let id = (self.on_p)(query);
        info.reply(id, &self.params, &self.columns)
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser,
        results: QueryResultWriter<net::TcpStream>,
    ) -> io::Result<()> {
        (self.on_e)(id, params.into_iter().collect(), results)
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<net::TcpStream>,
    ) -> io::Result<()> {
        (self.on_q)(query, results)
    }
}

impl<Q, P, E> TestingShim<Q, P, E>
where
    Q: 'static + Send + FnMut(&str, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
    P: 'static + Send + FnMut(&str) -> u32,
    E: 'static
        + Send
        + FnMut(u32, Vec<msql_srv::ParamValue>, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
{
    fn new(on_q: Q, on_p: P, on_e: E) -> Self {
        TestingShim {
            columns: Vec::new(),
            params: Vec::new(),
            on_q,
            on_p,
            on_e,
        }
    }

    fn with_params(mut self, p: Vec<Column>) -> Self {
        self.params = p;
        self
    }

    fn with_columns(mut self, c: Vec<Column>) -> Self {
        self.columns = c;
        self
    }

    fn test<C, F>(self, c: C)
    where
        F: IntoFuture<Item = (), Error = mysql_async::errors::Error>,
        C: FnOnce(mysql_async::Conn) -> F,
    {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let jh = thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            MysqlIntermediary::run_on_tcp(self, s)
        });

        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let db = core
            .run(mysql_async::Conn::new(
                &format!("mysql://127.0.0.1:{}", port),
                &handle,
            ))
            .unwrap();
        core.run(c(db).into_future()).unwrap();
        jh.join().unwrap().unwrap();
    }
}

#[test]
fn it_connects() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|_| Ok(()))
}

#[test]
fn it_pings() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| db.ping().map(|_| ()))
}

#[test]
fn empty_response() {
    TestingShim::new(
        |_, w| w.completed(0, 0),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn no_rows() {
    let cols = [Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    TestingShim::new(
        move |_, w| w.start(&cols[..])?.finish(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn no_columns() {
    TestingShim::new(
        move |_, w| w.start(&[])?.finish(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn no_columns_but_rows() {
    TestingShim::new(
        move |_, w| w.start(&[])?.write_col(42).map(|_| ()),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn error_response() {
    let err = (ErrorKind::ER_NO, "clearly not");
    TestingShim::new(
        move |_, w| w.error(err.0, err.1.as_bytes()),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo").then(|r| {
            match r {
                Ok(_) => assert!(false),
                Err(mysql_async::errors::Error(
                    mysql_async::errors::ErrorKind::Server(ref state, code, ref msg),
                    _,
                )) => {
                    assert_eq!(
                        state,
                        &String::from_utf8(err.0.sqlstate().to_vec()).unwrap()
                    );
                    assert_eq!(code, err.0 as u16);
                    assert_eq!(msg, &err.1);
                }
                Err(e) => {
                    eprintln!("unexpected {:?}", e);
                    assert!(false);
                }
            }
            Ok(())
        })
    })
}

#[test]
fn empty_on_drop() {
    let cols = [Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    TestingShim::new(
        move |_, w| w.start(&cols[..]).map(|_| ()),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn it_queries_nulls() {
    TestingShim::new(
        |_, w| {
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut w = w.start(cols)?;
            w.write_col(None::<i16>)?;
            w.finish()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 1);
                assert_eq!(rs[0].len(), 1);
                assert_eq!(rs[0][0], mysql_async::Value::NULL);
                Ok(())
            })
    })
}

#[test]
fn it_queries() {
    TestingShim::new(
        |_, w| {
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut w = w.start(cols)?;
            w.write_col(1024i16)?;
            w.finish()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 1);
                assert_eq!(rs[0].len(), 1);
                assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
                Ok(())
            })
    })
}

#[test]
fn it_queries_many_rows() {
    TestingShim::new(
        |_, w| {
            let cols = &[
                Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "b".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut w = w.start(cols)?;
            w.write_col(1024i16)?;
            w.write_col(1025i16)?;
            w.end_row()?;
            w.write_row(&[1024i16, 1025i16])?;
            w.finish()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        db.query("SELECT a, b FROM foo")
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 2);
                assert_eq!(rs[0].len(), 2);
                assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
                assert_eq!(rs[0].get::<i16, _>(1), Some(1025));
                assert_eq!(rs[1].len(), 2);
                assert_eq!(rs[1].get::<i16, _>(0), Some(1024));
                assert_eq!(rs[1].get::<i16, _>(1), Some(1025));
                Ok(())
            })
    })
}

#[test]
fn it_prepares() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 1);
            // rust-mysql sends all numbers as LONGLONG
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
            );
            assert_eq!(Into::<i8>::into(params[0].value), 42i8);

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec("SELECT a FROM b WHERE c = ?", (42i16,))
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 1);
                assert_eq!(rs[0].len(), 1);
                assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
                Ok(())
            })
    })
}

#[test]
fn insert_exec() {
    let params = vec![
        Column {
            table: String::new(),
            column: "username".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "email".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "pw".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "created".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_DATETIME,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "session".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "rss".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "mail".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 1,
        move |_, params, w| {
            assert_eq!(params.len(), 7);
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[1].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[2].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[3].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_DATETIME
            );
            assert_eq!(
                params[4].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[5].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[6].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(Into::<&str>::into(params[0].value), "user199");
            assert_eq!(Into::<&str>::into(params[1].value), "user199@example.com");
            assert_eq!(
                Into::<&str>::into(params[2].value),
                "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka"
            );
            assert_eq!(
                Into::<chrono::NaiveDateTime>::into(params[3].value),
                chrono::NaiveDate::from_ymd(2018, 4, 6).and_hms(13, 0, 56)
            );
            assert_eq!(Into::<&str>::into(params[4].value), "token199");
            assert_eq!(Into::<&str>::into(params[5].value), "rsstoken199");
            assert_eq!(Into::<&str>::into(params[6].value), "mtok199");

            w.completed(42, 1)
        },
    )
    .with_params(params)
    .test(|db| {
        db.prep_exec(
            "INSERT INTO `users` \
             (`username`, `email`, `password_digest`, `created_at`, \
             `session_token`, `rss_token`, `mailing_list_token`) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "user199",
                "user199@example.com",
                "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka",
                mysql_async::Value::Date(2018, 4, 6, 13, 0, 56, 0),
                "token199",
                "rsstoken199",
                "mtok199",
            ),
        )
        .and_then(|res| {
            assert_eq!(res.affected_rows(), 42);
            assert_eq!(res.last_insert_id(), Some(1));
            Ok(())
        })
    })
}

#[test]
fn send_long() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 1);
            // rust-mysql sends all strings as VAR_STRING
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(Into::<&[u8]>::into(params[0].value), b"Hello world");

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec("SELECT a FROM b WHERE c = ?", (b"Hello world",))
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 1);
                assert_eq!(rs[0].len(), 1);
                assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
                Ok(())
            })
    })
}

#[test]
fn it_prepares_many() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a, b FROM x");
            41
        },
        move |stmt, params, w| {
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 0);

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.write_col(1025i16)?;
            w.end_row()?;
            w.write_row(&[1024i16, 1025i16])?;
            w.finish()
        },
    )
    .with_params(Vec::new())
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec("SELECT a, b FROM x", ())
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 2);
                assert_eq!(rs[0].len(), 2);
                assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
                assert_eq!(rs[0].get::<i16, _>(1), Some(1025));
                assert_eq!(rs[1].len(), 2);
                assert_eq!(rs[1].get::<i16, _>(0), Some(1024));
                assert_eq!(rs[1].get::<i16, _>(1), Some(1025));
                Ok(())
            })
    })
}

#[test]
fn prepared_empty() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert!(!params.is_empty());
            w.completed(0, 0)
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec("SELECT a FROM b WHERE c = ?", (42i16,))
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn prepared_no_params() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert!(params.is_empty());
            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec("foo", ())
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 1);
                assert_eq!(rs[0].len(), 1);
                assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
                Ok(())
            })
    })
}

#[test]
fn prepared_nulls() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert_eq!(params.len(), 2);
            assert!(params[0].value.is_null());
            assert!(!params[1].value.is_null());
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_SHORT
            );
            // rust-mysql sends all numbers as LONGLONG :'(
            assert_eq!(
                params[1].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
            );
            assert_eq!(Into::<i8>::into(params[1].value), 42i8);

            let mut w = w.start(&cols)?;
            w.write_row(vec![None::<i16>, Some(42)])?;
            w.finish()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec(
            "SELECT a, b FROM x WHERE c = ? AND d = ?",
            (mysql_async::Value::NULL, 42),
        )
        .and_then(|r| r.collect::<mysql_async::Row>())
        .and_then(|(_, rs)| {
            assert_eq!(rs.len(), 1);
            assert_eq!(rs[0].len(), 2);
            assert_eq!(rs[0].get::<Option<i16>, _>(0), Some(None));
            assert_eq!(rs[0].get::<i16, _>(1), Some(42));
            Ok(())
        })
    })
}

#[test]
fn prepared_no_rows() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&cols[..])?.finish(),
    )
    .with_columns(cols2)
    .test(|db| {
        db.prep_exec("SELECT a, b FROM foo", ())
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn prepared_no_cols_but_rows() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&[])?.write_col(42).map(|_| ()),
    )
    .test(|db| {
        db.prep_exec("SELECT a, b FROM foo", ())
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}

#[test]
fn prepared_no_cols() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&[])?.finish(),
    )
    .test(|db| {
        db.prep_exec("SELECT a, b FROM foo", ())
            .and_then(|r| r.collect::<mysql_async::Row>())
            .and_then(|(_, rs)| {
                assert_eq!(rs.len(), 0);
                Ok(())
            })
    })
}
