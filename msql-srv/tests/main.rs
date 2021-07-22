#![warn(clippy::dbg_macro)]
#![feature(async_closure)]

extern crate chrono;
extern crate msql_srv;
extern crate mysql;
extern crate mysql_common as myc;
extern crate nom;
#[macro_use]
extern crate async_trait;
extern crate tokio;

use std::io;
use std::net;
use std::thread;
use tokio::io::AsyncWrite;

use msql_srv::{
    Column, ErrorKind, InitWriter, MysqlIntermediary, MysqlShim, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use tokio::net::tcp::OwnedWriteHalf;

struct TestingShim<Q, P, E, I, W> {
    columns: Vec<Column>,
    params: Vec<Column>,
    on_q: Q,
    on_p: P,
    on_e: E,
    on_i: I,
    _phantom: PhantomData<W>,
}

#[async_trait]
impl<Q, P, E, I, W> MysqlShim<W> for TestingShim<Q, P, E, I, W>
where
    Q: for<'a> FnMut(
            &'a str,
            QueryResultWriter<'a, W>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send,
    P: FnMut(&str) -> u32 + Send,
    E: for<'a> FnMut(
            u32,
            Vec<msql_srv::ParamValue>,
            QueryResultWriter<'a, W>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send,
    I: for<'a> FnMut(
            &'a str,
            InitWriter<'a, W>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Error = io::Error;

    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
    ) -> io::Result<()> {
        let id = (self.on_p)(query);
        info.reply(id, &self.params, &self.columns).await
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        (self.on_e)(id, params.into_iter().collect(), results).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_init(&mut self, schema: &str, writer: InitWriter<'_, W>) -> io::Result<()> {
        (self.on_i)(schema, writer).await
    }

    async fn on_query(&mut self, query: &str, results: QueryResultWriter<'_, W>) -> io::Result<()> {
        (self.on_q)(query, results).await
    }

    fn password_for_username(&self, username: &[u8]) -> Option<Vec<u8>> {
        if username == b"user" {
            Some(b"password".to_vec())
        } else {
            None
        }
    }
}

impl<Q, P, E, I> TestingShim<Q, P, E, I, OwnedWriteHalf>
where
    Q: for<'a> FnMut(
            &'a str,
            QueryResultWriter<'a, OwnedWriteHalf>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
    P: FnMut(&str) -> u32 + Send + 'static,
    E: for<'a> FnMut(
            u32,
            Vec<msql_srv::ParamValue>,
            QueryResultWriter<'a, OwnedWriteHalf>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
    I: for<'a> FnMut(
            &'a str,
            InitWriter<'a, OwnedWriteHalf>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
{
    fn new(on_q: Q, on_p: P, on_e: E, on_i: I) -> Self {
        TestingShim {
            columns: Vec::new(),
            params: Vec::new(),
            on_q,
            on_p,
            on_e,
            on_i,
            _phantom: PhantomData,
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

    fn test<C>(self, c: C)
    where
        C: FnOnce(&mut mysql::Conn),
    {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let port = listener.local_addr().unwrap().port();
        let jh = thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            let s = {
                let _guard = rt.handle().enter();
                tokio::net::TcpStream::from_std(s).unwrap()
            };
            rt.block_on(MysqlIntermediary::run_on_tcp(self, s))
        });

        let mut db =
            mysql::Conn::new(&format!("mysql://user:password@127.0.0.1:{}", port)).unwrap();
        c(&mut db);
        drop(db);
        jh.join().unwrap().unwrap();
    }
}

#[test]
fn it_connects() {
    TestingShim::new(
        move |_, _| unreachable!(),
        move |_| unreachable!(),
        move |_, _, _| unreachable!(),
        move |_, _| unreachable!(),
    )
    .test(|_| {})
}

/*
#[test]
fn failed_authentication() {
    let shim = TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    );
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let jh = thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        MysqlIntermediary::run_on_tcp(shim, s)
    });

    let res = mysql::Conn::new(&format!("mysql://user:bad_password@127.0.0.1:{}", port));
    assert!(res.is_err());
    match res.err().unwrap() {
        mysql::Error::MySqlError(err) => {
            assert_eq!(err.code, u16::from(ErrorKind::ER_ACCESS_DENIED_ERROR));
            assert_eq!(err.message, "Access denied for user user".to_owned());
        }
        err => panic!("Not a mysql error: {:?}", err),
    }

    jh.join().unwrap().unwrap();
}
 */
#[test]
fn it_inits_ok() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |schema, writer| {
            assert_eq!(schema, "test");
            Box::pin(async move { writer.ok().await })
        },
    )
    .test(|db| assert_eq!(true, db.select_db("test")));
}

#[test]
fn it_inits_error() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |schema, writer| {
            assert_eq!(schema, "test");
            Box::pin(async move {
                writer
                    .error(
                        ErrorKind::ER_BAD_DB_ERROR,
                        format!("Database {} not found", schema).as_bytes(),
                    )
                    .await
            })
        },
    )
    .test(|db| assert_eq!(false, db.select_db("test")));
}

#[test]
fn it_pings() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| assert_eq!(db.ping(), true))
}

#[test]
fn empty_response() {
    TestingShim::new(
        |_, w| Box::pin(async move { w.completed(0, 0).await }),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
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
        move |_, w| {
            let cols = cols.clone();
            Box::pin(async move { w.start(&cols[..]).await?.finish().await })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn no_columns() {
    TestingShim::new(
        move |_, w| Box::pin(async move { w.start(&[]).await?.finish().await }),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn no_columns_but_rows() {
    TestingShim::new(
        move |_, w| {
            Box::pin(async move {
                let mut w = w.start(&[]).await?;
                w.write_col(42i32).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn error_response() {
    let err = (ErrorKind::ER_NO, "clearly not");
    TestingShim::new(
        move |_, w| Box::pin(async move { w.error(err.0, err.1.as_bytes()).await }),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        if let mysql::Error::MySqlError(e) = db.query("SELECT a, b FROM foo").unwrap_err() {
            assert_eq!(
                e,
                mysql::error::MySqlError {
                    state: String::from_utf8(err.0.sqlstate().to_vec()).unwrap(),
                    message: err.1.to_owned(),
                    code: err.0 as u16,
                }
            );
        } else {
            unreachable!();
        }
    })
}

#[test]
fn it_queries_nulls() {
    TestingShim::new(
        |_, w| {
            let cols = [Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(None::<i16>).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        let row = db
            .query("SELECT a, b FROM foo")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.as_ref(0), Some(&mysql::Value::NULL));
    })
}

#[test]
fn it_queries() {
    TestingShim::new(
        |_, w| {
            let cols = [Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        let row = db
            .query("SELECT a, b FROM foo")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024));
    })
}

#[test]
fn multi_result() {
    TestingShim::new(
        |_, w| {
            let cols = [Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            Box::pin(async move {
                let mut row = w.start(&cols).await?;
                row.write_col(1024i16).await?;
                let w = row.finish_one().await?;
                let mut row = w.start(&cols).await?;
                row.write_col(1025i16).await?;
                row.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        let mut result = db.query("SELECT a FROM foo; SELECT a FROM foo").unwrap();
        assert!(result.more_results_exists());
        let row1: Vec<_> = result
            .by_ref()
            .filter_map(|row| row.unwrap().get::<i16, _>(0))
            .collect();
        assert_eq!(row1, vec![1024]);
        assert!(result.more_results_exists());
        let row2: Vec<_> = result
            .by_ref()
            .filter_map(|row| row.unwrap().get::<i16, _>(0))
            .collect();
        assert_eq!(row2, vec![1025]);
        assert!(!result.more_results_exists());
    })
}

#[test]
fn it_queries_many_rows() {
    TestingShim::new(
        |_, w| {
            let cols = [
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
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16).await?;
                w.write_col(1025i16).await?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(|db| {
        let mut rows = 0;
        for row in db.query("SELECT a, b FROM foo").unwrap() {
            let row = row.unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024));
            assert_eq!(row.get::<i16, _>(1), Some(1025));
            rows += 1;
        }
        assert_eq!(rows, 2);
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

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16).await?;
                w.finish().await
            })
        },
        |_, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        let row = db
            .prep_exec("SELECT a FROM b WHERE c = ?", (42i16,))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024i16));
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

            Box::pin(async move { w.completed(42, 1).await })
        },
        |_, _| unreachable!(),
    )
    .with_params(params)
    .test(|db| {
        let res = db
            .prep_exec(
                "INSERT INTO `users` \
                 (`username`, `email`, `password_digest`, `created_at`, \
                 `session_token`, `rss_token`, `mailing_list_token`) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "user199",
                    "user199@example.com",
                    "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka",
                    mysql::Value::Date(2018, 4, 6, 13, 0, 56, 0),
                    "token199",
                    "rsstoken199",
                    "mtok199",
                ),
            )
            .unwrap();
        assert_eq!(res.affected_rows(), 42);
        assert_eq!(res.last_insert_id(), 1);
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

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16).await?;
                w.finish().await
            })
        },
        |_, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        let row = db
            .prep_exec("SELECT a FROM b WHERE c = ?", (b"Hello world",))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024i16));
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

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16).await?;
                w.write_col(1025i16).await?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            })
        },
        |_, _| unreachable!(),
    )
    .with_params(Vec::new())
    .with_columns(cols2)
    .test(|db| {
        let mut rows = 0;
        for row in db.prep_exec("SELECT a, b FROM x", ()).unwrap() {
            let row = row.unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024));
            assert_eq!(row.get::<i16, _>(1), Some(1025));
            rows += 1;
        }
        assert_eq!(rows, 2);
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
    let cols2 = cols;
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
            Box::pin(async move { w.completed(0, 0).await })
        },
        |_, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        assert_eq!(
            db.prep_exec("SELECT a FROM b WHERE c = ?", (42i16,))
                .unwrap()
                .count(),
            0
        );
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
            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16).await?;
                w.finish().await
            })
        },
        |_, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        let row = db.prep_exec("foo", ()).unwrap().next().unwrap().unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024i16));
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
                myc::constants::ColumnType::MYSQL_TYPE_NULL
            );
            // rust-mysql sends all numbers as LONGLONG :'(
            assert_eq!(
                params[1].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
            );
            assert_eq!(Into::<i8>::into(params[1].value), 42i8);

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_row(vec![None::<i16>, Some(42)]).await?;
                w.finish().await
            })
        },
        |_, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        let row = db
            .prep_exec(
                "SELECT a, b FROM x WHERE c = ? AND d = ?",
                (mysql::Value::NULL, 42),
            )
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.as_ref(0), Some(&mysql::Value::NULL));
        assert_eq!(row.get::<i16, _>(1), Some(42));
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
        move |_, _, w| {
            let cols = cols.clone();
            Box::pin(async move { w.start(&cols[..]).await?.finish().await })
        },
        |_, _| unreachable!(),
    )
    .with_columns(cols2)
    .test(|db| {
        assert_eq!(db.prep_exec("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn prepared_no_cols_but_rows() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            Box::pin(async move {
                let mut w = w.start(&[]).await?;
                w.write_col(42).await?;
                w.finish().await
            })
        },
        |_, _| unreachable!(),
    )
    .test(|db| {
        assert_eq!(db.prep_exec("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn prepared_no_cols() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| Box::pin(async move { w.start(&[]).await?.finish().await }),
        |_, _| unreachable!(),
    )
    .test(|db| {
        assert_eq!(db.prep_exec("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn really_long_query() {
    let long = "CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    TestingShim::new(
        move |q, w| {
            assert_eq!(q, long);
            Box::pin(async move { w.start(&[]).await?.finish().await })
        },
        |_| 0,
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(move |db| {
        db.query(long).unwrap();
    })
}
