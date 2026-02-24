extern crate chrono;
extern crate mysql_async as mysql;
extern crate mysql_common as myc;
extern crate mysql_srv;
extern crate tokio;

use core::iter;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::{io, net};

use database_utils::TlsMode;
use futures::StreamExt;
use mysql::consts::Command;
use mysql::prelude::Queryable;
use mysql::{Row, ServerError};
use mysql_srv::{
    CachedSchema, Column, ErrorKind, MySqlIntermediary, MySqlShim, ParamParser, QueryResultWriter,
    QueryResultsResponse, StatementMetaWriter,
};
use readyset_adapter_types::DeallocateId;
use readyset_util::redacted::RedactedString;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

static DEFAULT_CHARACTER_SET: u16 = myc::constants::UTF8_GENERAL_CI;

struct TestingShim<Q, P, E, I, CU, W> {
    columns: Vec<Column>,
    params: Vec<Column>,
    on_q: Q,
    on_p: P,
    on_e: E,
    on_i: I,
    on_cu: CU,
    _phantom: PhantomData<W>,
}

impl<Q, P, E, I, CU, W> MySqlShim<W> for TestingShim<Q, P, E, I, CU, W>
where
    Q: for<'a> FnMut(
            &'a str,
            QueryResultWriter<'a, W>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send,
    P: FnMut(&str) -> u32 + Send,
    E: for<'a> FnMut(
            u32,
            Vec<mysql_srv::ParamValue>,
            QueryResultWriter<'a, W>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send,
    I: for<'a> FnMut(&'a str) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>> + Send,
    CU: for<'a> FnMut(
            &'a str,
            &'a str,
            &'a str,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send,
    W: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
        _schema_cache: &mut HashMap<u32, CachedSchema>,
    ) -> io::Result<()> {
        let id = (self.on_p)(query);
        info.reply(id, &self.params, &self.columns).await
    }

    async fn set_auth_info(
        &mut self,
        _user: &str,
        _password: Option<RedactedString>,
    ) -> io::Result<()> {
        Ok(())
    }

    async fn set_charset(&mut self, _charset: u16) -> io::Result<()> {
        Ok(())
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
        _schema_cache: &mut HashMap<u32, CachedSchema>,
    ) -> io::Result<()> {
        let mut extract_params = Vec::new();
        for p in params {
            extract_params.push(p.map_err(|e| {
                let e: std::io::Error = e.into();
                e
            })?);
        }
        (self.on_e)(id, extract_params, results).await
    }

    async fn on_close(&mut self, _: DeallocateId) {}

    async fn on_ping(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn on_reset(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn on_init(&mut self, schema: &str) -> io::Result<()> {
        (self.on_i)(schema).await
    }

    async fn on_change_user(
        &mut self,
        username: &str,
        password: &str,
        schema: &str,
    ) -> io::Result<()> {
        (self.on_cu)(username, password, schema).await
    }
    async fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> QueryResultsResponse {
        if query.starts_with("SELECT @@") || query.starts_with("select @@") {
            let var = &query.get(b"SELECT @@".len()..);
            match var {
                Some("max_allowed_packet") => {
                    let cols = &[Column {
                        table: String::new(),
                        column: "@@max_allowed_packet".to_owned(),
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                        column_length: 11,
                        colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                        character_set: DEFAULT_CHARACTER_SET,
                        decimals: 0,
                    }];
                    let mut w = results.start(cols).await.expect("cols");
                    w.write_row(iter::once(67108864u32)).await.expect("writer");
                    QueryResultsResponse::IoResult(w.finish().await)
                }
                _ => QueryResultsResponse::IoResult(results.completed(0, 0, None).await),
            }
        } else {
            QueryResultsResponse::IoResult((self.on_q)(query, results).await)
        }
    }

    fn password_for_username(&self, username: &str) -> Option<Vec<u8>> {
        if username == "user" {
            Some(b"password".to_vec())
        } else {
            None
        }
    }

    fn version(&self) -> String {
        "8.0.26-readyset\0".to_string()
    }
}

impl<Q, P, E, I, CU> TestingShim<Q, P, E, I, CU, TcpStream>
where
    Q: for<'a> FnMut(
            &'a str,
            QueryResultWriter<'a, TcpStream>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
    P: FnMut(&str) -> u32 + Send + 'static,
    E: for<'a> FnMut(
            u32,
            Vec<mysql_srv::ParamValue>,
            QueryResultWriter<'a, TcpStream>,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
    I: for<'a> FnMut(&'a str) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
    CU: for<'a> FnMut(
            &'a str,
            &'a str,
            &'a str,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a + Send>>
        + Send
        + 'static,
{
    fn new(on_q: Q, on_p: P, on_e: E, on_i: I, on_cu: CU) -> Self {
        TestingShim {
            columns: Vec::new(),
            params: Vec::new(),
            on_q,
            on_p,
            on_e,
            on_i,
            on_cu,
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

    async fn test<C>(self, c: C)
    where
        C: for<'a> FnOnce(&'a mut mysql::Conn) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
    {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        // Convert to async TcpListener
        let listener = {
            listener
                .set_nonblocking(true)
                .expect("couldn't set nonblocking");
            TcpListener::from_std(listener).unwrap()
        };

        // Spawn the server task
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            MySqlIntermediary::run_on_tcp(self, socket, false, None, TlsMode::Optional).await
        });

        // Connect to the server
        let mut db = mysql::Conn::new(
            mysql::Opts::from_url(&format!("mysql://user:password@127.0.0.1:{port}")).unwrap(),
        )
        .await
        .unwrap();

        // Run the test closure
        c(&mut db).await;

        // Clean up
        drop(db);
        server_handle.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn it_connects() {
    TestingShim::new(
        move |_, _| unreachable!(),
        move |_| unreachable!(),
        move |_, _, _| unreachable!(),
        move |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|_| Box::pin(async move {}))
    .await;
}

/*
#[test]
fn failed_authentication() {
    let shim = TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
    );
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let jh = thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        MySqlIntermediary::run_on_tcp(shim, s, false)
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
#[tokio::test]
async fn it_inits_ok() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |schema| {
            assert_eq!(schema, "test");
            Box::pin(async move { Ok(()) })
        },
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            db.write_command_data(Command::COM_INIT_DB, "test")
                .await
                .unwrap();
            let packet = db.read_packet().await.unwrap();
            // OK packet: [0x00, affected_rows(0), last_insert_id(0), status_flags(2), warnings(2)]
            assert_eq!(packet[0], 0x00, "expected OK packet");
            let status_flags =
                myc::constants::StatusFlags::from_bits_truncate(u16::from_le_bytes([
                    packet[3], packet[4],
                ]));
            assert!(
                status_flags.contains(myc::constants::StatusFlags::SERVER_STATUS_AUTOCOMMIT),
                "COM_INIT_DB OK should include SERVER_STATUS_AUTOCOMMIT, got {status_flags:?}"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn it_inits_error() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |schema| {
            assert_eq!(schema, "test");
            Box::pin(async move { Err(io::Error::other(format!("Database {schema} not found"))) })
        },
        |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            db.write_command_data(Command::COM_INIT_DB, "test")
                .await
                .unwrap();
            let res = db.read_packet().await;
            assert!(res.is_err());
        })
    })
    .await;
}

#[tokio::test]
async fn it_pings() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| Box::pin(async move { assert!(db.ping().await.is_ok()) }))
    .await;
}

#[tokio::test]
async fn empty_response() {
    TestingShim::new(
        |_, w| Box::pin(async move { w.completed(0, 0, None).await }),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.query::<Row, _>("SELECT a, b FROM foo")
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn no_columns() {
    TestingShim::new(
        move |_, w| Box::pin(async move { w.start(&[]).await?.finish().await }),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.query::<Row, _>("SELECT a, b FROM foo")
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn no_columns_but_rows() {
    TestingShim::new(
        move |_, w| {
            Box::pin(async move {
                let mut w = w.start(&[]).await?;
                w.write_col(42i32)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.query::<Row, _>("SELECT a, b FROM foo")
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn error_response() {
    let err = (ErrorKind::ER_NO, "clearly not");
    TestingShim::new(
        move |_, w| Box::pin(async move { w.error(err.0, err.1.as_bytes()).await }),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            if let mysql::Error::Server(e) = db
                .query::<Row, _>("SELECT a, b FROM foo")
                .await
                .unwrap_err()
            {
                assert_eq!(
                    e,
                    ServerError {
                        state: String::from_utf8(err.0.sqlstate().to_vec()).unwrap(),
                        message: err.1.to_owned(),
                        code: err.0 as u16,
                    },
                );
            } else {
                unreachable!();
            }
        })
    })
    .await;
}

#[tokio::test]
async fn it_queries_nulls() {
    TestingShim::new(
        |_, w| {
            let cols = [Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                column_length: 6,
                colflags: myc::constants::ColumnFlags::empty(),
                character_set: DEFAULT_CHARACTER_SET,
                decimals: 0,
            }];
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(None::<i16>)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            let res = db.query::<Row, _>("SELECT a, b FROM foo").await.unwrap();
            let row = res.first().unwrap();
            assert_eq!(row.get(0), Some(mysql::Value::NULL));
        })
    })
    .await;
}

#[tokio::test]
async fn it_queries() {
    TestingShim::new(
        |_, w| {
            let cols = [Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                column_length: 6,
                colflags: myc::constants::ColumnFlags::empty(),
                character_set: DEFAULT_CHARACTER_SET,
                decimals: 0,
            }];
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            let res = db.query::<Row, _>("SELECT a, b FROM foo").await.unwrap();
            let row = res.first().unwrap();
            assert_eq!(row.get(0), Some(1024));
        })
    })
    .await;
}

#[tokio::test]
async fn multi_result() {
    TestingShim::new(
        |_, w| {
            let cols = [Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                column_length: 6,
                colflags: myc::constants::ColumnFlags::empty(),
                character_set: DEFAULT_CHARACTER_SET,
                decimals: 0,
            }];
            Box::pin(async move {
                let mut row = w.start(&cols).await?;
                row.write_col(1024i16)?;
                let w = row.finish_one().await?;
                let mut row = w.start(&cols).await?;
                row.write_col(1025i16)?;
                row.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            let mut result = db
                .query_iter("SELECT a FROM foo; SELECT a FROM foo")
                .await
                .unwrap();

            let mut stream1 = result.stream::<u16>().await.unwrap().unwrap();
            let row1 = stream1.next().await.unwrap().unwrap();
            assert_eq!(row1, 1024_u16);
            drop(stream1);
            let mut stream2 = result.stream::<u16>().await.unwrap().unwrap();
            let row2 = stream2.next().await.unwrap().unwrap();
            assert_eq!(row2, 1025_u16);
            drop(stream2);
        })
    })
    .await;
}

#[tokio::test]
async fn it_queries_many_rows() {
    TestingShim::new(
        |_, w| {
            let cols = [
                Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    column_length: 6,
                    colflags: myc::constants::ColumnFlags::empty(),
                    character_set: DEFAULT_CHARACTER_SET,
                    decimals: 0,
                },
                Column {
                    table: String::new(),
                    column: "b".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    column_length: 6,
                    colflags: myc::constants::ColumnFlags::empty(),
                    character_set: DEFAULT_CHARACTER_SET,
                    decimals: 0,
                },
            ];
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.write_col(1025i16)?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            let mut rows = 0;
            let mut result = db.query_iter("SELECT a, b FROM foo").await.unwrap();
            while let Ok(Some(row)) = result.next().await {
                assert_eq!(row.get::<i16, _>(0), Some(1024));
                assert_eq!(row.get::<i16, _>(1), Some(1025));
                rows += 1;
            }
            assert_eq!(rows, 2);
        })
    })
    .await;
}

#[tokio::test]
async fn it_prepares() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
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
            assert_eq!(
                std::convert::TryInto::<i8>::try_into(params[0].value)
                    .expect("Error calling try_into"),
                42i8
            );

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            let res = db
                .exec::<Row, _, _>("SELECT a FROM b WHERE c = ?", (42i16,))
                .await
                .unwrap();
            let row = res.first().unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024i16));
        })
    })
    .await;
}

#[tokio::test]
async fn insert_exec() {
    let params = vec![
        Column {
            table: String::new(),
            column: "username".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "email".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "pw".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "created".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_DATETIME,
            column_length: 19,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "session".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "rss".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "mail".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
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
            assert_eq!(
                std::convert::TryInto::<&str>::try_into(params[0].value)
                    .expect("Error calling try_into"),
                "user199"
            );
            assert_eq!(
                std::convert::TryInto::<&str>::try_into(params[1].value)
                    .expect("Error calling try_into"),
                "user199@example.com"
            );
            assert_eq!(
                std::convert::TryInto::<&str>::try_into(params[2].value)
                    .expect("Error calling try_into"),
                "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka"
            );
            assert_eq!(
                std::convert::TryInto::<chrono::NaiveDateTime>::try_into(params[3].value)
                    .expect("Error calling try_into"),
                chrono::NaiveDate::from_ymd_opt(2018, 4, 6)
                    .unwrap()
                    .and_hms_opt(13, 0, 56)
                    .unwrap()
            );
            assert_eq!(
                std::convert::TryInto::<&str>::try_into(params[4].value)
                    .expect("Error calling try_into"),
                "token199"
            );
            assert_eq!(
                std::convert::TryInto::<&str>::try_into(params[5].value)
                    .expect("Error calling try_into"),
                "rsstoken199"
            );
            assert_eq!(
                std::convert::TryInto::<&str>::try_into(params[6].value)
                    .expect("Error calling try_into"),
                "mtok199"
            );

            Box::pin(async move { w.completed(42, 1, None).await })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(params)
    .test(|db| {
        Box::pin(async move {
            db.exec::<Row, _, _>(
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
            .await
            .unwrap();
            assert_eq!(db.affected_rows(), 42);
            assert_eq!(db.last_insert_id(), Some(1));
        })
    })
    .await;
}

#[tokio::test]
async fn send_long() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
        column_length: 65535,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
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
            assert_eq!(
                std::convert::TryInto::<&[u8]>::try_into(params[0].value)
                    .expect("Error calling try_into"),
                b"Hello world"
            );

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            let res = db
                .exec::<Row, _, _>("SELECT a FROM b WHERE c = ?", (b"Hello world",))
                .await
                .unwrap();
            let row = res.first().unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024i16));
        })
    })
    .await;
}

#[tokio::test]
async fn it_prepares_many() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
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
                w.write_col(1024i16)?;
                w.write_col(1025i16)?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(Vec::new())
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            let mut rows = 0;
            for row in db
                .exec::<Row, _, _>("SELECT a, b FROM x", ())
                .await
                .unwrap()
            {
                assert_eq!(row.get::<i16, _>(0), Some(1024));
                assert_eq!(row.get::<i16, _>(1), Some(1025));
                rows += 1;
            }
            assert_eq!(rows, 2);
        })
    })
    .await;
}

#[tokio::test]
async fn prepared_empty() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
    }];
    let cols2 = cols;
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert!(!params.is_empty());
            Box::pin(async move { w.completed(0, 0, None).await })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.exec::<Row, _, _>("SELECT a FROM b WHERE c = ?", (42i16,))
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn prepared_no_params() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
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
                w.write_col(1024i16)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            let res = db.exec::<Row, _, _>("foo", ()).await.unwrap();
            let row = res.first().unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024i16));
        })
    })
    .await;
}

#[tokio::test]
async fn prepared_nulls() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            column_length: 6,
            colflags: myc::constants::ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
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
            assert_eq!(
                std::convert::TryInto::<i8>::try_into(params[1].value)
                    .expect("Error calling try_into"),
                42i8
            );

            let cols = cols.clone();
            Box::pin(async move {
                let mut w = w.start(&cols).await?;
                w.write_row(vec![None::<i16>, Some(42)]).await?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            let res = db
                .exec::<Row, _, _>(
                    "SELECT a, b FROM x WHERE c = ? AND d = ?",
                    (mysql::Value::NULL, 42),
                )
                .await
                .unwrap();
            let row = res.first().unwrap();
            assert_eq!(row.as_ref(0), Some(&mysql::Value::NULL));
            assert_eq!(row.get::<i16, _>(1), Some(42));
        })
    })
    .await;
}

#[tokio::test]
async fn prepared_no_rows() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        column_length: 6,
        colflags: myc::constants::ColumnFlags::empty(),
        character_set: DEFAULT_CHARACTER_SET,
        decimals: 0,
    }];
    let cols2 = cols.clone();
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            let cols = cols.clone();
            Box::pin(async move { w.start(&cols[..]).await?.finish().await })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .with_columns(cols2)
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.exec::<Row, _, _>("SELECT a, b FROM foo", ())
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn prepared_no_cols_but_rows() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            Box::pin(async move {
                let mut w = w.start(&[]).await?;
                w.write_col(42)?;
                w.finish().await
            })
        },
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.exec::<Row, _, _>("SELECT a, b FROM foo", ())
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn prepared_no_cols() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| Box::pin(async move { w.start(&[]).await?.finish().await }),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(|db| {
        Box::pin(async move {
            assert_eq!(
                db.exec::<Row, _, _>("SELECT a, b FROM foo", ())
                    .await
                    .unwrap()
                    .len(),
                0
            );
        })
    })
    .await;
}

#[tokio::test]
async fn really_long_query() {
    let long = "CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    TestingShim::new(
        move |q, w| {
            assert_eq!(q, long);
            Box::pin(async move { w.start(&[]).await?.finish().await })
        },
        |_| 0,
        |_, _, _| unreachable!(),
        |_| unreachable!(),
        move |_, _, _| unreachable!(),
    )
    .test(move |db| {
        Box::pin(async move {
            db.query::<Row, _>(long).await.unwrap();
        })
    })
    .await;
}
