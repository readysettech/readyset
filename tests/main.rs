extern crate msql_srv;
extern crate mysql;
extern crate mysql_common as myc;
extern crate nom;

use std::thread;
use std::net;
use std::io;

use msql_srv::{Column, ErrorKind, MysqlIntermediary, MysqlShim, ParamParser, QueryResultWriter,
               StatementMetaWriter};

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
    E: FnMut(u32, Vec<msql_srv::Value>, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
{
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
        (self.on_e)(id, params.iter(&self.params[..]).collect(), results)
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
        + FnMut(u32, Vec<msql_srv::Value>, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
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

    fn test<C>(self, c: C)
    where
        C: FnOnce(&mut mysql::Conn) -> (),
    {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let jh = thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            MysqlIntermediary::run_on_tcp(self, s)
        });

        let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
        c(&mut db);
        drop(db);
        jh.join().unwrap().unwrap();
    }
}

#[test]
fn it_connects() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|_| {})
}

#[test]
fn it_pings() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| assert_eq!(db.ping(), true))
}

#[test]
fn empty_response() {
    TestingShim::new(
        |_, w| w.completed(0, 0),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn no_rows() {
    let cols = [
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    TestingShim::new(
        move |_, w| w.start(&cols[..])?.finish(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn no_columns() {
    TestingShim::new(
        move |_, w| w.start(&[])?.finish(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn no_columns_but_rows() {
    TestingShim::new(
        move |_, w| w.start(&[])?.write_col(42).map(|_| ()),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn error_response() {
    let err = (ErrorKind::ER_NO, "clearly not");
    TestingShim::new(
        move |_, w| w.error(err.0, err.1.as_bytes()),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
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
fn empty_on_drop() {
    let cols = [
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    TestingShim::new(
        move |_, w| w.start(&cols[..]).map(|_| ()),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
    })
}

#[test]
fn it_queries_nulls() {
    TestingShim::new(
        |_, w| {
            let cols = &[
                Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: mysql::consts::ColumnFlags::empty(),
                },
            ];
            let mut w = w.start(cols)?;
            w.write_col(None::<i16>)?;
            w.finish()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        let row = db.query("SELECT a, b FROM foo")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.as_ref(0), Some(&msql_srv::Value::NULL));
    })
}

#[test]
fn it_queries() {
    TestingShim::new(
        |_, w| {
            let cols = &[
                Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: mysql::consts::ColumnFlags::empty(),
                },
            ];
            let mut w = w.start(cols)?;
            w.write_col(1024i16)?;
            w.finish()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    ).test(|db| {
        let row = db.query("SELECT a, b FROM foo")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024));
    })
}

#[test]
fn it_prepares() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            assert_eq!(stmt, 41);
            assert_eq!(params, vec![msql_srv::Value::Int(42)]);

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
        },
    ).with_params(params)
        .with_columns(cols2)
        .test(|db| {
            let row = db.prep_exec("SELECT a FROM b WHERE c = ?", (42i16,))
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024i16));
        })
}

#[test]
fn prepared_empty() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert!(!params.is_empty());
            w.completed(0, 0)
        },
    ).with_params(params)
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
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
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
    ).with_params(params)
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
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert_eq!(
                params,
                vec![msql_srv::Value::NULL, msql_srv::Value::Int(42)]
            );

            let mut w = w.start(&cols)?;
            w.write_row(vec![None::<i16>, Some(42)])?;
            w.finish()
        },
    ).with_params(params)
        .with_columns(cols2)
        .test(|db| {
            let row = db.prep_exec(
                "SELECT a, b FROM x WHERE c = ? AND d = ?",
                (msql_srv::Value::NULL, 42),
            ).unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(row.as_ref(0), Some(&msql_srv::Value::NULL));
            assert_eq!(row.get::<i16, _>(1), Some(42));
        })
}

#[test]
fn prepared_no_rows() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&cols[..])?.finish(),
    ).with_columns(cols2)
        .test(|db| {
            assert_eq!(db.prep_exec("SELECT a, b FROM foo", ()).unwrap().count(), 0);
        })
}

#[test]
fn prepared_no_cols_but_rows() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&[])?.write_col(42).map(|_| ()),
    ).test(|db| {
        assert_eq!(db.prep_exec("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn prepared_no_cols() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&[])?.finish(),
    ).test(|db| {
        assert_eq!(db.prep_exec("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}
