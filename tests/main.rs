extern crate msql_proto;
extern crate mysql;
extern crate mysql_common as myc;
extern crate nom;

use std::thread;
use std::net;
use std::iter;
use std::io;

use msql_proto::{Column, MysqlIntermediary, MysqlShim, QueryResultWriter, StatementMetaWriter};

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
    E: FnMut(u32, Vec<myc::value::Value>, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
{
    fn param_info(&self, _: u32) -> &[Column] {
        &self.params[..]
    }

    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<net::TcpStream>,
    ) -> io::Result<()> {
        let id = (self.on_p)(query);
        info.write(id, &self.params, &self.columns)
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: Vec<myc::value::Value>,
        results: QueryResultWriter<net::TcpStream>,
    ) -> io::Result<()> {
        (self.on_e)(id, params, results)
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
        + FnMut(u32, Vec<myc::value::Value>, QueryResultWriter<net::TcpStream>) -> io::Result<()>,
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
            let srv = MysqlIntermediary::from_stream(self, s).unwrap();
            srv.run().unwrap();
        });

        let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
        c(&mut db);
        drop(db);
        jh.join().unwrap();
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
fn it_queries() {
    TestingShim::new(
        |_, w| {
            let cols = &[
                Column {
                    schema: String::new(),
                    table_alias: String::new(),
                    table: String::new(),
                    column_alias: "a".to_owned(),
                    column: String::new(),
                    coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: mysql::consts::ColumnFlags::empty(),
                },
            ];
            let mut w = w.start(cols)?;
            w.write_row(iter::once(1024i16))?;
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
            schema: String::new(),
            table_alias: String::new(),
            table: String::new(),
            column_alias: "a".to_owned(),
            column: String::new(),
            coltype: mysql::consts::ColumnType::MYSQL_TYPE_SHORT,
            colflags: mysql::consts::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            schema: String::new(),
            table_alias: String::new(),
            table: String::new(),
            column_alias: "c".to_owned(),
            column: String::new(),
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
            assert_eq!(params, vec![mysql::Value::Int(42)]);

            let mut w = w.start(&cols)?;
            w.write_row(iter::once(1024i16))?;
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
