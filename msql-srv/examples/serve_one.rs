//! After running this, you should be able to run:
//!
//! ```console
//! $ echo "SELECT * FROM foo" | mysql -h 127.0.0.1 --table
//! $
//! ```

extern crate msql_srv;
extern crate mysql;

use msql_srv::*;
use std::io;
use std::net;
use std::thread;

struct Backend;
impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }
    fn on_execute(
        &mut self,
        _: u32,
        _: msql_srv::ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(0, 0)
    }
    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        results.start(&[])?.finish()
    }

    fn password_for_username(&self, _username: &[u8]) -> Option<Vec<u8>> {
        Some(b"password".to_vec())
    }
}

fn main() {
    let mut threads = Vec::new();
    let listener = net::TcpListener::bind("127.0.0.1:3306").unwrap();

    while let Ok((s, _)) = listener.accept() {
        threads.push(thread::spawn(move || {
            MysqlIntermediary::run_on_tcp(Backend, s).unwrap();
        }));
    }

    for t in threads {
        t.join().unwrap();
    }
}
