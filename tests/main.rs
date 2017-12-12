extern crate msql_proto;
extern crate mysql;
extern crate nom;

use std::io::prelude::*;
use std::io::BufWriter;
use std::thread;
use std::net;
use std::iter;

use msql_proto::PacketWriter;

fn test_with_server<C, S: Send>(c: C, s: S)
where
    C: FnOnce(&mut mysql::Conn) -> (),
    S: 'static + FnMut(&mut PacketWriter<BufWriter<net::TcpStream>>, msql_proto::Command) -> (),
{
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let jh = thread::spawn(move || server(listener, s));

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    c(&mut db);
    drop(db);
    jh.join().unwrap();
}

fn ok<W: Write>(w: &mut msql_proto::PacketWriter<W>) {
    w.write_all(&[0x00]).unwrap(); // OK packet type
    w.write_all(&[0x00]).unwrap(); // 0 rows (with lenenc encoding)
    w.write_all(&[0x00]).unwrap(); // no inserted rows
    w.write_all(&[0x00, 0x00]).unwrap(); // no server status
    w.write_all(&[0x00, 0x00]).unwrap(); // no warnings
    w.flush().unwrap();
}

fn server<S>(l: net::TcpListener, mut serve: S)
where
    S: FnMut(&mut PacketWriter<BufWriter<net::TcpStream>>, msql_proto::Command) -> (),
{
    if let Ok((s, _)) = l.accept() {
        let r = s;
        let w = r.try_clone().unwrap();
        let mut r = msql_proto::PacketReader::new(r);
        let mut w = msql_proto::PacketWriter::new(BufWriter::new(w));

        w.write_all(&[10]).unwrap(); // protocol 10
        w.write_all(&b"0.0.1-alpha-msql-proxy\0"[..]).unwrap();
        w.write_all(&[0x08, 0x00, 0x00, 0x00]).unwrap(); // connection ID
        w.write_all(&b";X,po_k}\0"[..]).unwrap(); // auth seed
        w.write_all(&[0x00, 0x42]).unwrap(); // just 4.1 proto
        w.write_all(&[0x21]).unwrap(); // UTF8_GENERAL_CI
        w.write_all(&[0x00, 0x00]).unwrap(); // status flags
        w.write_all(&[0x00, 0x00]).unwrap(); // extended capabilities
        w.write_all(&[0x00]).unwrap(); // no plugins
        w.write_all(&[0x00; 6][..]).unwrap(); // filler
        w.write_all(&[0x00; 4][..]).unwrap(); // filler
        w.write_all(&b">o6^Wz!/kM}N\0"[..]).unwrap(); // 4.1+ servers must extend salt
        w.flush().unwrap();;

        {
            let (seq, handshake) = r.next().unwrap().unwrap();
            let _handshake = msql_proto::client_handshake(&handshake).unwrap().1;
            w.set_seq(seq + 1);
        }

        ok(&mut w);

        while let Ok(Some((seq, packet))) = r.next() {
            w.set_seq(seq + 1);
            let cmd = msql_proto::command(&packet).unwrap().1;
            match cmd {
                msql_proto::Command::Query(ref q) => {
                    if q.starts_with(b"SELECT @@") {
                        let var = &q[b"SELECT @@".len()..];
                        match var {
                            b"max_allowed_packet" => {
                                msql_proto::start_resultset_text(
                                    iter::once(msql_proto::Column {
                                        schema: "",
                                        table_alias: "",
                                        table: "",
                                        column_alias: "@@max_allowed_packet",
                                        column: "",
                                        coltype: mysql::consts::ColumnType::MYSQL_TYPE_LONGLONG,
                                        colflags: mysql::consts::ColumnFlags::UNSIGNED_FLAG
                                            | mysql::consts::ColumnFlags::BINARY_FLAG,
                                    }),
                                    &mut w,
                                ).unwrap();
                                msql_proto::write_resultset_text(
                                    iter::once(iter::once(mysql::Value::UInt(1024))),
                                    &mut w,
                                ).unwrap();
                                w.flush().unwrap();
                                continue;
                            }
                            b"socket" => {
                                ok(&mut w);
                                continue;
                            }
                            _ => {}
                        }
                    }
                }
                msql_proto::Command::Quit => {
                    ok(&mut w);
                    break;
                }
                _ => {}
            }

            serve(&mut w, cmd);
        }
    }
}

#[test]
fn it_connects() {
    test_with_server(
        |_| {},
        |_, _| {
            unreachable!();
        },
    );
}

#[test]
fn it_pings() {
    test_with_server(
        |db| assert_eq!(db.ping(), true),
        |w, cmd| {
            if let msql_proto::Command::Ping = cmd {
                ok(w);
            } else {
                unreachable!();
            }
        },
    );
}

#[test]
fn it_queries() {
    test_with_server(
        |db| {
            let row = db.query("SELECT a, b FROM foo")
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(row.get::<i32, _>(0), Some(1024));
        },
        |w, cmd| {
            if let msql_proto::Command::Query(_) = cmd {
                msql_proto::start_resultset_text(
                    iter::once(msql_proto::Column {
                        schema: "",
                        table_alias: "",
                        table: "",
                        column_alias: "a",
                        column: "",
                        coltype: mysql::consts::ColumnType::MYSQL_TYPE_LONG,
                        colflags: mysql::consts::ColumnFlags::empty(),
                    }),
                    w,
                ).unwrap();
                msql_proto::write_resultset_text(
                    iter::once(iter::once(mysql::Value::Int(1024))),
                    w,
                ).unwrap();
                w.flush().unwrap();
            } else {
                unreachable!();
            }
        },
    );
}
