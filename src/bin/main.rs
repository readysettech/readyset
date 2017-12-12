extern crate msql_proto;
extern crate mysql;
extern crate nom;

use std::io::prelude::*;
use std::io::BufWriter;
use std::thread;
use std::net;

fn main() {
    thread::spawn(server);

    let mut db = mysql::Conn::new("mysql://localhost:3306").unwrap();
    assert_eq!(db.ping(), true);
}

fn server() {
    let x = net::TcpListener::bind("127.0.0.1:3306").unwrap();
    while let Ok((s, _)) = x.accept() {
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

        println!("wrote greeeting");
        {
            let (seq, handshake) = r.next().unwrap().unwrap();
            println!(
                "got client handshake\n{:#?}",
                msql_proto::client_handshake(&handshake).unwrap().1
            );
            w.set_seq(seq + 1);
        }

        let ok = |w: &mut msql_proto::PacketWriter<_>| {
            w.write_all(&[0x00]).unwrap(); // OK packet type
            w.write_all(&[0x00]).unwrap(); // 0 rows (with lenenc encoding)
            w.write_all(&[0x00]).unwrap(); // no inserted rows
            w.write_all(&[0x00, 0x00]).unwrap(); // no server status
            w.write_all(&[0x00, 0x00]).unwrap(); // no warnings
            w.flush().unwrap();
        };
        ok(&mut w);

        println!("ready for client requests");
        while let Ok(Some((seq, packet))) = r.next() {
            w.set_seq(seq + 1);
            for b in &*packet {
                print!("0x{:02X} ", b);
            }
            println!("");
            match msql_proto::command(&packet) {
                Ok((_, msql_proto::Command::Query(q))) => {
                    if q.starts_with(b"SELECT @@") {
                        let var = &q[b"SELECT @@".len()..];
                        println!("query var {}", ::std::str::from_utf8(var).unwrap());
                        match var {
                            b"max_allowed_packet" => {
                                use std::iter;
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
                            }
                            _ => ok(&mut w),
                        }
                    } else {
                        println!("query {}", ::std::str::from_utf8(q).unwrap());
                        ok(&mut w);
                    }
                }
                Ok((_, msql_proto::Command::Init(db))) => {
                    println!("use {:?}", ::std::str::from_utf8(db));
                    ok(&mut w);
                }
                Ok((_, msql_proto::Command::Ping)) => {
                    println!("ping");
                    ok(&mut w);
                }
                Ok((_, msql_proto::Command::Quit)) => {
                    println!("quit");
                    ok(&mut w);
                }
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
            }
        }
    }
}
