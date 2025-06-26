use std::env::args;

use mysql_async::prelude::Queryable;
use yore::code_pages::CP1252;

fn query(a: u8, collation: &str) -> String {
    format!("select weight_string(_latin1 X'{a:02X}' collate {collation})")
}

#[tokio::main]
async fn main() {
    let Some(collation) = args().nth(1) else {
        panic!("no collation!");
    };

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .user(Some("root"))
        .pass(Some("noria"))
        .prefer_socket(false);
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    // no attempt at formatting; use of cargo fmt is expected
    println!("////////////////////////////////////////////////////////////////////////////////");
    println!("//");
    println!("//             THIS FILE IS MACHINE-GENERATED!!!  DO NOT EDIT!!!");
    println!("//");
    println!("// To regenerate this file:");
    println!("//");
    println!("// cargo run -p database-utils --bin mysql_collation <collation> > \\");
    println!("//     readyset-data/src/collation/<collation>_weights.rs");
    println!("// cargo fmt");
    println!("//");
    println!("////////////////////////////////////////////////////////////////////////////////");
    println!();

    println!("pub(crate) const WEIGHTS: [(char, u8); 256] = [");
    for a in 0u8..=255 {
        let q = query(a, &collation);
        let w: Vec<u8> = conn.query_first(&q).await.unwrap().unwrap();
        let c = CP1252.decode(&[a]).chars().next().unwrap() as u32;
        let c = format!("\\u{{{c:04x}}}");
        println!("('{c}', {w}), ", w = w[0]);
    }
    println!("];");
}
