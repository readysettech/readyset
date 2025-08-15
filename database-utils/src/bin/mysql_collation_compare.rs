use std::collections::BTreeMap;
use std::ops::Range;

use icu::collator::{Collator, CollatorBorrowed, options::*};
use icu::locale::locale;
use icu::properties::CodePointMapData;
use icu::properties::props::GeneralCategory;
use mysql_async::prelude::Queryable;

fn expr(b: u16, name: &str) -> String {
    let b = String::from(char::from_u32(b as _).unwrap());
    let b = b
        .as_bytes()
        .iter()
        .map(|x| format!("{x:02X}"))
        .collect::<String>();
    format!("_utf8mb4 x'{b}' collate {name}")
}

async fn range(
    r: Range<u16>,
    name: &str,
    conn: &mut mysql_async::Conn,
    collator: &CollatorBorrowed<'_>,
    my: &mut BTreeMap<Vec<u8>, Vec<u16>>,
    col: &mut BTreeMap<Vec<u8>, Vec<u16>>,
) {
    let props = CodePointMapData::<GeneralCategory>::new();

    for a in r {
        if props.get32(a as _) == GeneralCategory::Unassigned {
            println!("skip {a:X}");
            continue;
        }

        println!("get {a:X}");
        let push = |v: &mut Vec<u16>| v.push(a);

        let e = expr(a, name);
        let q = format!("select weight_string({e})");
        let w: Vec<Vec<u8>> = conn.query(&q).await.unwrap();
        my.entry(w[0].clone()).and_modify(push).or_insert(vec![a]);

        let b = String::from(char::from_u32(a as _).unwrap());
        let mut k = Vec::new();
        let Ok(()) = collator.write_sort_key_to(&b, &mut k);
        col.entry(k).and_modify(push).or_insert(vec![a]);
    }
}

#[tokio::main]
async fn main() {
    let name = "utf8mb4_0900_ai_ci";
    let locale = locale!("utf").into();
    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Primary);
    let collator = Collator::try_new(locale, options).unwrap();

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .user(Some("root"))
        .pass(Some("noria"))
        .prefer_socket(false);
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    let mut my: BTreeMap<Vec<u8>, Vec<u16>> = BTreeMap::new();
    let mut col: BTreeMap<Vec<u8>, Vec<u16>> = BTreeMap::new();

    let conn = &mut conn;
    let my = &mut my;
    let col = &mut col;

    range(0..0x300, name, conn, &collator, my, col).await; // latin, up to combining diacritics
    range(0x4e00..0xa000, name, conn, &collator, my, col).await; // cjk unified ideographs
    range(0xac00..0xd7af, name, conn, &collator, my, col).await; // hangul syllables

    for ((mw, m), (cw, c)) in my.iter().zip(col.iter()) {
        println!("mysql {mw:02X?} - {m:04X?} - coll {cw:02X?} - {c:04X?}");
        if m != c {
            eprintln!("diff - mysql {m:04X?} - coll {c:04X?}");
        }
    }
}
