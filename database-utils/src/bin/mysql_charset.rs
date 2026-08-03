//! Generates single-byte charset conversion tables for `readyset-data` from a live MySQL
//! server. MySQL's own CONVERT() behavior is the specification, so the tables are byte-exact
//! with whatever the server answers, including its choice of replacement character for
//! undefined byte positions.
//!
//! Usage:
//!
//! cargo run -p database-utils --bin mysql_charset -- <charset>...
//! cargo run -p database-utils --bin mysql_charset -- --all
//!
//! Output files are written to readyset-data/src/encoding/<charset>.rs, so run from the
//! workspace root. Run cargo fmt afterwards.

use std::env::args;
use std::fmt::Write;

use mysql_async::prelude::Queryable;
use yore::CodePage;

/// yore code pages we can cross-check against, for charsets where an eponymous page exists.
/// Divergences are reported as comments in the generated file. MySQL wins.
fn yore_page(charset: &str) -> Option<(&'static str, &'static dyn CodePage)> {
    use yore::code_pages::*;
    match charset {
        "latin1" => Some(("CP1252", &CP1252)),
        "cp850" => Some(("CP850", &CP850)),
        "cp852" => Some(("CP852", &CP852)),
        "cp866" => Some(("CP866", &CP866)),
        "cp1250" => Some(("CP1250", &CP1250)),
        "cp1251" => Some(("CP1251", &CP1251)),
        "cp1256" => Some(("CP1256", &CP1256)),
        "cp1257" => Some(("CP1257", &CP1257)),
        _ => None,
    }
}

async fn generate(conn: &mut mysql_async::Conn, charset: &str, server_version: &str) {
    // Decode table: what MySQL converts each byte to in utf8mb4.
    let mut decode = Vec::with_capacity(256);
    for b in 0u8..=255 {
        let q = format!("SELECT CONVERT(_{charset} X'{b:02X}' USING utf8mb4)");
        let s: String = conn
            .query_first(&q)
            .await
            .expect("decode query failed")
            .expect("decode query returned no row");
        let mut chars = s.chars();
        let c = chars
            .next()
            .unwrap_or_else(|| panic!("{charset} byte {b:#04x} decoded to empty string"));
        assert!(
            chars.next().is_none(),
            "{charset} byte {b:#04x} decoded to multiple chars: {s:?}"
        );
        decode.push(c);
    }

    // Encode table: MySQL's canonical byte for each distinct char in the decode image. When a
    // char reverse-converts to the replacement byte 0x3F, only '?' itself is recorded; other
    // chars are omitted so encoding falls through to the lossy fallback.
    let mut image: Vec<char> = decode.clone();
    image.sort_unstable();
    image.dedup();
    let mut encode = Vec::with_capacity(image.len());
    for &c in &image {
        let mut utf8 = [0u8; 4];
        let utf8 = c.encode_utf8(&mut utf8).as_bytes();
        let hex: String = utf8.iter().map(|b| format!("{b:02X}")).collect();
        let q = format!("SELECT HEX(CONVERT(_utf8mb4 X'{hex}' USING {charset}))");
        let h: String = conn
            .query_first(&q)
            .await
            .expect("encode query failed")
            .expect("encode query returned no row");
        assert_eq!(
            h.len(),
            2,
            "{charset} char U+{:04X} encoded to {} bytes: {h:?}",
            c as u32,
            h.len() / 2
        );
        let b = u8::from_str_radix(&h, 16).expect("invalid hex from server");
        if b == b'?' && c != '?' {
            continue;
        }
        encode.push((c, b));
    }

    // ASCII transparency covers both directions. Every ASCII byte must decode to the
    // identical char, and every ASCII char must encode back to the identical byte.
    let ascii_transparent = (0u8..=127).all(|b| {
        decode[b as usize] == b as char
            && encode
                .iter()
                .find(|&&(c, _)| c == b as char)
                .map(|&(_, eb)| eb)
                == Some(b)
    });

    // Cross-check against yore where an eponymous code page exists. Informational only.
    let mut divergences = String::new();
    if let Some((page_name, page)) = yore_page(charset) {
        for b in 0u8..=255 {
            let y = page.decode_lossy(&[b]).chars().next().expect("yore decode");
            let m = decode[b as usize];
            if y != m {
                writeln!(
                    divergences,
                    "// yore {page_name} divergence at byte {b:#04x}: mysql U+{:04X}, yore {}",
                    m as u32,
                    if y == '\u{FFFD}' {
                        "undefined".to_string()
                    } else {
                        format!("U+{:04X}", y as u32)
                    }
                )
                .unwrap();
            }
        }
    }

    let mut out = String::new();
    out.push_str(
        "////////////////////////////////////////////////////////////////////////////////\n",
    );
    out.push_str("//\n");
    out.push_str("//             THIS FILE IS MACHINE-GENERATED!!!  DO NOT EDIT!!!\n");
    out.push_str("//\n");
    writeln!(
        out,
        "// Generated from MySQL server version {server_version}."
    )
    .unwrap();
    out.push_str("//\n");
    out.push_str("// To regenerate this file:\n");
    out.push_str("//\n");
    writeln!(
        out,
        "// cargo run -p database-utils --bin mysql_charset -- {charset}"
    )
    .unwrap();
    out.push_str("// cargo fmt\n");
    out.push_str("//\n");
    out.push_str(
        "////////////////////////////////////////////////////////////////////////////////\n\n",
    );
    if !divergences.is_empty() {
        out.push_str(&divergences);
        out.push('\n');
    }
    out.push_str("use super::SingleByteEncoding;\n\n");
    out.push_str("pub(crate) static SPEC: SingleByteEncoding = SingleByteEncoding {\n");
    writeln!(out, "    name: \"{charset}\",").unwrap();
    out.push_str("    decode: &DECODE,\n");
    out.push_str("    encode: &ENCODE,\n");
    writeln!(out, "    ascii_transparent: {ascii_transparent},").unwrap();
    out.push_str("};\n\n");
    out.push_str("static DECODE: [char; 256] = [\n");
    for c in &decode {
        writeln!(out, "    '\\u{{{:04x}}}',", *c as u32).unwrap();
    }
    out.push_str("];\n\n");
    writeln!(out, "static ENCODE: [(char, u8); {}] = [", encode.len()).unwrap();
    for (c, b) in &encode {
        writeln!(out, "    ('\\u{{{:04x}}}', {b:#04x}),", *c as u32).unwrap();
    }
    out.push_str("];\n");

    let path = format!("readyset-data/src/encoding/{charset}.rs");
    std::fs::write(&path, out).unwrap_or_else(|e| panic!("failed to write {path}: {e}"));
    println!("wrote {path}");
}

#[tokio::main]
async fn main() {
    let requested: Vec<String> = args().skip(1).collect();
    if requested.is_empty() {
        panic!("no charsets given; pass charset names or --all");
    }

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .user(Some("root"))
        .pass(Some("noria"))
        .prefer_socket(false);
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    let server_version: String = conn
        .query_first("SELECT VERSION()")
        .await
        .unwrap()
        .expect("no server version");

    let charsets: Vec<String> = if requested == ["--all"] {
        // binary is single-byte but handled as a dedicated Encoding variant, not as a
        // conversion table.
        conn.query(
            "SELECT CHARACTER_SET_NAME FROM information_schema.CHARACTER_SETS \
             WHERE MAXLEN = 1 AND CHARACTER_SET_NAME <> 'binary' \
             ORDER BY CHARACTER_SET_NAME",
        )
        .await
        .unwrap()
    } else {
        requested
    };

    for charset in &charsets {
        generate(&mut conn, charset, &server_version).await;
    }
}
