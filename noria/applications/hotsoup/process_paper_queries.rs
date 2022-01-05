#[macro_use]
extern crate clap;
extern crate nom_sql;
extern crate regex;

extern crate noria;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn traverse(path: &Path, start: usize, stop: usize) -> HashMap<usize, PathBuf> {
    use std::fs;
    use std::str::FromStr;

    let mut files = HashMap::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            let fname = path.file_name().unwrap().to_str().unwrap();
            if fname.starts_with("hotcrp_v") {
                let sv = usize::from_str(
                    &fname[fname.find("_").unwrap() + 2..fname.rfind("_").unwrap()],
                )
                .unwrap();
                if sv >= start && sv <= stop {
                    files.insert(sv, path.clone());
                }
            }
        }
    }
    files
}

fn process_file(fp: &Path) -> Vec<String> {
    use regex::Regex;
    use std::fs::File;
    use std::io::Read;

    let e_re = "\\s+ [0-9]+ .+\t(.*)+";
    let q_re = "\\s+ [0-9]+ Query\t(.*)+";
    let entry_regex = Regex::new(e_re).unwrap();
    let query_regex = Regex::new(q_re).unwrap();

    let mut f = File::open(fp).unwrap();
    let mut s = String::new();

    f.read_to_string(&mut s).unwrap();

    let mut queries = Vec::new();
    let mut start = false;
    let mut capturing = false;
    let mut buffer = String::new();
    for l in s.lines() {
        if l.contains("### CHAIR PAPER LIST") {
            start = true;
        }

        if !start {
            continue;
        }

        let flush = |buf: &mut String, queries: &mut Vec<String>| {
            if !buf.ends_with(";") {
                buf.push_str(";");
            }
            queries.push(buf.clone());
            buf.clear();
        };

        if query_regex.is_match(&l) {
            if !buffer.is_empty() {
                flush(&mut buffer, &mut queries);
            }
            for cap in query_regex.captures_iter(&l) {
                let qstr = &cap[1];
                buffer.push_str(qstr);
                buffer.push_str(" ");
                capturing = true;
            }
        } else if entry_regex.is_match(&l) && capturing {
            if !buffer.is_empty() {
                flush(&mut buffer, &mut queries);
            }
            capturing = false;
        } else if capturing {
            buffer.push_str(&l);
            buffer.push_str(" ");
        }
    }
    queries
}

fn main() {
    use clap::{App, Arg};

    let matches = App::new("process_paper_queries")
        .version("0.1")
        .about("Process extracted HotCRP paper queries.")
        .arg(
            Arg::with_name("source")
                .index(1)
                .help("Location of the HotCRP paper queries (in MySQL query log form).")
                .required(true),
        )
        .arg(
            Arg::with_name("start_at")
                .default_value("10")
                .long("start_at")
                .help("Schema version to start at; versions prior to this will be skipped."),
        )
        .arg(
            Arg::with_name("stop_at")
                .default_value("163")
                .long("stop_at")
                .help("Schema version to stop at; versions after this will be skipped."),
        )
        .get_matches();

    let path = matches.value_of("source").unwrap();
    let start_at_schema: usize = matches.value_of_t_or_exit("start_at");
    let stop_at_schema: uszie = matches.value_of_t_or_exit("stop_at");

    let files = traverse(Path::new(path), start_at_schema, stop_at_schema);

    for sv in start_at_schema..stop_at_schema + 1 {
        let e = files.get(&sv);
        let fname = match e {
            None => {
                warn!(log, "skipping non-existent schema version {}", sv);
                continue;
            }
            Some(f) => f,
        };

        info!(log, "Processing {:?}", fname);
        let queries = process_file(fname.as_path());

        for q in queries {
            match nom_sql::parse_query(&q) {
                Ok(_) => info!(log, "OK: {}", q),
                Err(e) => error!(log, "FAIL: {}: {:?}", q, e),
            }
        }
    }
}
