#![feature(duration_float)]

mod populate;

#[macro_use]
extern crate clap;
#[macro_use]
extern crate slog;

use noria::{Builder, LocalAuthority, SyncHandle};

pub struct Backend {
    blacklist: Vec<String>,
    r: String,
    log: slog::Logger,
    g: SyncHandle<LocalAuthority>,
}

fn make(blacklist: &str, sharding: bool, partial: bool) -> Box<Backend> {
    use std::fs::File;
    use std::io::Read;

    // load query blacklist
    let mut bf = File::open(blacklist).unwrap();
    let mut s = String::new();
    bf.read_to_string(&mut s).unwrap();
    let blacklisted_queries = s
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| String::from(l.split(':').next().unwrap()))
        .map(|l| String::from(l.split('_').nth(1).unwrap()))
        .collect();

    // set up graph
    let mut b = Builder::default();
    let log = noria::logger_pls();
    let blender_log = log.clone();
    b.log_with(blender_log);
    if !sharding {
        b.set_sharding(None);
    }
    if !partial {
        b.disable_partial();
    }
    let g = b.start_simple().unwrap();

    //recipe.enable_reuse(reuse);
    Box::new(Backend {
        blacklist: blacklisted_queries,
        r: String::new(),
        log,
        g,
    })
}

impl Backend {
    fn migrate(&mut self, schema_file: &str, query_file: Option<&str>) -> Result<(), String> {
        use std::fs::File;
        use std::io::Read;

        let blacklist = &self.blacklist;

        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();

        let mut blacklisted = 0;

        // load schema
        sf.read_to_string(&mut s).unwrap();
        // HotCRP schema files have some DROP TABLE and DELETE queries, so skip those
        let mut rs = s
            .lines()
            .filter(|l| !l.starts_with("DROP") && !l.starts_with("delete"))
            .take_while(|l| !l.contains("insert"))
            .collect::<Vec<_>>()
            .join("\n");
        // load queries and concatenate them onto the table definitions from the schema
        s.clear();

        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(
                    &s.lines()
                        .filter(|ref l| {
                            // make sure to skip blacklisted queries
                            for q in blacklist {
                                if l.contains(q) || l.contains("LIKE") || l.contains("like") {
                                    blacklisted += 1;
                                    return false;
                                }
                            }
                            true
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            }
        }

        info!(self.log, "Ignored {} blacklisted queries", blacklisted);

        match self.g.install_recipe(&rs) {
            Ok(ar) => {
                info!(self.log, "{} expressions added", ar.expressions_added);
                info!(self.log, "{} expressions removed", ar.expressions_removed);
            }
            Err(e) => return Err(format!("failed to activate recipe: {}", e)),
        }

        self.r = rs;
        Ok(())
    }
}

fn main() {
    use clap::{App, Arg};
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;

    let matches = App::new("hotsoup")
        .version("0.1")
        .about("Soupy conference management system for your HotCRP needs.")
        .arg(
            Arg::with_name("graphs")
                .short("g")
                .value_name("DIR")
                .help("Directory to dump graphs for each schema version into (if set)."),
        )
        .arg(
            Arg::with_name("populate_from")
                .short("p")
                .required(true)
                .default_value("benchmarks/hotsoup/testdata")
                .help("Location of the HotCRP test data for population."),
        )
        .arg(
            Arg::with_name("schemas")
                .short("s")
                .required(true)
                .default_value("benchmarks/hotsoup/schemas")
                .help("Location of the HotCRP query recipes to move through."),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("benchmarks/hotsoup/queries")
                .help("Location of the HotCRP schema recipes to move through."),
        )
        .arg(
            Arg::with_name("blacklist")
                .short("b")
                .default_value("benchmarks/hotsoup/query_blacklist.txt")
                .help("File with blacklisted queries to skip."),
        )
        .arg(
            Arg::with_name("no-partial")
                .long("no-partial")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("no-sharding")
                .long("no-sharding")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("populate_at")
                .default_value("11")
                .long("populate_at")
                .help("Schema version to populate database at; must be compatible with test data."),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("finkelstein")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm to use."),
        )
        .arg(
            Arg::with_name("start_at")
                .default_value("1")
                .long("start_at")
                .help("Schema version to start at; versions prior to this will be skipped."),
        )
        .arg(
            Arg::with_name("stop_at")
                .default_value("161")
                .long("stop_at")
                .help("Schema version to stop at; versions after this will be skipped."),
        )
        .arg(
            Arg::with_name("base_only")
                .long("base_only")
                .help("Only add base tables, not queries."),
        )
        .arg(
            Arg::with_name("transactional")
                .short("t")
                .help("Use transactional writes."),
        )
        .get_matches();

    let blloc = matches.value_of("blacklist").unwrap();
    let gloc = matches.value_of("graphs");
    let sloc = matches.value_of("schemas").unwrap();
    let qloc = matches.value_of("queries").unwrap();
    let dataloc = matches.value_of("populate_from").unwrap();
    let transactional = matches.is_present("transactional");
    let base_only = matches.is_present("base_only");
    /*let reuse = match matches.value_of("reuse").unwrap() {
        "finkelstein" => ReuseConfigType::Finkelstein,
        "full" => ReuseConfigType::Full,
        "noreuse" => ReuseConfigType::NoReuse,
        "relaxed" => ReuseConfigType::Relaxed,
        _ => panic!("reuse configuration not supported"),
    };*/
    let disable_sharding = matches.is_present("no-sharding");
    let disable_partial = matches.is_present("no-partial");
    let start_at_schema = value_t_or_exit!(matches, "start_at", u64);
    let stop_at_schema = value_t_or_exit!(matches, "stop_at", u64);
    let populate_at_schema = value_t_or_exit!(matches, "populate_at", u64);

    let mut backend = make(blloc, !disable_sharding, !disable_partial);

    let mut query_files = Vec::new();
    let mut schema_files = Vec::new();

    for entry in fs::read_dir(qloc).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            query_files.push(entry.path());
        }
    }

    for entry in fs::read_dir(sloc).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            schema_files.push(entry.path());
        }
    }

    // hotcrp_*.sql
    let mut query_files = query_files
        .into_iter()
        .map(|k| {
            let fname = String::from(k.file_name().unwrap().to_str().unwrap());
            let ver = u64::from_str(&fname[7..fname.len() - 4]).unwrap();
            (ver, k)
        })
        .collect::<Vec<(u64, PathBuf)>>();
    let mut schema_files = schema_files
        .into_iter()
        .map(|k| {
            let fname = String::from(k.file_name().unwrap().to_str().unwrap());
            (u64::from_str(&fname[7..fname.len() - 4]).unwrap(), k)
        })
        .collect::<Vec<(u64, PathBuf)>>();
    query_files.sort_by_key(|t| t.0);
    schema_files.sort_by_key(|t| t.0);

    let files: Vec<((u64, PathBuf), (u64, PathBuf))> =
        schema_files.into_iter().zip(query_files).collect();

    for (sf, qf) in files {
        assert_eq!(sf.0, qf.0);
        let schema_version = sf.0;
        if schema_version < start_at_schema || schema_version > stop_at_schema {
            info!(backend.log, "Skipping schema {:?}", sf.1);
            continue;
        }

        info!(
            backend.log,
            "Loading HotCRP schema from {:?}, queries from {:?}", sf.1, qf.1
        );

        let queries = if base_only {
            None
        } else {
            Some(qf.1.to_str().unwrap())
        };
        if let Err(e) = backend.migrate(&sf.1.to_str().unwrap(), queries) {
            let graph_fname = format!("{}/failed_hotcrp_{}.gv", gloc.unwrap(), schema_version);
            let mut gf = File::create(graph_fname).unwrap();
            assert!(write!(gf, "{}", backend.g.graphviz().unwrap()).is_ok());
            panic!(e)
        }

        if gloc.is_some() {
            let graph_fname = format!("{}/hotcrp_{}.gv", gloc.unwrap(), schema_version);
            let mut gf = File::create(graph_fname).unwrap();
            assert!(write!(gf, "{}", backend.g.graphviz().unwrap()).is_ok());
        }

        // on the first auto-upgradeable schema, populate with test data
        if schema_version == populate_at_schema {
            info!(backend.log, "Populating database!");
            populate::populate(&mut backend, dataloc, transactional).unwrap();
        }
    }
}
