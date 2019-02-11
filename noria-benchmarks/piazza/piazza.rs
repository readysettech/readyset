#[macro_use]
extern crate clap;

use noria::{Builder, DataType, LocalAuthority, ReuseConfigType, SyncHandle};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::{thread, time};

#[macro_use]
mod populate;

use crate::populate::{Populate, NANOS_PER_SEC};

pub struct Backend {
    g: SyncHandle<LocalAuthority>,
}

#[derive(PartialEq)]
enum PopulateType {
    Before,
    After,
    NoPopulate,
}

impl Backend {
    pub fn new(partial: bool, _shard: bool, reuse: &str) -> Backend {
        let mut cb = Builder::default();
        let log = noria::logger_pls();
        let blender_log = log.clone();

        if !partial {
            cb.disable_partial();
        }

        match reuse.as_ref() {
            "finkelstein" => cb.set_reuse(ReuseConfigType::Finkelstein),
            "full" => cb.set_reuse(ReuseConfigType::Full),
            "noreuse" => cb.set_reuse(ReuseConfigType::NoReuse),
            "relaxed" => cb.set_reuse(ReuseConfigType::Relaxed),
            _ => panic!("reuse configuration not supported"),
        }

        cb.log_with(blender_log);

        let g = cb.start_simple().unwrap();

        Backend { g: g }
    }

    pub fn populate(&mut self, name: &'static str, mut records: Vec<Vec<DataType>>) -> usize {
        let mut mutator = self.g.table(name).unwrap().into_sync();

        let start = time::Instant::now();

        let i = records.len();
        for r in records.drain(..) {
            mutator.insert(r).unwrap();
        }

        let dur = dur_to_fsec!(start.elapsed());
        println!(
            "Inserted {} {} in {:.2}s ({:.2} PUTs/sec)!",
            i,
            name,
            dur,
            i as f64 / dur
        );

        i
    }

    fn login(&mut self, user_context: HashMap<String, DataType>) -> Result<(), String> {
        self.g
            .on_worker(|w| w.create_universe(user_context.clone()))
            .unwrap();

        Ok(())
    }

    fn set_security_config(&mut self, config_file: &str) {
        use std::io::Read;
        let mut config = String::new();
        let mut cf = File::open(config_file).unwrap();
        cf.read_to_string(&mut config).unwrap();

        // Install recipe with policies
        self.g.on_worker(|w| w.set_security_config(config)).unwrap();
    }

    fn migrate(&mut self, schema_file: &str, query_file: Option<&str>) -> Result<(), String> {
        use std::io::Read;

        // Read schema file
        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();
        sf.read_to_string(&mut s).unwrap();

        let mut rs = s.clone();
        s.clear();

        // Read query file
        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(&s);
            }
        }

        // Install recipe
        self.g.install_recipe(&rs).unwrap();

        Ok(())
    }
}

fn make_user(id: i32) -> HashMap<String, DataType> {
    let mut user = HashMap::new();
    user.insert(String::from("id"), id.into());

    user
}

fn main() {
    use clap::{App, Arg};
    let args = App::new("piazza")
        .version("0.1")
        .about("Benchmarks Piazza-like application with security policies.")
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("benchmarks/piazza/schema.sql")
                .help("Schema file for Piazza application"),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("benchmarks/piazza/post-queries.sql")
                .help("Query file for Piazza application"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("benchmarks/piazza/complex-policies.json")
                .help("Security policies file for Piazza application"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("pgraph.gv")
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("info")
                .short("i")
                .takes_value(true)
                .help("Directory to dump runtime process info (doesn't work on OSX)"),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("full")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("shard")
                .long("shard")
                .help("Enable sharding"),
        )
        .arg(
            Arg::with_name("partial")
                .long("partial")
                .help("Enable partial materialization"),
        )
        .arg(
            Arg::with_name("populate")
                .long("populate")
                .default_value("nopopulate")
                .possible_values(&["after", "before", "nopopulate"])
                .help("Populate app with randomly generated data"),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .default_value("1000")
                .help("Number of users in the db"),
        )
        .arg(
            Arg::with_name("nlogged")
                .short("l")
                .default_value("1000")
                .help(
                "Number of logged users. Must be less or equal than the number of users in the db",
            ),
        )
        .arg(
            Arg::with_name("nclasses")
                .short("c")
                .default_value("100")
                .help("Number of classes in the db"),
        )
        .arg(
            Arg::with_name("nposts")
                .short("p")
                .default_value("100000")
                .help("Number of posts in the db"),
        )
        .arg(
            Arg::with_name("private")
                .long("private")
                .default_value("0.1")
                .help("Percentage of private posts"),
        )
        .get_matches();

    println!("Starting benchmark...");

    // Read arguments
    let sloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let ploc = args.value_of("policies").unwrap();
    let gloc = args.value_of("graph");
    let iloc = args.value_of("info");
    let partial = args.is_present("partial");
    let shard = args.is_present("shard");
    let reuse = args.value_of("reuse").unwrap();
    let populate = args.value_of("populate").unwrap_or("nopopulate");
    let nusers = value_t_or_exit!(args, "nusers", i32);
    let nlogged = value_t_or_exit!(args, "nlogged", i32);
    let nclasses = value_t_or_exit!(args, "nclasses", i32);
    let nposts = value_t_or_exit!(args, "nposts", i32);
    let private = value_t_or_exit!(args, "private", f32);

    assert!(
        nlogged <= nusers,
        "nusers must be greater or equal to nlogged"
    );
    assert!(
        nusers >= populate::TAS_PER_CLASS as i32,
        "nusers must be greater or equal to TAS_PER_CLASS"
    );

    // Initiliaze backend application with some queries and policies
    println!("Initiliazing database schema...");
    let mut backend = Backend::new(partial, shard, reuse);
    backend.migrate(sloc, None).unwrap();

    backend.set_security_config(ploc);
    backend.migrate(sloc, Some(qloc)).unwrap();

    let populate = match populate.as_ref() {
        "before" => PopulateType::Before,
        "after" => PopulateType::After,
        _ => PopulateType::NoPopulate,
    };

    let mut p = Populate::new(nposts, nusers, nclasses, private);

    p.enroll_students();
    let roles = p.get_roles();
    let users = p.get_users();
    let posts = p.get_posts();
    let classes = p.get_classes();

    backend.populate("Role", roles);
    println!("Waiting for groups to be constructed...");
    thread::sleep(time::Duration::from_millis(120 * (nclasses as u64)));

    backend.populate("User", users);
    backend.populate("Class", classes);

    if populate == PopulateType::Before {
        backend.populate("Post", posts.clone());
        println!("Waiting for posts to propagate...");
        thread::sleep(time::Duration::from_millis((nposts / 10) as u64));
    }

    println!("Finished writing! Sleeping for 2 seconds...");
    thread::sleep(time::Duration::from_millis(2000));

    // if partial, read 25% of the keys
    if partial {
        let leaf = format!("post_count");
        let mut getter = backend.g.view(&leaf).unwrap().into_sync();
        for author in 0..nusers / 4 {
            getter.lookup(&[author.into()], false).unwrap();
        }
    }

    // Login a user
    println!("Login in users...");
    for i in 0..nlogged {
        let start = time::Instant::now();
        backend.login(make_user(i)).is_ok();
        let dur = dur_to_fsec!(start.elapsed());
        println!("Migration {} took {:.2}s!", i, dur,);

        // if partial, read 25% of the keys
        if partial {
            let leaf = format!("post_count_u{}", i);
            let mut getter = backend.g.view(&leaf).unwrap().into_sync();
            for author in 0..nusers / 4 {
                getter.lookup(&[author.into()], false).unwrap();
            }
        }

        if iloc.is_some() && i % 50 == 0 {
            use std::fs;
            let fname = format!("{}-{}", iloc.unwrap(), i);
            fs::copy("/proc/self/status", fname).unwrap();
        }
    }

    if populate == PopulateType::After {
        backend.populate("Post", posts);
    }

    if !partial {
        let mut dur = time::Duration::from_millis(0);
        for uid in 0..nlogged {
            let leaf = format!("post_count_u{}", uid);
            let mut getter = backend.g.view(&leaf).unwrap().into_sync();
            let start = time::Instant::now();
            for author in 0..nusers {
                getter.lookup(&[author.into()], true).unwrap();
            }
            dur += start.elapsed();
        }

        let dur = dur_to_fsec!(dur);

        println!(
            "Read {} keys in {:.2}s ({:.2} GETs/sec)!",
            nlogged * nusers,
            dur,
            (nlogged * nusers) as f64 / dur,
        );
    }

    println!("Done with benchmark.");

    if gloc.is_some() {
        let graph_fname = gloc.unwrap();
        let mut gf = File::create(graph_fname).unwrap();
        assert!(write!(gf, "{}", backend.g.graphviz().unwrap()).is_ok());
    }
}
