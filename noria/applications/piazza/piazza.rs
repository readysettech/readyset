#![allow(dead_code, unused_variables)]
use std::collections::HashMap;
use std::fs::File;
use std::time;

use noria::{Builder, Handle, ReuseConfigType};
use noria_data::DataType;

#[macro_use]
mod populate;

use crate::populate::Populate;

pub struct Backend {
    g: Handle,
}

#[derive(PartialEq)]
enum PopulateType {
    Before,
    After,
    NoPopulate,
}

impl Backend {
    pub async fn new(partial: bool, _shard: bool, reuse: &str) -> Backend {
        let mut cb = Builder::default();
        if !partial {
            cb.disable_partial();
        }

        match reuse {
            "finkelstein" => cb.set_reuse(Some(ReuseConfigType::Finkelstein)),
            "full" => cb.set_reuse(Some(ReuseConfigType::Full)),
            "noreuse" => cb.set_reuse(None),
            "relaxed" => cb.set_reuse(Some(ReuseConfigType::Relaxed)),
            _ => panic!("reuse configuration not supported"),
        }

        let g = cb.start_local().await.unwrap();

        Backend { g }
    }

    pub async fn populate(&mut self, name: &'static str, records: Vec<Vec<DataType>>) -> usize {
        let mut mutator = self.g.table(name).await.unwrap();

        let start = time::Instant::now();

        let i = records.len();
        mutator.perform_all(records).await.unwrap();

        let dur = start.elapsed().as_secs_f64();
        println!(
            "Inserted {} {} in {:.2}s ({:.2} PUTs/sec)!",
            i,
            name,
            dur,
            i as f64 / dur
        );

        i
    }

    async fn login(&mut self, user_context: HashMap<String, DataType>) -> Result<(), String> {
        self.g.create_universe(user_context.clone()).await.unwrap();

        Ok(())
    }

    async fn set_security_config(&mut self, config_file: &str) {
        use std::io::Read;
        let mut config = String::new();
        let mut cf = File::open(config_file).unwrap();
        cf.read_to_string(&mut config).unwrap();

        // Install recipe with policies
        self.g.set_security_config(config).await.unwrap();
    }

    async fn migrate(&mut self, schema_file: &str, query_file: Option<&str>) -> Result<(), String> {
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
                rs.push('\n');
                rs.push_str(&s);
            }
        }

        // Install recipe
        self.g.extend_recipe(rs.parse().unwrap()).await.unwrap();

        Ok(())
    }
}

fn make_user(id: i32) -> HashMap<String, DataType> {
    let mut user = HashMap::new();
    user.insert(String::from("id"), id.into());

    user
}

#[tokio::main]
async fn main() {
    use clap::{App, Arg};
    let args = App::new("piazza")
        .version("0.1")
        .about("Benchmarks Piazza-like application with security policies.")
        .arg(
            Arg::new("schema")
                .short('s')
                .required(true)
                .default_value("benchmarks/piazza/schema.sql")
                .help("Schema file for Piazza application"),
        )
        .arg(
            Arg::new("queries")
                .short('q')
                .required(true)
                .default_value("benchmarks/piazza/post-queries.sql")
                .help("Query file for Piazza application"),
        )
        .arg(
            Arg::new("policies")
                .long("policies")
                .required(true)
                .default_value("benchmarks/piazza/complex-policies.json")
                .help("Security policies file for Piazza application"),
        )
        .arg(
            Arg::new("graph")
                .short('g')
                .default_value("pgraph.gv")
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::new("info")
                .short('i')
                .takes_value(true)
                .help("Directory to dump runtime process info (doesn't work on OSX)"),
        )
        .arg(
            Arg::new("reuse")
                .long("reuse")
                .default_value("full")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(Arg::new("shard").long("shard").help("Enable sharding"))
        .arg(
            Arg::new("partial")
                .long("partial")
                .help("Enable partial materialization"),
        )
        .arg(
            Arg::new("populate")
                .long("populate")
                .default_value("nopopulate")
                .possible_values(&["after", "before", "nopopulate"])
                .help("Populate app with randomly generated data"),
        )
        .arg(
            Arg::new("nusers")
                .short('u')
                .default_value("1000")
                .help("Number of users in the db"),
        )
        .arg(Arg::new("nlogged").short('l').default_value("1000").help(
            "Number of logged users. Must be less or equal than the number of users in the db",
        ))
        .arg(
            Arg::new("nclasses")
                .short('c')
                .default_value("100")
                .help("Number of classes in the db"),
        )
        .arg(
            Arg::new("nposts")
                .short('p')
                .default_value("100000")
                .help("Number of posts in the db"),
        )
        .arg(
            Arg::new("private")
                .long("private")
                .default_value("0.0")
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
    // let nusers: i32 = args.value_of_t_or_exit("nusers");
    // let nlogged: i32 = args.value_of_t_or_exit("nlogged");
    // let nclasses: i32 = args.value_of_t_or_exit("nclasses");
    // let nposts: i32 = args.value_of_t_or_exit("nposts");
    let private: f32 = args.value_of_t_or_exit("private");
    let nusers = 5;
    let nlogged = 5;
    let nclasses = 1;
    let nposts = 100;

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
    let mut backend = Backend::new(partial, shard, reuse).await;
    // backend.migrate(sloc, None).await.unwrap();

    // backend.set_security_config(ploc).await;
    backend.migrate(sloc, Some(qloc)).await.unwrap();

    let populate = match populate {
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

    backend.populate("Role", roles).await;
    println!("Waiting for groups to be constructed...");
    tokio::time::sleep(time::Duration::from_millis(120 * (nclasses as u64))).await;

    backend.populate("User", users).await;
    backend.populate("Class", classes).await;

    backend.populate("Post", posts.clone()).await;
    println!("Waiting for posts to propagate...");
    tokio::time::sleep(time::Duration::from_millis((nposts / 10) as u64)).await;

    println!("Finished writing! Sleeping for 2 seconds...");
    tokio::time::sleep(time::Duration::from_secs(10)).await;
    let leaf = "posts".to_string();
    let mut getter = backend.g.view(&leaf).await.unwrap();

    let res = getter.lookup(&[0.into()], false).await.unwrap(); // query by cid
    println!("result: {:?}", res);
}
