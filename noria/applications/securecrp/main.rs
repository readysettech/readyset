#![warn(clippy::dbg_macro)]
mod test_populate;

use noria::{Builder, Handle, ReuseConfigType};
use noria_data::DataType;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io::Write;
use std::time;

pub struct Backend {
    g: Handle,
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
        self.g.install_recipe(&rs).await.unwrap();

        Ok(())
    }
}

fn make_user(name: &str) -> HashMap<String, DataType> {
    let mut user = HashMap::new();
    user.insert(String::from("id"), DataType::try_from(name).unwrap());

    user
}

#[tokio::main]
async fn main() {
    use clap::{App, Arg};
    let args = App::new("SecureCRP")
        .version("0.1")
        .about("Benchmarks HotCRP-like application with security policies.")
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("benchmarks/securecrp/jeeves_schema.sql")
                .help("SQL schema file"),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("benchmarks/securecrp/jeeves_queries.sql")
                .help("SQL query file"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("benchmarks/securecrp/jeeves_policies.json")
                .help("Security policies file"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("graph.gv")
                .help("File to dump graph"),
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
                .help("Populate app with randomly generated data"),
        )
        .arg(Arg::with_name("user").long("user").default_value("malte"))
        .get_matches();

    println!("Starting SecureCRP...");

    // Read arguments
    let sloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let ploc = args.value_of("policies").unwrap();
    let gloc = args.value_of("graph");
    let partial = args.is_present("partial");
    let shard = args.is_present("shard");
    let reuse = args.value_of("reuse").unwrap();
    let user = args.value_of("user").unwrap();

    let mut backend = Backend::new(partial, shard, reuse).await;
    backend.migrate(sloc, None).await.unwrap();
    backend.set_security_config(ploc).await;
    backend.migrate(sloc, Some(qloc)).await.unwrap();

    if args.is_present("populate") {
        test_populate::create_users(&mut backend).await;
    }

    tokio::time::sleep(time::Duration::from_secs(2)).await;
    let _ = backend.login(make_user(user)).await.is_ok();

    if args.is_present("populate") {
        test_populate::create_papers(&mut backend).await;
        test_populate::dump_papers(&mut backend, user).await;
    }

    test_populate::dump_all_papers(&mut backend).await;

    if let Some(graph_fname) = gloc {
        let mut gf = File::create(graph_fname).unwrap();
        assert!(write!(gf, "{}", backend.g.graphviz().await.unwrap()).is_ok());
    }

    // sleep "forever"
    tokio::time::sleep(time::Duration::from_secs(200_000)).await;
}
