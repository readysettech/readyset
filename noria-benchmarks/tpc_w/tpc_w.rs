mod parameters;
mod populate;

#[macro_use]
extern crate clap;

use crate::parameters::SampleKeys;
use noria::{Builder, LocalAuthority, SyncHandle};
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::{thread, time};

pub struct Backend {
    r: String,
    g: SyncHandle<LocalAuthority>,
    parallel_prepop: bool,
    prepop_counts: HashMap<String, usize>,
    barrier: Arc<Barrier>,
}

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_fsec {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64 / NANOS_PER_SEC as f64
    }};
}

fn get_queries(recipe_location: &str, random: bool) -> Vec<String> {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open(recipe_location).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    let mut queries = s
        .lines()
        .filter(|l| {
            !l.is_empty() && !l.starts_with('#') && !l.starts_with("--") && !l.starts_with("CREATE")
        })
        .map(String::from)
        .collect::<Vec<_>>();

    if random {
        let mut rng = rand::thread_rng();
        rng.shuffle(queries.as_mut_slice());
    }

    queries
}

fn make(
    recipe_location: &str,
    parallel: bool,
    single_query: bool,
    disable_partial: bool,
) -> Backend {
    use std::fs::File;
    use std::io::Read;

    // set up graph
    let mut b = Builder::default();

    let main_log = noria::logger_pls();
    b.log_with(main_log);
    if disable_partial {
        b.disable_partial();
    }

    let mut g = b.start_simple().unwrap();

    let recipe = {
        let mut f = File::open(recipe_location).unwrap();
        let mut s = String::new();

        // load queries
        f.read_to_string(&mut s).unwrap();
        if single_query {
            s = s
                .lines()
                .take_while(|l| l.starts_with("CREATE"))
                .collect::<Vec<_>>()
                .join("\n");
        }

        s
    };

    g.install_recipe(&recipe).unwrap();

    // XXX(malte): fix reuse configuration passthrough
    /*match Recipe::from_str(&s, Some(recipe_log.clone())) {
        Ok(mut recipe) => {
            match reuse.as_ref() {
                "finkelstein" => recipe.enable_reuse(ReuseConfigType::Finkelstein),
                "full" => recipe.enable_reuse(ReuseConfigType::Full),
                "noreuse" => recipe.enable_reuse(ReuseConfigType::NoReuse),
                "relaxed" => recipe.enable_reuse(ReuseConfigType::Relaxed),
                _ => panic!("reuse configuration not supported"),
            }
            recipe.activate(mig, transactions).unwrap();
            recipe
        }
        Err(e) => panic!(e),
    }*/

    // println!("{}", g);

    Backend {
        r: recipe,
        g: g,
        parallel_prepop: parallel,
        prepop_counts: HashMap::new(),
        barrier: Arc::new(Barrier::new(9)), // N.B.: # base tables
    }
}

impl Backend {
    fn extend(mut self, query: &str) -> Backend {
        let query_name = query.split(":").next().unwrap();

        let mut new_recipe = self.r.clone();
        new_recipe.push_str("\n");
        new_recipe.push_str(query);

        let start = time::Instant::now();
        self.g.install_recipe(&new_recipe).unwrap();

        let dur = dur_to_fsec!(start.elapsed());
        println!("Migrate query {}: ({:.2} sec)", query_name, dur,);

        self.r = new_recipe;
        self
    }

    #[allow(dead_code)]
    fn size(&mut self, _query_name: &str) -> usize {
        // XXX(malte): fix -- needs len RPC
        unimplemented!();
        /*match self.outputs.get(query_name) {
            None => panic!("no node for {}!", query_name),
            Some(nd) => {
                let g = self.g.view(*nd).unwrap();
                g.len()
            }
        }*/
    }

    fn read(
        &mut self,
        keys: &mut SampleKeys,
        query_name: &str,
        read_scale: f32,
        parallel: bool,
    ) -> Option<JoinHandle<()>> {
        println!("reading {}", query_name);
        let mut g = self
            .g
            .view(query_name)
            .expect(&format!("no node for {}!", query_name))
            .into_sync();
        let query_name = String::from(query_name);

        let num = ((keys.keys_size(&query_name) as f32) * read_scale) as usize;
        let params = keys.generate_parameter(&query_name, num);

        let mut read_view = move || {
            let mut ok = 0;

            let start = time::Instant::now();
            for i in 0..num {
                match g.lookup(&params[i..(i + 1)], true) {
                    Err(_) => continue,
                    Ok(datas) => {
                        if datas.len() > 0 {
                            ok += 1;
                        }
                    }
                }
            }
            let dur = dur_to_fsec!(start.elapsed());
            println!(
                "{}: ({:.2} GETs/sec) (ok: {})!",
                query_name,
                f64::from(num as i32) / dur,
                ok
            );
        };

        if parallel {
            Some(thread::spawn(move || {
                read_view();
            }))
        } else {
            read_view();
            None
        }
    }
}

fn main() {
    use crate::populate::*;
    use clap::{App, Arg};

    let matches = App::new("tpc_w")
        .version("0.1")
        .about("Soup TPC-W driver.")
        .arg(
            Arg::with_name("recipe")
                .short("r")
                .required(true)
                .default_value("tests/tpc-w-queries.txt")
                .help("Location of the TPC-W recipe file."),
        )
        .arg(
            Arg::with_name("populate_from")
                .short("p")
                .required(true)
                .default_value("benchmarks/tpc_w/data")
                .help("Location of the data files for TPC-W prepopulation."),
        )
        .arg(
            Arg::with_name("parallel_prepopulation")
                .long("parallel-prepopulation")
                .help("Prepopulate using parallel threads."),
        )
        .arg(
            Arg::with_name("transactional")
                .short("t")
                .help("Use transactional writes."),
        )
        .arg(
            Arg::with_name("single_query_migration")
                .long("single-query-migration")
                .short("s")
                .help("Add queries one by one, instead of in a batch."),
        )
        .arg(
            Arg::with_name("gloc")
                .short("g")
                .value_name("DIR")
                .help("Directory to store graphs generated by benchmark"),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("finkelstein")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("disable_partial")
                .long("disable_partial")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("read")
                .long("read")
                .default_value("0.00")
                .help("Reads % of keys for each query"),
        )
        .arg(
            Arg::with_name("write_to")
                .long("write_to")
                .possible_values(&["item", "author", "order_line"])
                .default_value("item")
                .help("Base table to write to"),
        )
        .arg(
            Arg::with_name("write")
                .long("write")
                .short("w")
                .default_value("1.00")
                .help("Writes % before reads and (1-%) after reads"),
        )
        .arg(
            Arg::with_name("random")
                .long("random")
                .help("Adds queries in random order")
                .requires("single_query_migration"),
        )
        .arg(
            Arg::with_name("parallel_read")
                .long("parallel_read")
                .help("Reads using parallel threads"),
        )
        .get_matches();

    let rloc = matches.value_of("recipe").unwrap();
    let ploc = matches.value_of("populate_from").unwrap();
    let parallel_prepop = matches.is_present("parallel_prepopulation");
    let parallel_read = matches.is_present("parallel_read");
    let single_query = matches.is_present("single_query_migration");
    let gloc = matches.value_of("gloc");
    let disable_partial = matches.is_present("disable_partial");
    let read_scale = value_t_or_exit!(matches, "read", f32);
    let write_to = matches.value_of("write_to").unwrap();
    let write = value_t_or_exit!(matches, "write", f32);
    let random = matches.is_present("random");

    if read_scale > write {
        panic!("can't read scale must be less or equal than write scale");
    }

    println!("Loading TPC-W recipe from {}", rloc);
    let mut backend = make(&rloc, parallel_prepop, single_query, disable_partial);

    println!("Prepopulating from data files in {}", ploc);
    let (item_write, author_write, order_line_write) = match write_to.as_ref() {
        "item" => (write, 1.0, 1.0),
        "author" => (1.0, write, 1.0),
        "order_line" => (1.0, 1.0, write),
        _ => unreachable!(),
    };

    let num_addr = populate_addresses(&mut backend, &ploc);
    backend.prepop_counts.insert("addresses".into(), num_addr);
    let num_authors = populate_authors(&mut backend, &ploc, author_write, true);
    backend.prepop_counts.insert("authors".into(), num_authors);
    let num_countries = populate_countries(&mut backend, &ploc);
    backend
        .prepop_counts
        .insert("countries".into(), num_countries);
    let num_customers = populate_customers(&mut backend, &ploc);
    backend
        .prepop_counts
        .insert("customers".into(), num_customers);
    let num_items = populate_items(&mut backend, &ploc, item_write, true);
    backend.prepop_counts.insert("items".into(), num_items);
    let num_orders = populate_orders(&mut backend, &ploc);
    backend.prepop_counts.insert("orders".into(), num_orders);
    let num_cc_xacts = populate_cc_xacts(&mut backend, &ploc);
    backend
        .prepop_counts
        .insert("cc_xacts".into(), num_cc_xacts);
    let num_order_line = populate_order_line(&mut backend, &ploc, order_line_write, true);
    backend
        .prepop_counts
        .insert("order_line".into(), num_order_line);

    if parallel_prepop {
        backend.barrier.wait();
        backend.barrier.wait();
    }

    //println!("{}", backend.g);

    println!("Finished writing! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    if single_query {
        use std::fs::File;
        use std::io::Write;

        println!("Migrating individual queries...");
        let queries = get_queries(&rloc, random);

        for (i, q) in queries.iter().enumerate() {
            backend = backend.extend(&q);

            if gloc.is_some() {
                let graph_fname = format!("{}/tpcw_{}.gv", gloc.unwrap(), i);
                let mut gf = File::create(graph_fname).unwrap();
                assert!(write!(gf, "{}", backend.g.graphviz().unwrap()).is_ok());
            }
        }
    }

    if read_scale > 0.0 {
        println!("Reading...");
        let mut keys = SampleKeys::new(&ploc, item_write, order_line_write);
        let item_queries = [
            "getBestSellers",
            "getMostRecentOrderLines",
            "getBook",
            "doSubjectSearch",
            "getNewProducts",
            "getRelated1",
            "getCart",
            "verifyDBConsistencyItemId",
        ];
        let mut handles = Vec::new();
        for nq in item_queries.iter() {
            handles.push(backend.read(&mut keys, nq, read_scale, parallel_read));
        }

        for h in handles {
            match h {
                Some(jh) => jh.join().unwrap(),
                None => continue,
            }
        }

        /*println!("Checking size of leaf views...");
        for nq in backend.r.aliases() {
            let populated = backend.size(nq);
            let total = keys.key_space(nq);
            let ratio = (populated as f32) / (total as f32);

            println!(
                "{}: {} of {} keys populated ({})",
                nq,
                populated,
                total,
                ratio
            );
        }*/
    }

    match write_to.as_ref() {
        "item" => populate_items(&mut backend, &ploc, write, false),
        "author" => populate_authors(&mut backend, &ploc, write, false),
        "order_line" => populate_order_line(&mut backend, &ploc, write, false),
        _ => unreachable!(),
    };
}
