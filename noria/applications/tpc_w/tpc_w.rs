mod parameters;
mod populate;

use crate::parameters::SampleKeys;
use clap::value_t_or_exit;
use futures_util::stream::StreamExt;
use noria::{Builder, Handle, LocalAuthority};
use rand::prelude::*;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Barrier};
use std::time;

pub struct Backend {
    r: String,
    g: Handle<LocalAuthority>,
    parallel_prepop: bool,
    prepop_counts: HashMap<String, usize>,
    barrier: Arc<Barrier>,
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
        queries.as_mut_slice().shuffle(&mut rng);
    }

    queries
}

async fn make(
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

    let mut g = b.start_local().await.unwrap();

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

    g.install_recipe(&recipe).await.unwrap();

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
        g,
        parallel_prepop: parallel,
        prepop_counts: HashMap::new(),
        barrier: Arc::new(Barrier::new(9)), // N.B.: # base tables
    }
}

impl Backend {
    async fn extend(&mut self, query: &str) {
        let query_name = query.split(':').next().unwrap();

        let mut new_recipe = self.r.clone();
        new_recipe.push('\n');
        new_recipe.push_str(query);

        let start = time::Instant::now();
        self.g.install_recipe(&new_recipe).await.unwrap();

        let dur = start.elapsed().as_secs_f64();
        println!("Migrate query {}: ({:.2} sec)", query_name, dur,);

        self.r = new_recipe;
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

    async fn read(
        &mut self,
        keys: &mut SampleKeys,
        query_name: &str,
        read_scale: f32,
    ) -> impl Future<Output = ()> {
        println!("reading {}", query_name);
        let mut g = self
            .g
            .view(query_name)
            .await
            .unwrap_or_else(|e| panic!("no node for {}: {:?}", query_name, e));
        let query_name = String::from(query_name);

        let num = ((keys.keys_size(&query_name) as f32) * read_scale) as usize;
        let params = keys.generate_parameter(&query_name, num);

        async move {
            let mut ok = 0usize;

            let start = time::Instant::now();
            for i in 0..num {
                match g.lookup(&params[i..=i], true).await {
                    Err(_) => continue,
                    Ok(datas) => {
                        if !datas.is_empty() {
                            ok += 1;
                        }
                    }
                }
            }
            let dur = start.elapsed().as_secs_f64();
            println!(
                "{}: ({:.2} GETs/sec) (ok: {})!",
                query_name,
                f64::from(num as i32) / dur,
                ok
            );
        }
    }
}

#[tokio::main]
async fn main() {
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
    let mut backend = make(&rloc, parallel_prepop, single_query, disable_partial).await;

    println!("Prepopulating from data files in {}", ploc);
    let (item_write, author_write, order_line_write) = match write_to {
        "item" => (write, 1.0, 1.0),
        "author" => (1.0, write, 1.0),
        "order_line" => (1.0, 1.0, write),
        _ => unreachable!(),
    };

    let num_addr = populate_addresses(&mut backend, &ploc).await;
    backend.prepop_counts.insert("addresses".into(), num_addr);
    let num_authors = populate_authors(&mut backend, &ploc, author_write, true).await;
    backend.prepop_counts.insert("authors".into(), num_authors);
    let num_countries = populate_countries(&mut backend, &ploc).await.unwrap();
    backend
        .prepop_counts
        .insert("countries".into(), num_countries);
    let num_customers = populate_customers(&mut backend, &ploc).await.unwrap();
    backend
        .prepop_counts
        .insert("customers".into(), num_customers);
    let num_items = populate_items(&mut backend, &ploc, item_write, true)
        .await
        .unwrap();
    backend.prepop_counts.insert("items".into(), num_items);
    let num_orders = populate_orders(&mut backend, &ploc).await.unwrap();
    backend.prepop_counts.insert("orders".into(), num_orders);
    let num_cc_xacts = populate_cc_xacts(&mut backend, &ploc).await.unwrap();
    backend
        .prepop_counts
        .insert("cc_xacts".into(), num_cc_xacts);
    let num_order_line = populate_order_line(&mut backend, &ploc, order_line_write, true)
        .await
        .unwrap();
    backend
        .prepop_counts
        .insert("order_line".into(), num_order_line);

    if parallel_prepop {
        backend.barrier.wait();
    }

    //println!("{}", backend.g);

    println!("Finished writing! Sleeping for 1 second...");
    tokio::time::sleep(time::Duration::from_secs(1)).await;

    if single_query {
        use std::fs::File;
        use std::io::Write;

        println!("Migrating individual queries...");
        let queries = get_queries(&rloc, random);

        for (i, q) in queries.iter().enumerate() {
            backend.extend(&q).await;

            if gloc.is_some() {
                let graph_fname = format!("{}/tpcw_{}.gv", gloc.unwrap(), i);
                let mut gf = File::create(graph_fname).unwrap();
                write!(gf, "{}", backend.g.graphviz().await.unwrap()).unwrap();
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

        if parallel_read {
            let mut wait = futures_util::stream::futures_unordered::FuturesUnordered::new();
            for nq in item_queries.iter() {
                wait.push(backend.read(&mut keys, nq, read_scale).await);
            }
            while wait.next().await.is_some() {}
        } else {
            for nq in item_queries.iter() {
                backend.read(&mut keys, nq, read_scale).await.await;
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

    match write_to {
        "item" => populate_items(&mut backend, &ploc, write, false)
            .await
            .unwrap(),
        "author" => populate_authors(&mut backend, &ploc, write, false).await,
        "order_line" => populate_order_line(&mut backend, &ploc, write, false)
            .await
            .unwrap(),
        _ => unreachable!(),
    };

    backend.g.shutdown();
    backend.g.wait_done().await;
}
