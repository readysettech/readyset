use mysql::prelude::Queryable;
use mysql::OptsBuilder;
use mysql::Params;
use serde::Deserialize;

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use std::io;
use std::panic;
use std::thread;
use std::time;

use noria_data::DataType;
use noria_server::Builder;

const DIRECTORY_PREFIX: &str = "tests/mysql_comparison_tests";

#[derive(Debug, Deserialize)]
enum Type {
    Int,
    Text,
    Real,
    Date,
    Timestamp,
}

impl Type {
    pub fn make_datatype(&self, value: &str) -> DataType {
        match *self {
            Type::Int => i64::from_str(value).unwrap().into(),
            Type::Text => value.try_into().unwrap(),
            Type::Real => f64::from_str(value).unwrap().try_into().unwrap(),
            Type::Date => value.try_into().unwrap(),
            Type::Timestamp => value.try_into().unwrap(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Table {
    create_query: String,
    types: Vec<Type>,
    data: Option<Vec<Vec<String>>>,
    data_file: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Query {
    select_query: String,
    types: Vec<Type>,
    values: Vec<Vec<String>>,
    ignore: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct Schema {
    name: String,
    tables: BTreeMap<String, Table>,
    queries: BTreeMap<String, Query>,
}

#[derive(Clone)]
struct PanicState {
    message: String,
    _thread: String,
    file: String,
    line: u32,
    backtrace: backtrace::Backtrace,
}

fn set_panic_hook(panic_state: Arc<Mutex<Option<PanicState>>>) {
    panic::set_hook(Box::new(move |info| {
        if panic_state.lock().unwrap().is_some() {
            return;
        }

        let backtrace = backtrace::Backtrace::new();

        let thread = thread::current();
        let thread = thread.name().unwrap_or("unnamed").to_owned();

        let message = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            },
        }
        .to_owned();

        let (file, line) = match info.location() {
            Some(l) => (l.file().to_owned(), l.line()),
            None => ("[unknown]".to_owned(), 0),
        };
        *panic_state.lock().unwrap() = Some(PanicState {
            message,
            _thread: thread,
            file,
            line,
            backtrace,
        });
    }));
}

fn read_file<P: AsRef<Path>>(file_name: P) -> String {
    let mut contents = String::new();
    let mut file = File::open(file_name).unwrap();
    file.read_to_string(&mut contents).unwrap();
    contents
}

fn write_file<P: AsRef<Path>>(file_name: P, contents: String) {
    let mut file = File::create(file_name).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
}

fn run_for_all_in_directory<F: FnMut(String, String)>(directory: &str, mut f: F) {
    let directory = Path::new(DIRECTORY_PREFIX).join(directory);
    for entry in fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        f(
            entry.file_name().to_str().unwrap().to_owned(),
            read_file(entry.path().to_str().unwrap()),
        );
    }
}

pub fn setup_mysql(addr: &str) -> mysql::Pool {
    use mysql::Opts;

    let addr = format!("mysql://{}", addr);
    let db = &addr[addr.rfind('/').unwrap() + 1..];
    let options = Opts::from_url(&addr[0..addr.rfind('/').unwrap()]).unwrap();

    // clear the db (note that we strip of /db so we get default)
    let opts = OptsBuilder::from_opts(options.clone())
        .db_name(Some(db))
        .init(vec!["SET max_heap_table_size = 4294967296;"]);
    let pool = mysql::Pool::new_manual(1, 4, opts).unwrap();
    let mut conn = pool.get_conn().unwrap();
    if conn.query_drop(format!("USE {}", db)).is_ok() {
        conn.query_drop(format!("DROP DATABASE {}", &db).as_str())
            .unwrap();
    }
    conn.query_drop(format!("CREATE DATABASE {}", &db).as_str())
        .unwrap();
    conn.query_drop(format!("USE {}", db)).unwrap();

    drop(conn);

    // now we connect for real
    let opts = OptsBuilder::from_opts(options)
        .db_name(Some(db))
        .init(vec!["SET max_heap_table_size = 4294967296;"]);
    mysql::Pool::new_manual(1, 4, opts).unwrap()
}

fn generate_target_results(schemas: &BTreeMap<String, Schema>) {
    for (schema_name, schema) in schemas.iter() {
        let pool = setup_mysql("soup@127.0.0.1:3306/mysql_comparison_test");
        for (table_name, table) in schema.tables.iter() {
            let mut conn = pool.get_conn().unwrap();
            conn.exec_drop(&table.create_query, ()).unwrap();
            let query = format!(
                "INSERT INTO {} VALUES ({})",
                table_name,
                vec!["?"; table.types.len()].join(", ")
            );
            let insert = conn.prep(query).unwrap();
            for row in table.data.as_ref().unwrap().iter() {
                if let Err(msg) = conn.exec_drop(&insert, row.clone()) {
                    println!(
                        "MySQL insert query failed for table: {}, values: {:?}",
                        table_name, row
                    );
                    println!("{:?}", msg);
                    panic!();
                }
            }
        }

        let mut target_data: BTreeMap<String, BTreeMap<String, Vec<Vec<String>>>> = BTreeMap::new();
        for (query_name, query) in schema.queries.iter() {
            if query.values.is_empty() {
                continue;
            }

            target_data.insert(query_name.clone(), BTreeMap::new());
            for (i, values) in query.values.iter().enumerate() {
                target_data
                    .get_mut(query_name)
                    .unwrap()
                    .insert(i.to_string(), Vec::new());

                let values = Params::Positional(values.iter().map(|v| v.into()).collect());
                for row in pool
                    .get_conn()
                    .unwrap()
                    .exec::<mysql::Row, _, _>(&query.select_query, values)
                    .unwrap()
                {
                    let row = row
                        .unwrap()
                        .into_iter()
                        .map(|v| {
                            format!("{:?}", v)
                                .trim_matches(|c| c == '\'' || c == '"')
                                .to_owned()
                        })
                        .collect();
                    target_data
                        .get_mut(query_name)
                        .unwrap()
                        .get_mut(&i.to_string())
                        .unwrap()
                        .push(row);
                }
            }
        }
        let target_data_toml = toml::to_string(&target_data).unwrap();
        let target_data_file = Path::new(DIRECTORY_PREFIX)
            .join("targets")
            .join(schema_name);
        write_file(target_data_file, target_data_toml);
    }
}

/// Compare two sets of results, returning none if they are the same, and the diff between them
/// otherwise.
fn compare_results(mysql: &[Vec<String>], soup: &[Vec<String>]) -> Option<String> {
    let mut mysql = mysql.to_vec();
    let mut soup = soup.to_vec();
    mysql.sort();
    soup.sort();

    // TODO: Remove hack to drop key column from Soup output.
    if mysql.len() == soup.len()
        && mysql
            .iter()
            .zip(soup.iter())
            .all(|(m, s)| m == s || m[..] == s[..(s.len() - 1)])
    {
        return None;
    }

    let mysql: Vec<_> = mysql.into_iter().map(|r| format!("{:?}", r)).collect();
    let soup: Vec<_> = soup.into_iter().map(|r| format!("{:?}", r)).collect();

    let mut output = String::new();
    for diff in diff::lines(&mysql.join("\n"), &soup.join("\n")) {
        match diff {
            diff::Result::Left(l) => writeln!(&mut output, "-{}", l).unwrap(),
            diff::Result::Both(l, _) => writeln!(&mut output, " {}", l).unwrap(),
            diff::Result::Right(r) => writeln!(&mut output, "+{}", r).unwrap(),
        }
    }
    Some(output)
}

async fn check_query(
    tables: &BTreeMap<String, Table>,
    query_name: &str,
    query: &Query,
    target: &BTreeMap<String, Vec<Vec<String>>>,
) -> Result<(), String> {
    let queries: Vec<_> = tables
        .values()
        .map(|t| t.create_query.clone())
        .chain(Some(query_name.to_owned() + ": " + &query.select_query))
        .collect();

    let mut g = Builder::default().start_local().await.unwrap();
    g.install_recipe(&queries.join("\n")).await.unwrap();

    for (table_name, table) in tables.iter() {
        let mut mutator = g.table(table_name).await.unwrap();
        for row in table.data.as_ref().unwrap().iter() {
            assert_eq!(row.len(), table.types.len());
            let row: Vec<DataType> = row
                .iter()
                .enumerate()
                .map(|(i, v)| table.types[i].make_datatype(v))
                .collect();
            mutator.insert(row).await.unwrap();
        }
    }

    tokio::time::sleep(time::Duration::from_millis(300)).await;

    let mut getter = g.view(query_name).await.unwrap();

    for (i, query_parameter) in query.values.iter().enumerate() {
        let query_param = query.types[0].make_datatype(&query_parameter[0]);
        let query_results = getter.lookup(&[query_param], true).await.unwrap();

        let target_results = &target[&i.to_string()];
        let query_results: Vec<Vec<String>> = query_results
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        DataType::None | DataType::Max => "NULL".to_owned(),
                        DataType::Int(i) => i.to_string(),
                        DataType::UnsignedInt(i) => i.to_string(),
                        DataType::BigInt(i) => i.to_string(),
                        DataType::UnsignedBigInt(i) => i.to_string(),
                        DataType::Float(f, _) => format!("{}", f),
                        DataType::Double(f, _) => format!("{}", f),
                        DataType::Numeric(ref d) => d.to_string(),
                        DataType::Text(_) | DataType::TinyText(_) => {
                            let s: &str = (&v).try_into().unwrap();
                            s.to_string()
                        }
                        d @ DataType::ByteArray(_) => format!("{}", d),
                        DataType::Timestamp(_) | DataType::Time(_) => {
                            unimplemented!()
                        }
                        // These types are PostgreSQL specific
                        DataType::BitVector(_) | DataType::TimestampTz(_) => unimplemented!(),
                    })
                    .collect()
            })
            .collect();

        if let Some(diff) = compare_results(target_results, &query_results) {
            return Err(format!(
                "MySQL and Soup results do not match for ? = {:?}\n{}",
                query_parameter, diff
            ));
        }
    }
    g.shutdown();
    g.wait_done().await;
    Ok(())
}

#[test]
#[ignore]
fn mysql_comparison() {
    println!();

    let mut schemas: BTreeMap<String, Schema> = BTreeMap::new();
    run_for_all_in_directory("schemas", |file_name, contents| {
        {
            let ext = Path::new(&file_name).extension();
            if ext.is_none() || ext.unwrap() != "toml" {
                return;
            }
        }
        match toml::from_str(&contents) {
            Ok(schema) => {
                schemas.insert(file_name, schema);
            }
            Err(e) => panic!("Failed to parse {}: {}", file_name, e),
        }
    });

    for schema in schemas.values_mut() {
        for table in schema.tables.values_mut() {
            assert_ne!(table.data.is_some(), table.data_file.is_some());
            if let Some(ref file_name) = table.data_file {
                table.data = Some(Vec::new());
                let data = table.data.as_mut().unwrap();

                let path = Path::new(DIRECTORY_PREFIX).join("data").join(file_name);
                let f = File::open(path).unwrap();
                let mut reader = BufReader::new(f);
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap() > 0 {
                    data.push(
                        line.split('\t')
                            .map(str::trim)
                            .map(|s| s.to_owned())
                            .collect(),
                    );
                    line.clear();
                }
            }
        }
    }

    if cfg!(feature = "generate_mysql_tests") {
        generate_target_results(&schemas);
    }

    let mut fail = false;
    for (schema_name, schema) in schemas.iter() {
        let target_data_file = Path::new(DIRECTORY_PREFIX)
            .join("targets")
            .join(schema_name);
        let target_data: BTreeMap<String, BTreeMap<String, Vec<Vec<String>>>> =
            toml::from_str(&read_file(target_data_file)).unwrap();

        for (query_name, query) in schema.queries.iter() {
            print!("{}.{}... ", schema.name, query_name);
            io::stdout().flush().expect("Could not flush stdout");

            if let Some(true) = query.ignore {
                println!("\x1B[33mIGNORED\x1B[m");
                continue;
            }

            if query.values.is_empty() {
                println!("\x1B[33mPASS\x1B[m");
                continue;
            }

            let panic_state: Arc<Mutex<Option<PanicState>>> = Arc::new(Mutex::new(None));
            set_panic_hook(panic_state.clone());
            let result = panic::catch_unwind(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(check_query(
                    &schema.tables,
                    query_name,
                    query,
                    &target_data[query_name],
                ))
            });
            let _ = panic::take_hook();
            match result {
                Ok(Ok(())) => println!("\x1B[32;1mPASS\x1B[m"),
                Ok(Err(e)) => {
                    // No panic, but test didn't pass
                    fail = true;
                    print!("\x1B[31;1mFAIL\x1B[m: {}", e)
                }
                Err(_) => {
                    // Panicked
                    fail = true;
                    let panic_state = panic_state.lock().unwrap().take().unwrap();
                    println!(
                        "\x1B[31;1mFAIL\x1B[m: \"{}\" at {}:{}\n{:?}",
                        panic_state.message,
                        panic_state.file,
                        panic_state.line,
                        panic_state.backtrace,
                    );
                }
            }
        }
    }

    panic::set_hook(Box::new(|_info| {}));
    assert!(!fail);
}
