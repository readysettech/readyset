#![feature(duration_float)]
#![feature(type_ascription)]

#[macro_use]
extern crate clap;
extern crate noria;
#[macro_use]
extern crate mysql;
extern crate rand;

use mysql as my;
use noria::DataType;
use std::time;

#[macro_use]
mod populate;
use populate::Populate;

struct Backend {
    pool: mysql::Pool,
}

impl Backend {
    fn new(addr: &str) -> Backend {
        Backend {
            pool: my::Pool::new_manual(1, 1, addr).unwrap(),
        }
    }

    pub fn read(&self, uid: i32) {
        let qstring = format!(
            "SELECT p_author, COUNT(p_id) FROM Post WHERE p_author={} GROUP BY p_author",
            uid
        );
        self.pool.prep_exec(qstring, ()).unwrap();
    }

    pub fn secure_read(&self, uid: i32, logged_uid: i32) {
        let qstring = format!(
            "SELECT p_author, count(p_id) FROM Post \
                WHERE \
                p_author = {} AND \
                (
                    (Post.p_private = 1 AND Post.p_author = {}) OR \
                    (Post.p_private = 1 AND Post.p_cid in (SELECT r_cid FROM Role WHERE r_role = 1 AND Role.r_uid = {})) OR \
                    (Post.p_private = 0 AND Post.p_cid in (SELECT r_cid FROM Role WHERE r_role = 0 AND Role.r_uid = {})) \
                ) \
                GROUP BY p_author",
            uid,
            logged_uid,
            logged_uid,
            logged_uid
        );

        self.pool.prep_exec(qstring, ()).unwrap();
    }

    pub fn populate_tables(&self, pop: &mut Populate) {
        pop.enroll_students();
        let roles = pop.get_roles();
        let users = pop.get_users();
        let posts = pop.get_posts();
        let classes = pop.get_classes();

        self.populate("Role", roles);
        self.populate("User", users);
        self.populate("Post", posts);
        self.populate("Class", classes);
    }

    fn populate(&self, name: &'static str, records: Vec<Vec<DataType>>) {
        let params_arr: Vec<_> = records
            .iter()
            .map(|ref r| match name.as_ref() {
                "Role" => params! {
                    "r_uid" => r[0].clone().into() : i32,
                    "r_cid" => r[1].clone().into() : i32,
                    "r_role" => r[2].clone().into() : i32,
                },
                "User" => params! {
                    "u_id" => r[0].clone().into() : i32,
                },
                "Post" => params! {
                    "p_id" => r[0].clone().into() : i32,
                    "p_cid" => r[1].clone().into() : i32,
                    "p_author" => r[2].clone().into() : i32,
                    "p_content" => r[3].clone().into() : String,
                    "p_private" => r[4].clone().into() : i32,
                },
                "Class" => params! {
                    "c_id" => r[0].clone().into() : i32,
                },
                _ => panic!("unspecified table"),
            })
            .collect();

        let qstring = match name.as_ref() {
            "Role" => "INSERT INTO Role (r_uid, r_cid, r_role) VALUES (:r_uid, :r_cid, :r_role)",
            "User" => "INSERT INTO User (u_id) VALUES (:u_id)",
            "Post" => "INSERT INTO Post (p_id, p_cid, p_author, p_content, p_private) VALUES (:p_id, :p_cid, :p_author, :p_content, :p_private)",
            "Class" => "INSERT INTO Class (c_id) VALUES (:c_id)",
            _ => panic!("unspecified table"),
        };

        let start = time::Instant::now();
        for mut stmt in self.pool.prepare(qstring).into_iter() {
            for params in params_arr.iter() {
                stmt.execute(params).unwrap();
            }
        }
        let dur = start.elapsed().as_float_secs();
        println!(
            "Inserted {} {} in {:.2}s ({:.2} PUTs/sec)!",
            records.len(),
            name,
            dur,
            records.len() as f64 / dur
        );
    }

    fn create_connection(&self, db: &str) {
        let mut conn = self.pool.get_conn().unwrap();
        if conn.query(format!("USE {}", db)).is_ok() {
            conn.query(format!("DROP DATABASE {}", &db).as_str())
                .unwrap();
        }

        conn.query(format!("CREATE DATABASE {}", &db).as_str())
            .unwrap();
        conn.query(format!("USE {}", db)).unwrap();

        drop(conn);
    }

    fn create_tables(&self) {
        self.pool
            .prep_exec(
                "CREATE TABLE Post ( \
                 p_id int(11) NOT NULL, \
                 p_cid int(11) NOT NULL, \
                 p_author int(11) NOT NULL, \
                 p_content varchar(258) NOT NULL, \
                 p_private tinyint(1) NOT NULL default '0', \
                 PRIMARY KEY (p_id), \
                 UNIQUE KEY p_id (p_id), \
                 KEY p_cid (p_cid), \
                 KEY p_author (p_author) \
                 );",
                (),
            )
            .unwrap();

        self.pool
            .prep_exec(
                "CREATE TABLE User ( \
                 u_id int(11) NOT NULL, \
                 PRIMARY KEY  (u_id), \
                 UNIQUE KEY u_id (u_id) \
                 );",
                (),
            )
            .unwrap();

        self.pool
            .prep_exec(
                "CREATE TABLE Class ( \
                 c_id int(11) NOT NULL, \
                 PRIMARY KEY  (c_id), \
                 UNIQUE KEY c_id (c_id) \
                 );",
                (),
            )
            .unwrap();

        self.pool
            .prep_exec(
                "CREATE TABLE Role ( \
                 r_uid int(11) NOT NULL, \
                 r_cid int(11) NOT NULL, \
                 r_role tinyint(1) NOT NULL default '0', \
                 KEY r_uid (r_uid), \
                 KEY r_cid (r_cid) \
                 );",
                (),
            )
            .unwrap();
    }
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("piazza-mysql")
        .version("0.1")
        .about("Benchmarks a forum like application with security policies using MySql")
        .arg(Arg::with_name("dbname").required(true))
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
                .help("Number of logged users"),
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

    let dbn = args.value_of("dbname").unwrap();
    let nusers = value_t_or_exit!(args, "nusers", i32);
    let nlogged = value_t_or_exit!(args, "nlogged", i32);
    let nclasses = value_t_or_exit!(args, "nclasses", i32);
    let nposts = value_t_or_exit!(args, "nposts", i32);
    let private = value_t_or_exit!(args, "private", f32);

    let backend = Backend::new(dbn);

    let db = &dbn[dbn.rfind("/").unwrap() + 1..];
    backend.create_connection(db);
    backend.create_tables();

    let mut p = Populate::new(nposts, nusers, nclasses, private);
    backend.populate_tables(&mut p);

    // Do some reads without security
    let start = time::Instant::now();
    for uid in 0..nusers {
        backend.read(uid);
    }

    let dur = start.elapsed().as_float_secs();
    println!(
        "GET without security: {} in {:.2}s ({:.2} GET/sec)!",
        nusers,
        dur,
        (nusers) as f64 / dur
    );

    // Do some reads WITH security
    let start = time::Instant::now();
    for uid in 0..nusers {
        backend.secure_read(uid, 0);
    }
    let dur = start.elapsed().as_float_secs();
    println!(
        "GET with security: {} in {:.2}s ({:.2} GET/sec)!",
        nusers,
        dur,
        (nusers) as f64 / dur
    );
}
