use std::env;

use clap::Parser;
use mysql::prelude::Queryable;
use mysql::{Conn, Params};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

/// Runs all the IRL queries against the provided
/// MySQL database (which must have the schemas already installed).
/// This script will not panic, and it will report
/// how many queries succeeded and how many failed in the logs.
/// The failed queries will be showed along the error message.
#[derive(Parser, Debug)]
struct IrlQueryTest {
    /// The URL to the MySQL database.
    #[clap(long, default_value = "mysql://127.0.0.1:3307")]
    mysql_url: String,

    #[clap(long)]
    explicit_migrations: bool,
}

impl IrlQueryTest {
    async fn run(&self) {
        let opts = mysql::Opts::from_url(self.mysql_url.as_str()).unwrap();
        let mut adapter_conn = mysql::Conn::new(opts).unwrap();
        let mut total = 0;
        let mut failed = 0;
        let mut success = 0;

        /// Evaluates a given query with its parameters.
        /// $result = The expected query result TYPE. Must implement `FromRow`.
        /// $query = The query to be executed.
        /// $params = The parameters to use. Must implement `Into<Params>`.
        macro_rules! evaluate {
            ($(($query:expr, $params:expr);)+) => {
                $(evaluate!(@impl, $query, $params);)+
            };
            (@impl, $query:expr, $params:expr) => {
                match evaluate_query::<_>(&mut adapter_conn, $query, $params, self.explicit_migrations) {
                    Ok(_) => {
                        success += 1;
                    },
                    Err(_) => {
                        failed += 1;
                    },
                }
                total += 1;
            };
        }

        evaluate!(
             (r"SELECT * FROM users WHERE users.id = ? LIMIT ?", (2, 3));
             (r"SELECT * FROM user_auth_tokens WHERE auth_token = ? LIMIT ?", ("2", 3));
             (r"SELECT * FROM users WHERE id = ? LIMIT ?", (1, 2));
             (r"SELECT * FROM users WHERE users.id in (?, ?, ?)", (2,3,4));
             (r"SELECT hashtags.*, invites_hashtags.invite_id, invites_hashtags.hashtag_id, invites_hashtags.created_at, invites_hashtags.updated_at
                FROM hashtags INNER JOIN invites_hashtags ON hashtags.id = invites_hashtags.hashtag_id
                WHERE invites_hashtags.invite_id in (?, ?, ?)", (1,2,3));
             (r"SELECT * FROM invites_users WHERE invites_users.invite_id in (?, ?, ?) AND invites_users.user_id = ?", (1,2,3, 2));
             (r"select external_calendars.*, external_invites.invite_id from external_calendars inner join external_invites on external_invites.external_calendar_id=external_calendars.id where external_invites.invite_id in (?, ?, ?)", (1,2,3));
             (r"SELECT count(*) FROM invites_users WHERE invite_id=? and is_host=?", (1,0));
             (r"SELECT count(*) FROM invites_users WHERE invite_id=? and attending=?", (1,0));
             (r"SELECT count(*) FROM users_groups WHERE group_id=? and joined_at is not null", (1,));
        );

        info!(
            "run a total of {} queries, {} succeeded and {} failed",
            total, success, failed
        );
    }
}

fn evaluate_query<T>(
    adapter_conn: &mut Conn,
    query: &str,
    params: T,
    explicit_migrations: bool,
) -> Result<(), ()>
where
    T: Into<Params> + Clone,
{
    debug!("running query");
    let mut err = None;

    // For statements to hit noria they must be migrated beforehand with
    // CREATE CACHE FROM.
    if explicit_migrations {
        let stmt = "CREATE CACHE FROM ".to_string() + query;
        match adapter_conn.query_drop(stmt) {
            Ok(_) => err = None,
            Err(e) => {
                err = Some(e);
            }
        }
    }

    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(10));

        let params = params.clone();
        match adapter_conn.exec_drop::<_, _>(query, params) {
            Ok(_) => err = None,
            Err(e) => {
                err = Some(e);
            }
        }
    }

    if let Some(e) = err {
        let err_msg = e.to_string();
        info!(%query, %err_msg, "query failed");
        Err(())
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::new(
            env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned()),
        ))
        .init();

    let opts = IrlQueryTest::parse();
    opts.run().await;
}
