use log::error;
use redis::Commands;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::result;
use thiserror::Error;

use crate::taste::BenchmarkResult;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    RedisError(#[from] redis::RedisError),

    #[error("Error deserializing data from redis: {0}")]
    DeserializeError(#[from] bincode::Error),
}

pub type Result<T> = result::Result<T, Error>;

pub type BranchHistory = HashMap<String, HashMap<String, BenchmarkResult<f64>>>;

pub struct Persistence {
    client: redis::Client,
}

impl<'a> TryFrom<&'a clap::ArgMatches<'a>> for Persistence {
    type Error = Error;

    fn try_from(args: &'a clap::ArgMatches<'a>) -> Result<Self> {
        Self::new(
            args.value_of("redis_url")
                .unwrap_or("redis://127.0.0.1:6379/"),
        )
    }
}

impl Persistence {
    pub fn new<C>(url: C) -> Result<Self>
    where
        C: redis::IntoConnectionInfo,
    {
        Ok(Self {
            client: redis::Client::open(url)?,
        })
    }

    /// Return a hash map of the latest results per-benchmark for the given branch
    pub fn branch(&self, name: &str) -> Result<BranchHistory> {
        self.client
            .get_connection()?
            .hgetall::<_, HashMap<String, Vec<u8>>>(format!("branch:{}", name))?
            .into_iter()
            .map(|(k, v)| Ok((k, bincode::deserialize(&v)?)))
            .collect::<Result<HashMap<_, _>>>()
    }

    /// Save results for a single benchmark
    pub fn save_result(
        &self,
        branch_name: &str,
        benchmark_name: &str,
        result: &HashMap<String, BenchmarkResult<f64>>,
    ) -> Result<()> {
        self.client.get_connection()?.hset(
            format!("branch:{}", branch_name),
            benchmark_name,
            bincode::serialize(result)?,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;
    use std::env;

    use super::*;

    fn setup() -> Persistence {
        Persistence::new(
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/1".to_owned()),
        )
        .unwrap()
    }

    #[test]
    fn load_empty() {
        let p = setup();
        assert!(p.branch("some-branch").unwrap().is_empty());
    }

    #[test]
    fn save_then_load() {
        let p = setup();
        let result = hashmap! {
            "95p".to_owned() => BenchmarkResult::Improvement(1.0, 1.0),
        };
        p.save_result("main", "my/bench", &result).unwrap();
        let res = p.branch("main").unwrap();
        assert_eq!(res.keys().collect::<Vec<_>>(), vec!["my/bench"]);
    }
}
