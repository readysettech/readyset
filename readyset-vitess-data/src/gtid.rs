use std::fmt;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::mysql56;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GtidSet {
    Mysql56(mysql56::Mysql56GtidSet),
    Current,
}

impl GtidSet {
    pub fn flavor(&self) -> &str {
        match self {
            GtidSet::Mysql56(_) => "MySQL56",
            GtidSet::Current => "current",
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            GtidSet::Mysql56(gtid_set) => format!("{}/{}", self.flavor(), gtid_set),
            GtidSet::Current => "current".to_string(),
        }
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl TryFrom<&str> for GtidSet {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s == "current" {
            return Ok(GtidSet::Current);
        }

        // VGTID string format: <flavor>/<gtid_set>
        // Example: MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            panic!("invalid VGTID string: {}", s);
        }

        let flavor = parts[0];
        let gtid_set = parts[1];

        match flavor {
            "MySQL56" => Ok(GtidSet::Mysql56(gtid_set.try_into()?)),
            _ => Err(anyhow!("unsupported VGTID flavor: {}", flavor)),
        }
    }
}

impl PartialOrd for GtidSet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (GtidSet::Mysql56(a), GtidSet::Mysql56(b)) => a.partial_cmp(b),
            (GtidSet::Current, GtidSet::Current) => Some(std::cmp::Ordering::Equal),
            _ => None,
        }
    }
}
