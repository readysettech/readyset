#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Backend {
    Netsoup { shards: Option<usize> },
    Mssql,
    Mysql,
    Memcached,
    Hybrid,
}

impl Backend {
    pub(crate) fn multiclient_name(&self) -> &'static str {
        match *self {
            Backend::Netsoup { .. } => "netsoup",
            Backend::Mssql { .. } => "mssql",
            Backend::Mysql { .. } => "mysql",
            Backend::Memcached { .. } => "memcached",
            Backend::Hybrid { .. } => "hybrid",
        }
    }

    pub(crate) fn systemd_name(&self) -> Option<&'static str> {
        match *self {
            Backend::Memcached => Some("memcached"),
            Backend::Mysql => Some("mariadb"),
            Backend::Mssql => Some("mssql-server"),
            Backend::Netsoup { .. } | Backend::Hybrid { .. } => None,
        }
    }

    pub(crate) fn uniq_name(&self) -> String {
        match *self {
            Backend::Netsoup { shards } => format!("netsoup_{}s", shards.unwrap_or(0)),
            Backend::Hybrid | Backend::Memcached | Backend::Mysql | Backend::Mssql => {
                self.multiclient_name().to_string()
            }
        }
    }

    pub(crate) fn port(&self) -> u16 {
        match *self {
            Backend::Netsoup { .. } => unreachable!(),
            Backend::Memcached => 11211,
            Backend::Hybrid => 3306,
            Backend::Mysql => 3306,
            Backend::Mssql => 1433,
        }
    }
}
