use clap;
use tokio::prelude::*;

#[derive(Copy, Clone, Debug)]
pub(crate) struct Parameters {
    pub(crate) prime: bool,
    pub(crate) articles: usize,
}

pub(crate) trait VoteClient: Clone {
    type NewFuture: Future<Item = Self, Error = failure::Error> + Send + 'static;
    type ReadFuture: Future<Item = (), Error = failure::Error> + Send + 'static;
    type WriteFuture: Future<Item = (), Error = failure::Error> + Send + 'static;

    fn spawn(
        ex: tokio::runtime::TaskExecutor,
        params: Parameters,
        args: clap::ArgMatches,
    ) -> Self::NewFuture;
    fn handle_reads(&mut self, requests: &[i32]) -> Self::ReadFuture;
    fn handle_writes(&mut self, requests: &[i32]) -> Self::WriteFuture;
}

//pub(crate) mod hybrid;
//pub(crate) mod localsoup;
//pub(crate) mod memcached;
//pub(crate) mod mssql;
//pub(crate) mod mysql;
//pub(crate) mod netsoup;
