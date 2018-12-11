use clap;
use tokio::prelude::*;

pub(crate) struct Parameters {
    pub(crate) prime: bool,
    pub(crate) articles: usize,
}

pub(crate) trait VoteClient: Clone {
    type NewFuture: Future<Item = Self>;
    type ReadFuture: Future<Item = ()>;
    type WriteFuture: Future<Item = ()>;

    fn spawn(params: &Parameters, args: &clap::ArgMatches) -> Self::NewFuture;
    fn handle_reads(&mut self, requests: &[i32]) -> Self::ReadFuture;
    fn handle_writes(&mut self, requests: &[i32]) -> Self::WriteFuture;
}

//pub(crate) mod hybrid;
pub(crate) mod localsoup;
//pub(crate) mod memcached;
//pub(crate) mod mssql;
//pub(crate) mod mysql;
//pub(crate) mod netsoup;
