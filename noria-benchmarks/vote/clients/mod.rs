use clap;

pub(crate) struct Parameters {
    pub(crate) prime: bool,
    pub(crate) articles: usize,
}

pub(crate) trait VoteClientConstructor {
    type Instance: VoteClient;
    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self;
    fn make(&mut self) -> Self::Instance;
    fn spawns_threads() -> bool {
        false
    }
}

impl VoteClientConstructor for () {
    type Instance = ();
    fn new(_: &Parameters, _: &clap::ArgMatches) -> Self {}
    fn make(&mut self) -> Self::Instance {}
}

pub(crate) trait VoteClient {
    fn handle_reads(&mut self, requests: &[i32]);
    fn handle_writes(&mut self, requests: &[i32]);
}

impl VoteClient for () {
    fn handle_reads(&mut self, _: &[i32]) {
        ::std::thread::sleep(::std::time::Duration::from_micros(300));
    }
    fn handle_writes(&mut self, _: &[i32]) {
        ::std::thread::sleep(::std::time::Duration::from_micros(200));
    }
}

pub(crate) mod hybrid;
pub(crate) mod localsoup;
pub(crate) mod memcached;
pub(crate) mod mssql;
pub(crate) mod mysql;
pub(crate) mod netsoup;
