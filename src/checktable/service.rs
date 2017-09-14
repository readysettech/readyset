use std::net::SocketAddr;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use petgraph::graph::NodeIndex;
use tarpc::future::server;
use tarpc::util::Never;
use tokio_core::reactor;

use super::*;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TransactionId(pub u64);

pub type TransactionBatch = Vec<(TransactionId, Records, Option<Token>)>;

#[derive(Serialize, Deserialize)]
pub struct TimestampRequest {
    pub transactions: TransactionBatch,
    pub base: NodeIndex,
}

#[derive(Serialize, Deserialize)]
pub struct TimestampReply {
    pub timestamp: i64,
    pub committed_transactions: Vec<TransactionId>,
    pub prevs: Option<Box<HashMap<domain::Index, i64>>>,
}

service! {
    rpc apply_batch(request: TimestampRequest) -> Option<TimestampReply>;
    rpc claim_replay_timestamp(tag: Tag) -> (i64, Option<Box<HashMap<domain::Index, i64>>>);
    rpc track(token_generator: TokenGenerator);
    rpc perform_migration(deps: HashMap<domain::Index, (IngressFromBase, EgressForBase)>)
                          -> (i64, i64, Option<Box<HashMap<domain::Index, i64>>>);
    rpc add_replay_paths(additional_replay_paths: HashMap<ReplayPath, Vec<domain::Index>>);
    rpc validate_token(token: Token) -> bool;
}

#[derive(Clone)]
pub struct CheckTableServer {
    checktable: Arc<Mutex<CheckTable>>,
}
impl CheckTableServer {
    pub fn start(listen_addr: SocketAddr) -> SocketAddr {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let server = CheckTableServer {
                checktable: Arc::new(Mutex::new(CheckTable::new())),
            };

            let mut reactor = reactor::Core::new().unwrap();
            let (handle, server) = server
                .listen(listen_addr, &reactor.handle(), server::Options::default())
                .unwrap();
            tx.send(handle.addr()).unwrap();
            reactor.run(server)
        });
        rx.recv().unwrap()
    }
}
impl FutureService for CheckTableServer {
    type ApplyBatchFut = Result<Option<TimestampReply>, Never>;
    fn apply_batch(&self, request: TimestampRequest) -> Self::ApplyBatchFut {
        let mut checktable = self.checktable.lock().unwrap();
        let (tr, committed) = checktable.apply_batch(request.base, &request.transactions);
        Ok(match tr {
            TransactionResult::Committed(ts, prevs) => Some(TimestampReply {
                timestamp: ts,
                prevs,
                committed_transactions: committed,
            }),
            TransactionResult::Aborted => None,
        })
    }

    type ClaimReplayTimestampFut = Result<(i64, Option<Box<HashMap<domain::Index, i64>>>), Never>;
    fn claim_replay_timestamp(&self, tag: Tag) -> Self::ClaimReplayTimestampFut {
        Ok(self.checktable.lock().unwrap().claim_replay_timestamp(&tag))
    }

    type TrackFut = Result<(), Never>;
    fn track(&self, token_generator: TokenGenerator) -> Self::TrackFut {
        self.checktable.lock().unwrap().track(&token_generator);
        Ok(())
    }

    type PerformMigrationFut = Result<(i64, i64, Option<Box<HashMap<domain::Index, i64>>>), Never>;
    fn perform_migration(
        &self,
        deps: HashMap<domain::Index, (IngressFromBase, EgressForBase)>,
    ) -> Self::PerformMigrationFut {
        Ok(self.checktable.lock().unwrap().perform_migration(&deps))
    }

    type AddReplayPathsFut = Result<(), Never>;
    fn add_replay_paths(
        &self,
        additional_replay_paths: HashMap<ReplayPath, Vec<domain::Index>>,
    ) -> Self::AddReplayPathsFut {
        let mut checktable = self.checktable.lock().unwrap();
        Ok(checktable.add_replay_paths(additional_replay_paths))
    }

    type ValidateTokenFut = Result<bool, Never>;
    fn validate_token(&self, token: Token) -> Self::ValidateTokenFut {
        Ok(self.checktable.lock().unwrap().validate_token(&token))
    }
}
