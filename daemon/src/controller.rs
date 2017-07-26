use channel::tcp::TcpSender;
use channel::poll::{PollEvent, PollingLoop};
use slog::Logger;
use std::io;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use protocol::{CoordinationMessage, CoordinationPayload};

#[derive(Default)]
pub struct WorkerStatus {
    healthy: bool,
    last_heartbeat: Option<Instant>,
    sender: Option<TcpSender<CoordinationMessage>>,
}

pub struct Controller {
    listen_addr: String,
    listen_port: u16,

    log: Logger,

    workers: HashMap<SocketAddr, WorkerStatus>,
}

impl Controller {
    pub fn new(listen_addr: &str, port: u16, log: Logger) -> Controller {
        Controller {
            listen_addr: String::from(listen_addr),
            listen_port: port,
            log: log,
            workers: HashMap::new(),
        }
    }

    /// Listen for workers to connect
    pub fn listen(&mut self) {
        use channel::poll::ProcessResult;
        use mio::net::TcpListener;
        use std::str::FromStr;

        let listener = TcpListener::bind(&SocketAddr::from_str(
            &format!("{}:{}", self.listen_addr, self.listen_port),
        ).unwrap()).unwrap();

        let local_addr = listener.local_addr().unwrap();

        let mut pl: PollingLoop<CoordinationMessage> = PollingLoop::from_listener(listener);
        pl.run_polling_loop(|e| {
            match e {
                PollEvent::Process(ref msg) => {
                    debug!(self.log, "Received {:?}", msg);
                    match self.handle(msg) {
                        Ok(_) => {
                            // XXX(malte): don't always send AssignDomain; just for testing here
                            let mut ws = self.workers.get_mut(&msg.source).unwrap();
                            ws.sender
                                .as_mut()
                                .unwrap()
                                .send(CoordinationMessage {
                                    source: local_addr,
                                    payload: CoordinationPayload::AssignDomain,
                                })
                                .unwrap();
                        }
                        Err(e) => error!(self.log, "failed to handle message {:?}: {:?}", msg, e),
                    }
                }
                PollEvent::ResumePolling(_) => (),
                PollEvent::Timeout => (),
            }
            ProcessResult::KeepPolling
        })
    }

    fn handle(&mut self, msg: &CoordinationMessage) -> Result<(), io::Error> {
        match msg.payload {
            CoordinationPayload::Register(ref remote) => self.handle_register(msg, remote),
            CoordinationPayload::Heartbeat => self.handle_heartbeat(msg),
            _ => unimplemented!(),
        }
    }

    fn handle_register(
        &mut self,
        msg: &CoordinationMessage,
        remote: &SocketAddr,
    ) -> Result<(), io::Error> {
        info!(
            self.log,
            "new worker registered from {:?}, which listens on {:?}",
            msg.source,
            remote
        );

        let mut ws = WorkerStatus::default();
        ws.sender = Some(TcpSender::connect(remote, None)?);

        self.workers.insert(msg.source.clone(), ws);

        Ok(())
    }

    fn handle_heartbeat(&mut self, msg: &CoordinationMessage) -> Result<(), io::Error> {
        match self.workers.get_mut(&msg.source) {
            None => {
                crit!(
                    self.log,
                    "got heartbeat for unknown worker {:?}",
                    msg.source
                )
            }
            Some(ref mut ws) => {
                ws.last_heartbeat = Some(Instant::now());
            }
        }

        Ok(())
    }
}
