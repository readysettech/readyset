use channel::poll::{PollEvent, PollingLoop};
use slog::Logger;
use std::net::SocketAddr;

use protocol::{CoordinationMessage, CoordinationPayload};

pub struct Controller {
    listen_port: u16,
    log: Logger,
}

impl Controller {
    pub fn new(port: u16, log: Logger) -> Controller {
        Controller {
            listen_port: port,
            log: log,
        }
    }

    /// Listen for workers to connect
    pub fn listen(&mut self) {
        use channel::poll::ProcessResult;
        use mio::net::TcpListener;
        use std::str::FromStr;

        let listener = TcpListener::bind(&SocketAddr::from_str(
            &format!("0.0.0.0:{}", self.listen_port),
        ).unwrap()).unwrap();

        let mut pl: PollingLoop<CoordinationMessage> = PollingLoop::from_listener(listener);
        pl.run_polling_loop(|e| {
            match e {
                PollEvent::Process(ref msg) => {
                    debug!(self.log, "Received {:?}", msg);
                    self.handle(msg);
                }
                PollEvent::ResumePolling(_) => (),
                PollEvent::Timeout => (),
            }
            ProcessResult::KeepPolling
        })
    }

    fn handle(&mut self, msg: &CoordinationMessage) {
        match msg.payload {
            CoordinationPayload::Register => self.handle_register(msg),
            _ => unimplemented!(),
        }
    }

    fn handle_register(&mut self, msg: &CoordinationMessage) {
        info!(self.log, "new worker registered from {:?}", msg.source);
    }
}
