use channel::poll::{PollEvent, PollingLoop};
use slog::Logger;
use std::net::SocketAddr;

use protocol::CoordinationMessage;

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
                PollEvent::Process(msg) => {
                    info!(self.log, "Got {:?}", msg);
                }
                PollEvent::ResumePolling(_) => (),
                PollEvent::Timeout => (),
            }
            ProcessResult::KeepPolling
        })
    }

    fn handle_register(&mut self, mut socket: TcpStream, remote_addr: SocketAddr) {
        use std::io::Read;

        info!(self.log, "new worker connected from: {:?}", remote_addr);

        let mut buf = [0; 2000];
        loop {
            // XXX(malte): delimited reads
            socket.read(&mut buf);
            let msg: CoordinationMessage = bincode::deserialize(&buf).unwrap();
            info!(self.log, "got {:#?}", msg);
        }
        // XXX(malte): insert socket into epoll selector
    }
}
