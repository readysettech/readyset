use channel::{self, TcpReceiver, TcpSender};
use slog::Logger;
use std::net::SocketAddr;
use std::time::{Instant, Duration};

use protocol::CoordinationMessage;

pub struct Worker {
    controller_addr: String,
    log: Logger,
    _receiver: Option<TcpReceiver<CoordinationMessage>>,
    sender: Option<TcpSender<CoordinationMessage>>,
}

impl Worker {
    pub fn new(controller: &str, log: Logger) -> Worker {
        Worker {
            controller_addr: String::from(controller),
            log: log,
            _receiver: None,
            sender: None,
        }
    }

    /// Connect to controller
    pub fn connect(&mut self) -> Result<(), channel::tcp::Error> {
        use std::str::FromStr;

        let stream =
            TcpSender::connect(&SocketAddr::from_str(&self.controller_addr).unwrap(), None);
        match stream {
            Ok(s) => {
                self.sender = Some(s);

                // say hello
                self.register()?;

                // enter worker loop, wait for instructions
                self.handle();

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn handle(&mut self) {
        let mut last_heartbeat = Instant::now();

        loop {
            while last_heartbeat.elapsed() < Duration::from_millis(1000) {}

            match self.heartbeat() {
                Ok(_) => (),
                Err(e) => error!(self.log, "failed to send heartbeat to controller: {:?}", e),
            }

            last_heartbeat = Instant::now();
        }
    }

    fn heartbeat(&mut self) -> Result<(), channel::tcp::Error> {
        let msg = CoordinationMessage::Heartbeat;
        self.sender.as_mut().unwrap().send(msg)
    }

    fn register(&mut self) -> Result<(), channel::tcp::Error> {
        let msg = CoordinationMessage::Register;
        self.sender.as_mut().unwrap().send(msg)
    }
}
