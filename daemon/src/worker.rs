use channel::{self, TcpReceiver, TcpSender};
use channel::tcp::TryRecvError;
use slog::Logger;
use std::net::SocketAddr;
use std::time::{Instant, Duration};

use protocol::{CoordinationMessage, CoordinationPayload};

pub struct Worker {
    log: Logger,

    controller_addr: String,
    receiver: Option<TcpReceiver<CoordinationMessage>>,
    sender: Option<TcpSender<CoordinationMessage>>,

    // liveness
    heartbeat_every: Duration,
    last_heartbeat: Option<Instant>,
}

impl Worker {
    pub fn new(controller: &str, log: Logger) -> Worker {
        Worker {
            log: log,

            controller_addr: String::from(controller),
            receiver: None,
            sender: None,

            heartbeat_every: Duration::from_millis(1000),
            last_heartbeat: None,
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
                self.last_heartbeat = Some(Instant::now());

                // say hello
                self.register()?;

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Main worker loop: waits for instructions from master, and occasionally heartbeats to tell
    /// the master that we're still live
    pub fn handle(&mut self) {
        loop {
            if self.receiver.is_some() {
                match self.receiver.as_mut().unwrap().try_recv() {
                    Ok(msg) => info!(self.log, "received from controller: {:?}", msg),
                    Err(e) => {
                        match e {
                            TryRecvError::Disconnected => {
                                error!(self.log, "controller disconnected!");
                                return;
                            }
                            TryRecvError::Empty => (),
                            TryRecvError::DeserializationError => {
                                crit!(self.log, "failed to deserialize message from controller!");
                            }
                        }
                    }
                }
            }

            match self.heartbeat() {
                Ok(_) => (),
                Err(e) => {
                    error!(self.log, "failed to send heartbeat to controller: {:?}", e);
                    return;
                }
            };
        }
    }

    fn wrap_payload(&self, pl: CoordinationPayload) -> CoordinationMessage {
        let addr = match self.sender {
            None => panic!("socket not connected, failed to send"),
            Some(ref s) => s.local_addr().unwrap(),
        };
        CoordinationMessage {
            source: addr,
            payload: pl,
        }
    }

    fn heartbeat(&mut self) -> Result<(), channel::tcp::Error> {
        if self.last_heartbeat.is_some() &&
            self.last_heartbeat.as_ref().unwrap().elapsed() > self.heartbeat_every
        {
            let msg = self.wrap_payload(CoordinationPayload::Heartbeat);
            match self.sender.as_mut().unwrap().send(msg) {
                Ok(_) => debug!(self.log, "sent heartbeat to controller"),
                Err(e) => return Err(e),
            }

            self.last_heartbeat = Some(Instant::now());
        }
        Ok(())
    }

    fn register(&mut self) -> Result<(), channel::tcp::Error> {
        let msg = self.wrap_payload(CoordinationPayload::Register);
        self.sender.as_mut().unwrap().send(msg)
    }
}
