use channel::{self, TcpSender};
use channel::poll::{PollEvent, PollingLoop, ProcessResult};
use slog::Logger;
use std::net::SocketAddr;
use std::time::{Instant, Duration};

use protocol::{CoordinationMessage, CoordinationPayload};

pub struct Worker {
    log: Logger,

    controller_addr: String,
    listen_addr: String,
    listen_port: u16,
    receiver: Option<PollingLoop<CoordinationMessage>>,
    sender: Option<TcpSender<CoordinationMessage>>,

    // liveness
    heartbeat_every: Duration,
    last_heartbeat: Option<Instant>,
}

impl Worker {
    pub fn new(
        controller: &str,
        listen_addr: &str,
        port: u16,
        heartbeat_every: Duration,
        log: Logger,
    ) -> Worker {
        Worker {
            log: log,

            listen_addr: String::from(listen_addr),
            listen_port: port,
            controller_addr: String::from(controller),

            receiver: None,
            sender: None,

            heartbeat_every: heartbeat_every,
            last_heartbeat: None,
        }
    }

    /// Connect to controller
    pub fn connect(&mut self) -> Result<(), channel::tcp::Error> {
        use mio::net::TcpListener;
        use std::str::FromStr;

        let local_addr = match self.receiver {
            Some(ref r) => r.get_listener_addr().unwrap(),
            None => {
                let listener = TcpListener::bind(&SocketAddr::from_str(
                    &format!("{}:{}", self.listen_addr, self.listen_port),
                ).unwrap()).unwrap();
                let addr = listener.local_addr().unwrap();
                self.receiver = Some(PollingLoop::from_listener(listener));
                addr
            }
        };

        let stream =
            TcpSender::connect(&SocketAddr::from_str(&self.controller_addr).unwrap(), None);
        match stream {
            Ok(s) => {
                self.sender = Some(s);
                self.last_heartbeat = Some(Instant::now());

                // say hello
                self.register(local_addr)?;

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Main worker loop: waits for instructions from controller, and occasionally heartbeats to tell
    /// the controller that we're still here
    pub fn handle(&mut self) {
        // needed to make the borrow checker happy, replaced later
        let mut receiver = self.receiver.take();

        receiver.as_mut().unwrap().run_polling_loop(|e| {
            match e {
                PollEvent::ResumePolling(timeout) => {
                    *timeout = Some(self.heartbeat_every);
                    return ProcessResult::KeepPolling;
                }
                PollEvent::Process(ref msg) => {
                    debug!(self.log, "Received {:?}", msg);
                }
                PollEvent::Timeout => (),
            }

            match self.heartbeat() {
                Ok(_) => ProcessResult::KeepPolling,
                Err(e) => {
                    error!(self.log, "failed to send heartbeat to controller: {:?}", e);
                    ProcessResult::StopPolling
                }
            }
        });

        self.receiver = receiver;
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

    fn register(&mut self, listen_addr: SocketAddr) -> Result<(), channel::tcp::Error> {
        let msg = self.wrap_payload(CoordinationPayload::Register(listen_addr));
        self.sender.as_mut().unwrap().send(msg)
    }
}
