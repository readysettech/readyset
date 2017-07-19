use bincode;
use slog::Logger;
use std::io;
use std::net::TcpStream;
use std::time::{Instant, Duration};

use protocol::CoordinationMessage;

pub struct Worker {
    controller_addr: String,
    log: Logger,
    socket: Option<TcpStream>,
}

impl Worker {
    pub fn new(controller: &str, log: Logger) -> Worker {
        Worker {
            controller_addr: String::from(controller),
            log: log,
            socket: None,
        }
    }

    /// Connect to controller
    pub fn connect(&mut self) {
        let stream = TcpStream::connect(&self.controller_addr);
        match stream {
            Ok(s) => {
                self.socket = Some(s);

                // say hello
                self.register();

                // enter worker loop, wait for instructions
                self.handle();
            }
            Err(e) => error!(self.log, "worker failed to connect: {:?}", e),
        }
    }

    fn write_serialized(&mut self, msg: CoordinationMessage) -> Result<(), io::Error> {
        use std::io::Write;

        let data: Vec<u8> = bincode::serialize(&msg, bincode::Infinite).unwrap();
        match self.socket {
            None => crit!(self.log, "not connected to controller!"),
            Some(ref mut s) => s.write_all(&data)?,
        }
        Ok(())
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

    fn heartbeat(&mut self) -> Result<(), io::Error> {
        let msg = CoordinationMessage::Heartbeat;
        self.write_serialized(msg)
    }

    fn register(&mut self) -> Result<(), io::Error> {
        let msg = CoordinationMessage::Register;
        self.write_serialized(msg)
    }
}
