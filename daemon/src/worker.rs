use slog::Logger;
use std::net::TcpStream;

pub struct Worker {
    controller_addr: String,
    log: Logger,
}

impl Worker {
    pub fn new(controller: &str, log: Logger) -> Worker {
        Worker {
            controller_addr: String::from(controller),
            log: log,
        }
    }

    /// Connect to controller
    pub fn connect(&self) {
        let mut stream = TcpStream::connect(&self.controller_addr);
        match stream {
            Ok(_) => (),
            Err(e) => error!(self.log, "worker failed to connect: {:?}", e),
        }
    }
}
