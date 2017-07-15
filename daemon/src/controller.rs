use slog::Logger;
use std::net::TcpStream;

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
    pub fn listen(&self) {
        use std::net::TcpListener;

        let listener = TcpListener::bind(&format!("0.0.0.0:{}", self.listen_port)).unwrap();
        loop {
            match listener.accept() {
                Ok((socket, addr)) => {
                    info!(self.log, "new worker connected from: {:?}", addr);
                }
                Err(e) => error!(self.log, "worker failed to connect: {:?}", e),
            }
        }
    }
}
