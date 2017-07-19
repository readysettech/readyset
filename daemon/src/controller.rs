use bincode;
use slog::Logger;
use std::net::{SocketAddr, TcpStream};

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
        use std::net::TcpListener;

        let listener = TcpListener::bind(&format!("0.0.0.0:{}", self.listen_port)).unwrap();
        loop {
            match listener.accept() {
                Ok((mut socket, addr)) => self.handle_register(socket, addr),
                Err(e) => error!(self.log, "worker failed to connect: {:?}", e),
            }
        }
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
