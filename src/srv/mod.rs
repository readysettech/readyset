use flow::prelude::*;
use flow;

use futures;
use tarpc;
use tarpc::util::Never;
use tokio_core::reactor;
use vec_map::VecMap;

use std::collections::HashMap;
use std::sync::Mutex;
use std::rc::Rc;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

/// Available RPC methods
pub mod ext {
    use super::*;
    use std::collections::HashMap;
    service! {
        /// Query the given `view` for all records whose columns match the given values.
        ///
        /// If `args = None`, all records are returned. Otherwise, all records are returned whose
        /// `i`th column matches the value contained in `args[i]` (or any value if `args[i] =
        /// None`).
        rpc query(view: usize, key: DataType) -> Vec<Vec<DataType>> | ();

        /// Insert a new record into the given view.
        ///
        /// `args` gives the column values for the new record.
        rpc insert(view: usize, args: Vec<DataType>) -> i64;

        /// List all available views, their names, and whether they are writeable.
        rpc list() -> HashMap<String, (usize, bool)>;
    }

    /// Construct a new `Server` handle for all Soup endpoints
    pub fn make_server(soup: &flow::Blender) -> Server {
        // Figure out what inputs and outputs to expose
        let ins = soup.inputs()
            .into_iter()
            .map(|(ni, n)| {
                     (ni.as_global().index(),
                      (n.name().to_owned(),
                       n.fields().iter().cloned().collect(),
                       Mutex::new(soup.get_mutator(ni))))
                 })
            .collect();
        let outs = soup.outputs()
            .into_iter()
            .map(|(ni, n, r)| {
                     (ni.as_global().index(),
                      (n.name().to_owned(),
                       n.fields().iter().cloned().collect(),
                       r.get_reader().unwrap()))
                 })
            .collect();

        Server {
            put: ins,
            get: outs,
        }
    }

    /// A handle to Soup put and get endpoints
    pub struct Server {
        /// All put endpoints.
        pub put: VecMap<(String, Vec<String>, Mutex<flow::Mutator>)>,
        /// All get endpoints.
        pub get: VecMap<(String, Vec<String>, Get)>,
    }

    /// Handle RPCs from a single `TcpStream`
    pub fn main(stream: TcpStream, s: Server) -> ! {
        use tarpc::tokio_proto::BindServer;
        use tokio_core;

        let mut reactor = reactor::Core::new().unwrap();
        let h = reactor.handle();

        let stream = tokio_core::net::TcpStream::from_stream(stream, &h).unwrap();
        let stream = tarpc::stream_type::StreamType::Tcp(stream);
        let srv = ext::TarpcNewService(Rc::new(s));
        tarpc::protocol::Proto::new(2 << 20).bind_server(&h, stream, srv);
        loop {
            reactor.turn(None)
        }
    }
}

type Get = Box<Fn(&DataType) -> Result<Vec<Vec<DataType>>, ()> + Send>;

impl ext::FutureService for Rc<ext::Server> {
    type QueryFut = futures::future::FutureResult<Vec<Vec<DataType>>, ()>;
    fn query(&self, view: usize, key: DataType) -> Self::QueryFut {
        let get = &self.get[view];
        futures::future::result(get.2(&key))
    }

    type InsertFut = futures::Finished<i64, Never>;
    fn insert(&self, view: usize, args: Vec<DataType>) -> Self::InsertFut {
        self.put[view].2.lock().unwrap().put(args);
        futures::finished(0)
    }

    type ListFut = futures::Finished<HashMap<String, (usize, bool)>, Never>;
    fn list(&self) -> Self::ListFut {
        futures::finished(self.get
                              .iter()
                              .map(|(ni, &(ref n, _, _))| (n.clone(), (ni.into(), false)))
                              .chain(self.put
                                         .iter()
                                         .map(|(ni, &(ref n, _, _))| {
                                                  (n.clone(), (ni.into(), true))
                                              }))
                              .collect())
    }
}

/// Starts a server which allows read/write access to the Soup using a binary protocol.
///
/// In particular, requests should all be of the form `types::Request`
pub fn run<T: Into<::std::net::SocketAddr>>(soup: flow::Blender, addr: T) {
    let listener = TcpListener::bind(addr.into()).unwrap();

    // Figure out what inputs and outputs to expose
    let mut i = 0;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let s = ext::make_server(&soup);
                thread::Builder::new()
                    .name(format!("rpc{}", i))
                    .spawn(move || {
                               stream.set_nodelay(true).unwrap();
                               ext::main(stream, s);
                           })
                    .unwrap();
                i += 1;
            }
            Err(e) => {
                print!("accept failed {:?}\n", e);
            }
        }
    }
}
