use flow::prelude::*;
use flow;

use tarpc;
use tarpc::util::Never;
use futures;
use tokio_core::reactor;

use std::collections::HashMap;
use std::sync::Mutex;
use std::rc::Rc;
use std::thread;

/// Available RPC methods
pub mod ext {
    use flow::prelude::DataType;
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
}

use self::ext::*;

type Get = Box<Fn(&DataType) -> Result<Vec<Vec<DataType>>, ()> + Send>;

struct Server {
    put: HashMap<NodeAddress, (String, Vec<String>, Mutex<flow::Mutator>)>,
    get: HashMap<NodeAddress, (String, Vec<String>, Get)>,
}

impl ext::FutureService for Rc<Server> {
    type QueryFut = futures::future::FutureResult<Vec<Vec<DataType>>, ()>;
    fn query(&self, view: usize, key: DataType) -> Self::QueryFut {
        let get = &self.get[&view.into()];
        futures::future::result(get.2(&key))
    }

    type InsertFut = futures::Finished<i64, Never>;
    fn insert(&self, view: usize, args: Vec<DataType>) -> Self::InsertFut {
        self.put[&view.into()]
            .2
            .lock()
            .unwrap()
            .put(args);
        futures::finished(0)
    }

    type ListFut = futures::Finished<HashMap<String, (usize, bool)>, Never>;
    fn list(&self) -> Self::ListFut {
        futures::finished(self.get
                              .iter()
                              .map(|(&ni, &(ref n, _, _))| (n.clone(), (ni.into(), false)))
                              .chain(self.put.iter().map(|(&ni, &(ref n, _, _))| {
                                                             (n.clone(), (ni.into(), true))
                                                         }))
                              .collect())
    }
}

/// A handle for a running RPC server.
///
/// Will terminate and wait for all server threads when dropped.
pub struct ServerHandle {
    threads: Vec<(futures::sync::oneshot::Sender<()>, thread::JoinHandle<()>)>,
    _g: flow::Blender, // never read or written, just needed so the server doesn't stop
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let wait: Vec<_> = self.threads
            .drain(..)
            .map(|(tx, jh)| {
                     tx.send(()).unwrap();
                     jh
                 })
            .collect();
        for jh in wait {
            jh.join().unwrap();
        }
    }
}

fn make_server(soup: &flow::Blender) -> Server {
    // Figure out what inputs and outputs to expose
    let ins = soup.inputs()
        .into_iter()
        .map(|(ni, n)| {
            (ni,
             (n.name().to_owned(),
              n.fields()
                  .iter()
                  .cloned()
                  .collect(),
              Mutex::new(soup.get_mutator(ni))))
        })
        .collect();
    let outs = soup.outputs()
        .into_iter()
        .map(|(ni, n, r)| {
            (ni,
             (n.name().to_owned(),
              n.fields()
                  .iter()
                  .cloned()
                  .collect(),
              r.get_reader().unwrap()))
        })
        .collect();

    Server {
        put: ins,
        get: outs,
    }
}

/// Starts a server which allows read/write access to the Soup using a binary protocol.
///
/// In particular, requests should all be of the form `types::Request`
pub fn run<T: Into<::std::net::SocketAddr>>(soup: flow::Blender,
                                            addr: T,
                                            threads: usize)
                                            -> ServerHandle {
    let addr = addr.into();
    let threads = (0..threads)
        .map(|i| {
            // Arc is just there so we can easily have Server be Clone
            let s = make_server(&soup);
            let (tx, rx) = futures::sync::oneshot::channel();
            let jh = thread::Builder::new()
                .name(format!("rpc{}", i))
                .spawn(move || {
                    let mut core = reactor::Core::new().unwrap();
                    let s = Rc::new(s);
                    let (_, handle) = s.listen(addr,
                                               &core.handle(),
                                               tarpc::future::server::Options::default())
                        .unwrap();
                    core.handle().spawn(handle);

                    match core.run(rx) {
                        Ok(_) => println!("RPC server thread quitting normally"),
                        Err(_) => println!("RPC server thread crashing and burning"),
                    }
                })
                .unwrap();
            (tx, jh)
        })
        .collect();

    ServerHandle {
        threads: threads,
        _g: soup,
    }
}
