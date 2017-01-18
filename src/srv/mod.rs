use query::DataType;
use flow::prelude::*;
use flow;

use tarpc;

use std::collections::HashMap;
use std::sync::Mutex;

/// Available RPC methods
pub mod ext {
    use query::DataType;
    use std::collections::HashMap;
    service! {
        /// Query the given `view` for all records whose columns match the given values.
        ///
        /// If `args = None`, all records are returned. Otherwise, all records are returned whose
        /// `i`th column matches the value contained in `args[i]` (or any value if `args[i] =
        /// None`).
        rpc query(view: usize, key: DataType) -> Vec<Vec<DataType>>;

        /// Insert a new record into the given view.
        ///
        /// `args` gives the column values for the new record.
        rpc insert(view: usize, args: Vec<DataType>) -> i64;

        /// List all available views, their names, and whether they are writeable.
        rpc list() -> HashMap<String, (usize, bool)>;
    }
}

use self::ext::*;

type Put = Box<Fn(Vec<DataType>) + Send + 'static>;
type Get = Box<Fn(&DataType) -> Vec<Vec<DataType>> + Send + Sync>;

struct Server {
    put: HashMap<NodeAddress, (String, Vec<String>, Mutex<Put>)>,
    get: HashMap<NodeAddress, (String, Vec<String>, Get)>,
    _g: Mutex<flow::Blender>, // never read or written, just needed so the server doesn't stop
}

impl ext::Service for Server {
    fn query(&self, view: usize, key: DataType) -> Vec<Vec<DataType>> {
        let get = &self.get[&view.into()];
        get.2(&key)
    }

    fn insert(&self, view: usize, args: Vec<DataType>) -> i64 {
        self.put[&view.into()].2.lock().unwrap()(args);
        0
    }

    fn list(&self) -> HashMap<String, (usize, bool)> {
        self.get
            .iter()
            .map(|(&ni, &(ref n, _, _))| (n.clone(), (ni.into(), false)))
            .chain(self.put.iter().map(|(&ni, &(ref n, _, _))| (n.clone(), (ni.into(), true))))
            .collect()
    }
}

/// Starts a server which allows read/write access to the Soup using a binary protocol.
///
/// In particular, requests should all be of the form `types::Request`
pub fn run<T>(soup: flow::Blender, addr: T)
    -> tarpc::ServeHandle<<T::Listener as tarpc::transport::Listener>::Dialer>
    where T: tarpc::transport::Transport
{
    // Figure out what inputs and outputs to expose
    let (ins, outs) = {
        let ins: Vec<_> = soup.inputs()
            .into_iter()
            .map(|(ni, n)| {
                (ni,
                 (n.name().to_owned(), n.fields().iter().cloned().collect(), soup.get_putter(ni).0))
            })
            .collect();
        let outs: Vec<_> = soup.outputs()
            .into_iter()
            .map(|(ni, n, r)| {
                (ni,
                 (n.name().to_owned(),
                  n.fields().iter().cloned().collect(),
                  r.get_reader().unwrap()))
            })
            .collect();
        (ins, outs)
    };

    let s = Server {
        put: ins.into_iter()
            .map(|(ni, (nm, args, putter))| (ni, (nm, args, Mutex::new(putter))))
            .collect(),
        get: outs.into_iter()
            .map(|(ni, (nm, args, getter))| (ni, (nm, args, getter)))
            .collect(),
        _g: Mutex::new(soup),
    };

    s.spawn(addr).unwrap()
}
