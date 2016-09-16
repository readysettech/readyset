use flow::{FlowGraph, NodeIndex};
use query::{DataType, Query};
use ops::Update;

use tarpc;
use shortcut;
use clocked_dispatch;

use std::collections::HashMap;
use std::sync::Mutex;

pub mod ext {
    use query::DataType;
    use std::collections::HashMap;
    service! {
        rpc query(view: usize, args: Option<Vec<Option<DataType>>>) -> Vec<Vec<DataType>>;
        rpc insert(view: usize, args: Vec<DataType>) -> ();
        rpc list() -> HashMap<String, (usize, bool)>;
    }
}

use self::ext::*;

type Put = clocked_dispatch::ClockedSender<Vec<DataType>>;
type Get = Box<Fn(Option<Query>) -> Vec<Vec<DataType>> + Send + Sync>;
type FG = FlowGraph<Query, Update, Vec<DataType>, Vec<shortcut::Value<DataType>>>;

struct Server {
    put: HashMap<NodeIndex, (String, Vec<String>, Mutex<Put>)>,
    get: HashMap<NodeIndex, (String, Vec<String>, Get)>,
    _g: Mutex<FG>, // never read or written, just needed so the server doesn't stop
}

impl ext::Service for Server {
    fn query(&self, view: usize, args: Option<Vec<Option<DataType>>>) -> Vec<Vec<DataType>> {
        let get = &self.get[&NodeIndex::new(view)];
        let arg = args.map(|mut args| {
            use std::iter;

            let conds = get.1
                .iter()
                .enumerate()
                .filter_map(|(i, _)| {
                    args.get_mut(i).and_then(|a| a.take()).map(|arg| {
                        shortcut::Condition {
                            column: i,
                            cmp: shortcut::Comparison::Equal(shortcut::Value::Const(arg)),
                        }
                    })
                })
                .collect();

            Query::new(&iter::repeat(true).take(get.1.len()).collect::<Vec<_>>(),
                       conds)
        });

        get.2(arg)
    }

    fn insert(&self, view: usize, args: Vec<DataType>) -> () {
        self.put[&NodeIndex::new(view)].2.lock().unwrap().send(args);

    }

    fn list(&self) -> HashMap<String, (usize, bool)> {
        self.get
            .iter()
            .map(|(ni, &(ref n, _, _))| (n.clone(), (ni.index(), false)))
            .chain(self.put.iter().map(|(ni, &(ref n, _, _))| (n.clone(), (ni.index(), true))))
            .collect()
    }
}

/// Starts a server which allows read/write access to the Soup using a binary protocol.
///
/// In particular, requests should all be of the form `types::Request`
pub fn run<T>(mut soup: FG, addr: T) -> tarpc::ServeHandle<<T::Listener as tarpc::transport::Listener>::Dialer>
    where T: tarpc::transport::Transport
{
    // Figure out what inputs and outputs to expose
    let (ins, outs) = {
        let (graph, source) = soup.graph();
        let ni2ep = |ni| {
            let ns = graph.node_weight(ni).unwrap().as_ref().unwrap();
            (ni, (ns.name().to_owned(), ns.args().iter().cloned().collect()))
        };

        // this maps the base nodes to inputs and other nodes to outputs
        (graph.neighbors(source).map(&ni2ep).collect::<Vec<_>>(),
         graph.node_indices()
            .filter(|ni| {
                let nw = graph.node_weight(*ni);
                nw.is_some() && nw.unwrap().as_ref().is_some()
            })
            .map(&ni2ep)
            .collect::<Vec<_>>())
    };

    // Start Soup
    let (mut put, mut get) = soup.run(10);

    let s = Server {
        put: ins.into_iter()
            .map(|(ni, (nm, args))| (ni, (nm, args, Mutex::new(put.remove(&ni).unwrap()))))
            .collect(),
        get: outs.into_iter()
            .map(|(ni, (nm, args))| (ni, (nm, args, get.remove(&ni).unwrap())))
            .collect(),
        _g: Mutex::new(soup),
    };

    s.spawn(addr).unwrap()
}
