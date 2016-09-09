use rustful::{Server, Handler, Context, Response, TreeRouter, HttpResult};
use rustful::server::Listening;
use flow::{FlowGraph, NodeIndex, FillableQuery};
use query::DataType;
use petgraph::EdgeDirection;
use std::fmt::Debug;

struct Endpoint {
    node: NodeIndex,
    arguments: Vec<String>,
}

pub fn run<Q, U, P>(mut soup: FlowGraph<Q, U, Vec<DataType>, P>) -> HttpResult<Listening>
    where Q: 'static + FillableQuery<Params = P> + Clone + Debug + Send + Sync,
          U: 'static + Clone + Send + From<Vec<DataType>>,
          P: 'static + Send
{
    let mut router = TreeRouter::new();

    let (ins, outs) = {
        let (graph, source) = soup.graph();
        let ni2ep = |ni| {
            let ns = graph.node_weight(ni).unwrap().as_ref().unwrap();
            (ns.name().to_owned(),
             Endpoint {
                node: ni,
                arguments: ns.args().iter().cloned().collect(),
            })
        };

        // this maps the base nodes to inputs
        // and the leaves to outputs
        // TODO: we may want to allow non-leaves to be outputs too
        (graph.neighbors(source).map(&ni2ep).collect::<Vec<_>>(),
         graph.externals(EdgeDirection::Outgoing).map(&ni2ep).collect::<Vec<_>>())
    };

    let (mut put, mut get) = soup.run(10);

    for (path, ep) in ins.into_iter() {
        use std::sync::Mutex;
        let put = Mutex::new(put.remove(&ep.node).unwrap());
        insert_routes! {
            &mut router => {
                path => Post: Box::new(move |mut ctx: Context, _: Response| {
                    let json = ctx.body.read_json_body().unwrap();

                    put.lock().unwrap().send(ep.arguments.iter().map(|arg| {
                        if let Some(num) = json[&**arg].as_i64() {
                            num.into()
                        } else {
                            json[&**arg].as_string().unwrap().into()
                        }
                    }).collect::<Vec<DataType>>());
                }) as Box<Handler>,
            }
        };
    }

    for (path, ep) in outs.into_iter() {
        let get = get.remove(&ep.node).unwrap();
        insert_routes! {
            &mut router => {
                path => Get: Box::new(move |_: Context, mut res: Response| {
                    use rustc_serialize::json::ToJson;
                    use rustful::header::ContentType;
                    res.headers_mut().set(ContentType::json());
                    res.send(format!("{}", get(None).to_json()));
                    // let q = Query::new(&[true, true, true],
                    // vec![shortcut::Condition {
                    // column: 0,
                    // cmp:
                    // shortcut::Comparison::Equal(shortcut::Value::Const(id.into())),
                    // }]);
                    // let res: Vec<_> = getter(Some(q));
                    //
                }) as Box<Handler>,
            }
        };
    }

    Server {
            handlers: router,
            host: 8080.into(),
            ..Server::default()
        }
        .run()
}
