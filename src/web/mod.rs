use rustful::{Server, Handler, Context, Response, TreeRouter, HttpResult};
use rustful::server::Listening;
use flow::{FlowGraph, NodeIndex};
use query::{DataType, Query};
use std::collections::HashMap;
use shortcut;

struct Endpoint {
    node: NodeIndex,
    arguments: Vec<String>,
}

/// Start exposing the given `FlowGraph` over HTTP.
///
/// All base nodes are available for writing by POSTing to `localhost:8080/<view>`. Each POST
/// should contain a single JSON object representing the record with field names equal to those
/// passed to `new()`.
///
/// All nodes are available for reading by GETing from `localhost:8080/<view>`. Without further
/// arguments, the view is queried without filters. Equality filters can be added using query
/// parameters such as `?id=1`. A JSON array with all matching records is returned. Each record is
/// represented as a JSON object with field names as dictated by those passed to `new()` for the
/// view being queried.
pub fn run<U>(mut soup: FlowGraph<Query, U, Vec<DataType>>) -> HttpResult<Listening>
    where U: 'static + Clone + Send + From<Vec<DataType>>
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
         graph.node_indices()
            .filter(|ni| {
                let nw = graph.node_weight(*ni);
                nw.is_some() && nw.unwrap().as_ref().is_some()
            })
            .map(&ni2ep)
            .collect::<Vec<_>>())
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
                path => Get: Box::new(move |ctx: Context, mut res: Response| {
                    use rustc_serialize::json::ToJson;
                    use rustful::header::ContentType;

                    let mut arg = None;
                    if !ctx.query.is_empty() {
                        use std::iter;
                        let conds = ep.arguments.iter().enumerate().filter_map(|(i, arg)| {
                            if ctx.query.contains_key(arg) {
                                let arg = if let Ok(n) = ctx.query.parse(arg) {
                                    let n: i64 = n;
                                    n.into()
                                } else {
                                    ctx.query.get(arg).unwrap().into_owned().into()
                                };

                                Some(shortcut::Condition {
                                    column: i,
                                    cmp:
                                        shortcut::Comparison::Equal(shortcut::Value::Const(arg))
                                })
                            } else {
                                None
                            }
                        }).collect();
                        arg = Some(Query::new(
                                &iter::repeat(true).take(ep.arguments.len()).collect::<Vec<_>>(),
                                conds
                                ));
                    };

                    let data = get(arg).into_iter().map(|row| {
                        ep
                            .arguments
                            .clone()
                            .into_iter()
                            .zip(row.into_iter())
                            .collect::<HashMap<_, _>>()
                    }).collect::<Vec<_>>();
                    res.headers_mut().set(ContentType::json());
                    res.send(format!("{}", data.to_json()));
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
