use rustful::{Server, Handler, Context, Response, TreeRouter, HttpResult};
use rustful::server::Listening;
use rustful::server::Global;
use std::sync::Mutex;

use flow::Blender;
use flow::prelude::DataType;
use serde_json::{self, Value};

struct GetEndpoint<F> {
    arguments: Vec<String>,
    f: F,
}

struct PutEndpoint<Mutator> {
    arguments: Vec<String>,
    mutator: Mutator,
}

/// Start exposing the given `FlowGraph` over HTTP.
///
/// All base nodes are available for writing by POSTing to `localhost:8080/<view>`. Each POST
/// should contain a single JSON object representing the record with field names equal to those
/// passed to `new()`.
///
/// All nodes are available for reading by GETing from `localhost:8080/<view>?key=<key>`. A JSON
/// array with all matching records is returned. Each record is represented as a JSON object with
/// field names as dictated by those passed to `new()` for the view being queried.
pub fn run(soup: Blender) -> HttpResult<Listening> {
    use rustful::header::ContentType;

    let mut router = TreeRouter::new();

    // Figure out what inputs and outputs to expose
    let (ins, outs) = {
        let ins: Vec<_> = soup.inputs()
            .into_iter()
            .map(|(ni, n)| {
                     (n.name().to_owned(),
                      PutEndpoint {
                          arguments: n.fields().iter().cloned().collect(),
                          mutator: soup.get_mutator(ni),
                      })
                 })
            .collect();
        let outs: Vec<_> = soup.outputs()
            .into_iter()
            .map(|(ni, n)| {
                     (n.name().to_owned(),
                      GetEndpoint {
                          arguments: n.fields().iter().cloned().collect(),
                          f: soup.get_getter(ni).unwrap(),
                      })
                 })
            .collect();
        (ins, outs)
    };

    for (path, ep) in ins.into_iter() {
        let put = Mutex::new(Box::new(ep.mutator));
        let args = ep.arguments;
        insert_routes! {
            &mut router => {
                path => Post: Box::new(move |mut ctx: Context, mut res: Response| {
                    let json = ctx.body.read_json_body().unwrap();

                    let ts: () = put.lock().unwrap().put((args.iter().map(|arg| {
                        if let Some(num) = json[&**arg].as_i64() {
                            num.into()
                        } else {
                            json[&**arg].as_string().unwrap().into()
                        }
                    })).collect::<Vec<DataType>>());
                    res.headers_mut().set(ContentType::json());
                    res.send(json!(ts).to_string()); // TODO this is always `null`
                }) as Box<Handler>,
            }
        };
    }

    for (path, ep) in outs.into_iter() {
        let get = Mutex::new(ep.f);
        let args = ep.arguments;
        insert_routes! {
            &mut router => {
                path => Get: Box::new(move |ctx: Context, mut res: Response| {
                    if let Some(key) = ctx.query.get("key") {
                        let key = if let Ok(n) = ctx.query.parse("key") {
                            let n: i64 = n;
                            n.into()
                        } else {
                            key.into_owned().into()
                        };

                        let data = get.lock().unwrap()(&key, true).into_iter().map(|row| {
                                args
                                .clone()
                                .into_iter()
                                .zip(row.iter().map(|vec| {
                                    vec.iter().map(DataType::to_json).collect()
                                }))
                                .collect::<serde_json::Map<_, _>>()
                        }).collect::<Value>();
                        res.headers_mut().set(ContentType::json());
                        res.send(data.to_string());
                    }
                }) as Box<Handler>,
            }
        };
    }

    insert_routes! {
        &mut router => {
            "graph" => Get: Box::new(move |ctx: Context, mut res: Response| {
                let m: &Mutex<Blender> = ctx.global.get().unwrap();
                res.headers_mut().set(ContentType::plaintext());
                res.send(format!("{}", *m.lock().unwrap()));
            }) as Box<Handler>,
        }
    };

    Server {
            handlers: router,
            host: 8080.into(),
            global: Global::from(Box::new(Mutex::new(soup))),
            ..Server::default()
        }
        .run()
}
