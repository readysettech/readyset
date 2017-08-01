use rustful::{Server, Handler, Context, Response, TreeRouter, HttpResult};
use rustful::server::Listening;
use rustful::server::Global;
use slog::Logger;
use std::sync::{Arc, Mutex};

use distributary::{Blender, Recipe};

pub fn run(soup: Arc<Mutex<Blender>>, log: Logger) -> HttpResult<Listening> {
    use rustful::header::ContentType;

    let mut router = TreeRouter::new();

    insert_routes! {
        &mut router => {
            "recipe" => Post: Box::new(move |mut ctx: Context, mut res: Response| {
                use std::io::Read;

                // read raw recipe from POST data
                let mut data = String::new();
                ctx.body.read_to_string(&mut data).unwrap();

                let mut result = Recipe::from_str(&data, None);
                match result {
                    Ok(ref mut r) => {
                        let m: &Arc<Mutex<Blender>> = ctx.global.get().unwrap();
                        let mut b = m.lock().unwrap();
                        let mut mig = b.start_migration();
                        match r.activate(&mut mig, false) {
                            Ok(ar) => {
                                info!(log, "successfully migrated to new recipe, {} new nodes",
                                      ar.new_nodes.len());
                                mig.commit();
                            },
                            Err(e) => {
                                error!(log, "failed to apply new recipe: {:?}", e);
                            },
                        }
                    },
                    Err(_) => (),
                }

                res.headers_mut().set(ContentType::json());
                res.send(json!(result.map(|r| r.version())).to_string());
            }) as Box<Handler>,
        }
    };

    insert_routes! {
        &mut router => {
            "graph" => Get: Box::new(move |ctx: Context, mut res: Response| {
                let m: &Arc<Mutex<Blender>> = ctx.global.get().unwrap();
                res.headers_mut().set(ContentType::plaintext());
                res.send(format!("{}", *m.lock().unwrap()));
            }) as Box<Handler>,
        }
    };

    Server {
        handlers: router,
        host: 8080.into(),
        global: Global::from(Box::new(soup)),
        ..Server::default()
    }.run()
}
