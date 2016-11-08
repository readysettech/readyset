#[macro_use]
extern crate log;
extern crate env_logger;
extern crate afterparty;
extern crate hyper;

mod taste;

use afterparty::{Delivery, Event, Hub};

use hyper::Server;

pub fn main() {
    env_logger::init().unwrap();
    let addr = format!("0.0.0.0:{}", 4567);

    let mut hub = Hub::new();
    hub.handle("push", |delivery: &Delivery| {
        match delivery.payload {
            Event::Push { ref commits, ref pusher, .. } => {
                println!("{} {}", pusher.name, commits.len());
                for c in commits.iter() {
                    taste::taste_commit(&c.id);
                }
            }
            _ => (),
        }
    });

    let srvc = Server::http(&addr[..])
        .unwrap()
        .handle(hub);
    println!("listening on {}", addr);
    srvc.unwrap();
}
