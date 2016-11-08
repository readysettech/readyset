#[macro_use]
extern crate log;
extern crate env_logger;
extern crate afterparty;
extern crate hyper;

use afterparty::{Delivery, Event, Hub};

use hyper::Server;

pub fn main() {
    env_logger::init().unwrap();
    let addr = format!("0.0.0.0:{}", 4567);
    let mut hub = Hub::new();
    hub.handle("pull_request", |delivery: &Delivery| {
        println!("rec delivery {:#?}", delivery);
        // match delivery.payload {
        // Event::PullRequest { ref action, ref sender, .. } => {
        // println!("sender {} action {}", sender.login, action)
        // }
        // _ => (),
        // }
    });
    let srvc = Server::http(&addr[..])
        .unwrap()
        .handle(hub);
    println!("listening on {}", addr);
    srvc.unwrap();
}
