use crate::controller::ControllerState;
use crate::coordination::{CoordinationMessage, CoordinationPayload};
use async_bincode::AsyncBincodeReader;
use futures::sync::mpsc::UnboundedSender;
use futures::{self, Future, Sink, Stream};
use hyper::{self, header::CONTENT_TYPE, Method, StatusCode};
use noria::consensus::Authority;
use noria::ControllerDescriptor;
use rand;
use slog;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time;
use stream_cancel::{Valve, Valved};
use tokio;
use tokio::prelude::future::Either;
use tokio::prelude::*;
use tokio_io_pool;

use crate::handle::Handle;
use crate::Config;

#[allow(clippy::large_enum_variant)]
crate enum Event {
    InternalMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Option<String>,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<Result<String, String>, StatusCode>>,
    ),
    LeaderChange(ControllerState, ControllerDescriptor),
    WonLeaderElection(ControllerState),
    CampaignError(failure::Error),
    #[cfg(test)]
    IsReady(futures::sync::oneshot::Sender<bool>),
    ManualMigration {
        f: Box<dyn FnOnce(&mut crate::controller::migrate::Migration) + Send + 'static>,
        done: futures::sync::oneshot::Sender<()>,
    },
}

use std::fmt;
impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Event::InternalMessage(ref cm) => write!(f, "Internal({:?})", cm),
            Event::ExternalRequest(ref m, ref path, ..) => write!(f, "Request({} {})", m, path),
            Event::LeaderChange(..) => write!(f, "LeaderChange(..)"),
            Event::WonLeaderElection(..) => write!(f, "Won(..)"),
            Event::CampaignError(ref e) => write!(f, "CampaignError({:?})", e),
            #[cfg(test)]
            Event::IsReady(..) => write!(f, "IsReady"),
            Event::ManualMigration { .. } => write!(f, "ManualMigration{{..}}"),
        }
    }
}

/// Start up a new instance and return a handle to it. Dropping the handle will stop the
/// instance. Make sure that this method is run while on a runtime.
pub(super) fn start_instance<A: Authority + 'static>(
    authority: Arc<A>,
    listen_addr: IpAddr,
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    log: slog::Logger,
) -> impl Future<Item = Handle<A>, Error = failure::Error> {
    let mut pool = tokio_io_pool::Builder::default();
    pool.name_prefix("io-worker-");
    if let Some(threads) = config.threads {
        pool.pool_size(threads);
    }
    let iopool = pool.build().unwrap();

    let (trigger, valve) = Valve::new();
    let (tx, rx) = futures::sync::mpsc::unbounded();

    let v = try {
        // we'll be listening for a couple of different types of events:
        // first, events from workers
        let wport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0))?;
        let waddr = wport.local_addr()?;
        // second, messages from the "real world"
        let xport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 6033))
            .or_else(|_| tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0)))?;
        let xaddr = xport.local_addr()?;
        // and third, domain control traffic. this traffic is a little special, since we may need to
        // receive from it while handling control messages (e.g., for replay acks). because of this, we
        // give it its own channel.
        let cport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0))?;
        let caddr = cport.local_addr()?;
        ((wport, waddr), (xport, xaddr), (cport, caddr))
    };
    let ((wport, waddr), (xport, xaddr), (cport, caddr)) = match v {
        Ok(v) => v,
        Err(e) => return future::Either::A(future::err(e)),
    };

    // set up different loops for the controller "part" and the worker "part" of us. this is
    // necessary because sometimes the two need to communicate (e.g., for migrations), and if they
    // were in a single loop, that could deadlock.
    let (ctrl_tx, ctrl_rx) = futures::sync::mpsc::unbounded();
    let (worker_tx, worker_rx) = futures::sync::mpsc::unbounded();

    // spawn all of those
    tokio::spawn(listen_internal(&valve, log.clone(), tx.clone(), wport));
    let ext_log = log.clone();
    tokio::spawn(
        listen_external(tx.clone(), valve.wrap(xport.incoming()), authority.clone()).map_err(
            move |e| {
                warn!(ext_log, "external request failed: {:?}", e);
            },
        ),
    );

    // first, a loop that just forwards to the appropriate place
    tokio::spawn(
        rx.map_err(|_| unreachable!())
            .fold((ctrl_tx, worker_tx), move |(ctx, wtx), e| {
                let fw = move |e, to_ctrl| {
                    if to_ctrl {
                        Either::A(ctx.send(e).map(move |ctx| (ctx, wtx)))
                    } else {
                        Either::B(wtx.send(e).map(move |wtx| (ctx, wtx)))
                    }
                };

                match e {
                    Event::InternalMessage(ref msg) => match msg.payload {
                        CoordinationPayload::Deregister => fw(e, true),
                        CoordinationPayload::RemoveDomain => fw(e, false),
                        CoordinationPayload::AssignDomain(..) => fw(e, false),
                        CoordinationPayload::DomainBooted(..) => fw(e, false),
                        CoordinationPayload::Register { .. } => fw(e, true),
                        CoordinationPayload::Heartbeat => fw(e, true),
                        CoordinationPayload::CreateUniverse(..) => fw(e, true),
                    },
                    Event::ExternalRequest(..) => fw(e, true),
                    Event::ManualMigration { .. } => fw(e, true),
                    Event::LeaderChange(..) => fw(e, false),
                    Event::WonLeaderElection(..) => fw(e, true),
                    Event::CampaignError(..) => fw(e, true),
                    #[cfg(test)]
                    Event::IsReady(..) => fw(e, true),
                }
                .map_err(|e| panic!("{:?}", e))
            })
            .map(|_| ()),
    );

    let descriptor = ControllerDescriptor {
        external_addr: xaddr,
        worker_addr: waddr,
        domain_addr: caddr,
        nonce: rand::random(),
    };
    tokio::spawn(crate::controller::main(
        &valve,
        config,
        descriptor,
        ctrl_rx,
        cport,
        log.clone(),
        authority.clone(),
        tx.clone(),
    ));
    tokio::spawn(crate::worker::main(
        iopool.handle().clone(),
        worker_rx,
        listen_addr,
        waddr,
        memory_limit,
        memory_check_frequency,
        log.clone(),
    ));

    future::Either::B(Handle::new(authority, tx, trigger, iopool))
}

fn listen_internal(
    valve: &Valve,
    log: slog::Logger,
    event_tx: UnboundedSender<Event>,
    on: tokio::net::TcpListener,
) -> impl Future<Item = (), Error = ()> {
    let valve = valve.clone();
    valve
        .wrap(on.incoming())
        .map_err(failure::Error::from)
        .for_each(move |sock| {
            tokio::spawn(
                valve
                    .wrap(AsyncBincodeReader::from(sock))
                    .map(Event::InternalMessage)
                    .map_err(failure::Error::from)
                    .forward(
                        event_tx
                            .clone()
                            .sink_map_err(|_| format_err!("main event loop went away")),
                    )
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e)),
            );
            Ok(())
        })
        .map_err(move |e| {
            warn!(log, "internal connection failed: {:?}", e);
        })
}

struct ExternalServer<A: Authority>(UnboundedSender<Event>, Arc<A>);
fn listen_external<A: Authority + 'static>(
    event_tx: UnboundedSender<Event>,
    on: Valved<tokio::net::tcp::Incoming>,
    authority: Arc<A>,
) -> impl Future<Item = (), Error = hyper::Error> + Send {
    use hyper::{
        service::{NewService, Service},
        Request, Response,
    };
    impl<A: Authority> Clone for ExternalServer<A> {
        // Needed due to #26925
        fn clone(&self) -> Self {
            ExternalServer(self.0.clone(), self.1.clone())
        }
    }
    impl<A: Authority> Service for ExternalServer<A> {
        type ReqBody = hyper::Body;
        type ResBody = hyper::Body;
        type Error = hyper::Error;
        type Future = Box<dyn Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send>;

        fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
            let mut res = Response::builder();
            // disable CORS to allow use as API server
            res.header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            if let Method::GET = *req.method() {
                match req.uri().path() {
                    "/graph.html" => {
                        res.header(CONTENT_TYPE, "text/html");
                        let res = res.body(hyper::Body::from(include_str!("graph.html")));
                        return Box::new(futures::future::ok(res.unwrap()));
                    }
                    path if path.starts_with("/zookeeper/") => {
                        let res = match self.1.try_read(&format!("/{}", &path[11..])) {
                            Ok(Some(data)) => {
                                res.header(CONTENT_TYPE, "application/json");
                                res.body(hyper::Body::from(data))
                            }
                            _ => {
                                res.status(StatusCode::NOT_FOUND);
                                res.body(hyper::Body::empty())
                            }
                        };
                        return Box::new(futures::future::ok(res.unwrap()));
                    }
                    _ => {}
                }
            }

            let method = req.method().clone();
            let path = req.uri().path().to_string();
            let query = req.uri().query().map(ToOwned::to_owned);
            let event_tx = self.0.clone();
            Box::new(req.into_body().concat2().and_then(move |body| {
                let body: Vec<u8> = body.iter().cloned().collect();
                let (tx, rx) = futures::sync::oneshot::channel();
                event_tx
                    .clone()
                    .send(Event::ExternalRequest(method, path, query, body, tx))
                    .map_err(|_| futures::Canceled)
                    .then(move |_| rx)
                    .then(move |reply| match reply {
                        Ok(reply) => {
                            let res = match reply {
                                Ok(Ok(reply)) => {
                                    res.header("Content-Type", "application/json; charset=utf-8");
                                    res.body(hyper::Body::from(reply))
                                }
                                Ok(Err(reply)) => {
                                    res.status(StatusCode::INTERNAL_SERVER_ERROR);
                                    res.header("Content-Type", "text/plain; charset=utf-8");
                                    res.body(hyper::Body::from(reply))
                                }
                                Err(status_code) => {
                                    res.status(status_code);
                                    res.body(hyper::Body::empty())
                                }
                            };
                            Ok(res.unwrap())
                        }
                        Err(_) => {
                            res.status(StatusCode::NOT_FOUND);
                            Ok(res.body(hyper::Body::empty()).unwrap())
                        }
                    })
            }))
        }
    }
    impl<A: Authority> NewService for ExternalServer<A> {
        type Service = Self;
        type ReqBody = hyper::Body;
        type ResBody = hyper::Body;
        type Error = hyper::Error;
        type InitError = io::Error;
        type Future = tokio::prelude::future::FutureResult<Self, Self::InitError>;
        fn new_service(&self) -> tokio::prelude::future::FutureResult<Self, io::Error> {
            Ok(self.clone()).into()
        }
    }

    let service = ExternalServer(event_tx, authority);
    hyper::server::Server::builder(on).serve(service)
}
