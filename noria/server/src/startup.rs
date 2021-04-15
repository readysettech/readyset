use crate::controller::ControllerState;
use crate::coordination::{CoordinationMessage, CoordinationPayload};
use async_bincode::AsyncBincodeReader;
use futures_util::{
    future::FutureExt,
    future::TryFutureExt,
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use hyper::{self, header::CONTENT_TYPE, Method, StatusCode};
use noria::consensus::Authority;
use noria::ControllerDescriptor;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use stream_cancel::Valve;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream, UnboundedReceiverStream};

use crate::handle::Handle;
use crate::{Config, ReadySetResult};

#[allow(clippy::large_enum_variant)]
pub(crate) enum Event {
    InternalMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Option<String>,
        hyper::body::Bytes,
        tokio::sync::oneshot::Sender<Result<Result<String, String>, StatusCode>>,
    ),
    LeaderChange(ControllerState, ControllerDescriptor),
    WonLeaderElection(ControllerState),
    CampaignError(anyhow::Error),
    #[cfg(test)]
    IsReady(tokio::sync::oneshot::Sender<bool>),
    ManualMigration {
        f: Box<
            dyn FnOnce(&mut crate::controller::migrate::Migration) -> ReadySetResult<()>
                + Send
                + 'static,
        >,
        done: tokio::sync::oneshot::Sender<()>,
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
pub(super) async fn start_instance<A: Authority + 'static>(
    authority: Arc<A>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    log: slog::Logger,
) -> Result<(Handle<A>, impl Future<Output = ()> + Unpin + Send), anyhow::Error> {
    let (trigger, valve) = Valve::new();
    let (alive, done) = tokio::sync::mpsc::channel(1);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut rx = valve.wrap(UnboundedReceiverStream::new(rx));

    // we'll be listening for a couple of different types of events:
    // first, events from workers
    let wport = tokio::net::TcpListener::bind(SocketAddr::new(listen_addr, 0)).await?;
    let mut waddr = wport.local_addr()?;
    // second, messages from the "real world"
    let xport = tokio::net::TcpListener::bind(external_addr)
        .or_else(|_| tokio::net::TcpListener::bind(SocketAddr::new(listen_addr, 0)))
        .await?;
    let mut xaddr = xport.local_addr()?;
    // and third, domain control traffic. this traffic is a little special, since we may need to
    // receive from it while handling control messages (e.g., for replay acks). because of this, we
    // give it its own channel.
    let cport = tokio::net::TcpListener::bind(SocketAddr::new(listen_addr, 0)).await?;
    let mut caddr = cport.local_addr()?;

    // set up different loops for the controller "part" and the worker "part" of us. this is
    // necessary because sometimes the two need to communicate (e.g., for migrations), and if they
    // were in a single loop, that could deadlock.
    let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::unbounded_channel();
    let (worker_tx, worker_rx) = tokio::sync::mpsc::unbounded_channel();

    // spawn all of those
    tokio::spawn(listen_internal(
        alive.clone(),
        valve.clone(),
        log.clone(),
        tx.clone(),
        wport,
    ));
    let ext_log = log.clone();
    let ext_log2 = log.clone();
    let ext_log3 = log.clone();
    let ext_log4 = log.clone();
    tokio::spawn(
        listen_external(
            alive.clone(),
            valve.clone(),
            tx.clone(),
            xport,
            authority.clone(),
        )
        .map_err(move |e| {
            eprintln!("external request failed: {:?}", e);
            warn!(ext_log, "external request failed: {:?}", e);
        })
        .map(|_| ()),
    );

    // first, a loop that just forwards to the appropriate place
    let a = alive.clone();
    tokio::spawn(async move {
        let _alive = a;
        let ctx = ctrl_tx;
        let wtx = worker_tx;
        while let Some(e) = rx.next().await {
            let snd = match e {
                Event::InternalMessage(ref msg) => match msg.payload {
                    CoordinationPayload::Deregister => ctx.send(e),
                    CoordinationPayload::RemoveDomain => wtx.send(e),
                    CoordinationPayload::AssignDomain(..) => wtx.send(e),
                    CoordinationPayload::DomainBooted(..) => wtx.send(e),
                    CoordinationPayload::Register { .. } => ctx.send(e),
                    CoordinationPayload::Heartbeat => ctx.send(e),
                    CoordinationPayload::CreateUniverse(..) => ctx.send(e),
                },
                Event::ExternalRequest(..) => ctx.send(e),
                Event::ManualMigration { .. } => ctx.send(e),
                Event::LeaderChange(..) => wtx.send(e),
                Event::WonLeaderElection(..) => ctx.send(e),
                Event::CampaignError(..) => ctx.send(e),
                #[cfg(test)]
                Event::IsReady(..) => ctx.send(e),
            };
            // needed for https://gist.github.com/nikomatsakis/fee0e47e14c09c4202316d8ea51e50a0
            if let Err(e) = snd {
                eprintln!("forwarding loop failed: {:?}", e);
                warn!(ext_log2, "Forwarding loop failed (!!): {:?}", e);
                break;
            }
        }
    });

    xaddr.set_ip(external_addr.ip());
    waddr.set_ip(external_addr.ip());
    caddr.set_ip(external_addr.ip());

    let descriptor = ControllerDescriptor {
        external_addr: xaddr,
        worker_addr: waddr,
        domain_addr: caddr,
        nonce: rand::random(),
    };
    tokio::spawn(
        crate::controller::main(
            alive.clone(),
            valve,
            config,
            descriptor,
            ctrl_rx,
            cport,
            log.clone(),
            authority.clone(),
            tx.clone(),
        )
        .map_err(move |e| {
            eprintln!("controller failed: {:?}", e);
            warn!(ext_log4, "controller failed: {:?}", e);
            e
        }),
    );
    tokio::spawn(
        crate::worker::main(
            alive.clone(),
            worker_rx,
            listen_addr,
            external_addr.ip(),
            external_addr,
            waddr,
            memory_limit,
            memory_check_frequency,
            log.clone(),
        )
        .map_err(move |e| {
            eprintln!("worker domain failed: {:?}", e);
            warn!(ext_log3, "worker domain failed: {:?}", e);
            e
        }),
    );

    let h = Handle::new(authority, tx, trigger).await?;
    Ok((h, ReceiverStream::new(done).into_future().map(|_| {})))
}

async fn listen_internal(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    log: slog::Logger,
    event_tx: UnboundedSender<Event>,
    on: tokio::net::TcpListener,
) {
    let mut rx = valve.wrap(TcpListenerStream::new(on));
    while let Some(r) = { rx.next().await } {
        match r {
            Err(e) => {
                warn!(log, "internal connection failed: {:?}", e);
                return;
            }
            Ok(sock) => {
                let alive = alive.clone();
                tokio::spawn(
                    valve
                        .wrap(AsyncBincodeReader::from(sock))
                        .map_ok(|m| Event::InternalMessage(m))
                        .map_err(anyhow::Error::from)
                        .forward(
                            crate::ImplSinkForSender(event_tx.clone())
                                .sink_map_err(|_| format_err!("main event loop went away")),
                        )
                        .map_err(|e| panic!("{:?}", e))
                        .map(move |_| {
                            let _ = alive;
                            ()
                        }),
                );
            }
        }
    }
}

struct ExternalServer<A: Authority>(
    tokio::sync::mpsc::Sender<()>,
    UnboundedSender<Event>,
    Arc<A>,
);

async fn listen_external<A: Authority + 'static>(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    event_tx: UnboundedSender<Event>,
    on: tokio::net::TcpListener,
    authority: Arc<A>,
) -> Result<(), hyper::Error> {
    let on = valve.wrap(TcpListenerStream::new(on));
    use hyper::{service::make_service_fn, Body, Request, Response};
    use tower::Service;
    impl<A: Authority> Clone for ExternalServer<A> {
        // Needed due to #26925
        fn clone(&self) -> Self {
            ExternalServer(self.0.clone(), self.1.clone(), self.2.clone())
        }
    }

    impl<A: Authority> Service<Request<Body>> for ExternalServer<A> {
        type Response = Response<Body>;
        type Error = hyper::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<Body>) -> Self::Future {
            let res = Response::builder();
            // disable CORS to allow use as API server
            let res = res.header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            if let Method::GET = *req.method() {
                match req.uri().path() {
                    "/graph.html" => {
                        let res = res
                            .header(CONTENT_TYPE, "text/html")
                            .body(hyper::Body::from(include_str!("graph.html")));
                        return Box::pin(async move { Ok(res.unwrap()) });
                    }
                    path if path.starts_with("/zookeeper/") => {
                        let res = match self.2.try_read(&format!("/{}", &path[11..])) {
                            Ok(Some(data)) => res
                                .header(CONTENT_TYPE, "application/json")
                                .body(hyper::Body::from(data)),
                            _ => res.status(StatusCode::NOT_FOUND).body(hyper::Body::empty()),
                        };
                        return Box::pin(async move { Ok(res.unwrap()) });
                    }
                    _ => {}
                }
            }

            let method = req.method().clone();
            let path = req.uri().path().to_string();
            let query = req.uri().query().map(ToOwned::to_owned);
            let event_tx = self.1.clone();

            Box::pin(async move {
                let body = hyper::body::to_bytes(req.into_body()).await?;
                let (tx, rx) = tokio::sync::oneshot::channel();

                if let Err(_) = event_tx.send(Event::ExternalRequest(method, path, query, body, tx))
                {
                    let res = res
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .header("Content-Type", "text/plain; charset=utf-8");
                    return Ok(res.body(hyper::Body::from("server went away")).unwrap());
                }

                match rx.await {
                    Ok(reply) => {
                        let res = match reply {
                            Ok(Ok(reply)) => res
                                .header("Content-Type", "application/json; charset=utf-8")
                                .body(hyper::Body::from(reply)),
                            Ok(Err(reply)) => res
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .header("Content-Type", "text/plain; charset=utf-8")
                                .body(hyper::Body::from(reply)),
                            Err(status_code) => res.status(status_code).body(hyper::Body::empty()),
                        };
                        Ok(res.unwrap())
                    }
                    Err(_) => {
                        let res = res.status(StatusCode::NOT_FOUND);
                        Ok(res.body(hyper::Body::empty()).unwrap())
                    }
                }
            })
        }
    }

    let service = ExternalServer(alive, event_tx, authority);
    hyper::server::Server::builder(hyper::server::accept::from_stream(on))
        .serve(make_service_fn(move |_| {
            let s = service.clone();
            async move { io::Result::Ok(s) }
        }))
        .await
}
