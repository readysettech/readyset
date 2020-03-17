#![feature(type_alias_impl_trait)]

use clap::value_t_or_exit;
use clap::{App, Arg};
use flurry::HashMap as ConcurrentHashMap;
use noria::{self, ControllerHandle, ZookeeperAuthority};
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time;
use tokio::sync::Mutex;
use tower_service::Service;
use trawler::{LobstersRequest, TrawlerRequest};

const SCHEMA: &'static str = include_str!("schema.sql");
const QUERIES: &'static str = include_str!("queries.sql");

struct NoriaTrawlerBuilder(Option<ZookeeperAuthority>);

type Conn = Arc<NoriaConnection>;

struct NoriaTrawler {
    noria: Arc<NoriaConnection>,
}

struct NoriaConnection {
    ch: Mutex<ControllerHandle<ZookeeperAuthority>>,
    views: ConcurrentHashMap<Cow<'static, str>, noria::View>,
    tables: ConcurrentHashMap<Cow<'static, str>, noria::Table>,
}

impl NoriaConnection {
    async fn view(&self, view: &'static str) -> Result<noria::View, failure::Error> {
        if let Some(v) = self.views.pin().get(view) {
            return Ok(v.clone());
        }

        // not there -- we'll need to lock our connection to the controller
        let mut ctrl = self.ch.lock().await;
        let handle = ctrl.view(view).await?;
        let _ = self.views.pin().insert(Cow::Borrowed(view), handle.clone());
        Ok(handle)
    }

    async fn table(&self, table: &'static str) -> Result<noria::Table, failure::Error> {
        if let Some(v) = self.tables.pin().get(table) {
            return Ok(v.clone());
        }

        // not there -- we'll need to lock our connection to the controller
        let mut ctrl = self.ch.lock().await;
        let handle = ctrl.table(table).await?;
        let _ = self
            .tables
            .pin()
            .insert(Cow::Borrowed(table), handle.clone());
        Ok(handle)
    }
}

mod endpoints;

impl Service<bool> for NoriaTrawlerBuilder {
    type Response = NoriaTrawler;
    type Error = failure::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, priming: bool) -> Self::Future {
        let zk = self.0.take().unwrap();
        Box::pin(async move {
            let mut c = ControllerHandle::new(zk).await?;

            if priming {
                c.install_recipe(SCHEMA).await?;
                let mut tbl = c.table("tags").await?;
                let tag = noria::row!(tbl, "id" => 1, "tag" => "test");
                tbl.insert(tag).await?;
                c.extend_recipe(QUERIES).await?;
            }

            let tables = ConcurrentHashMap::new();
            for (table, _) in c.inputs().await? {
                let handle = c.table(&table).await?;
                tables.pin().insert(Cow::Owned(table), handle);
            }

            Ok(NoriaTrawler {
                noria: Arc::new(NoriaConnection {
                    ch: Mutex::new(c),
                    views: ConcurrentHashMap::new(),
                    tables,
                }),
            })
        })
    }
}

impl Service<TrawlerRequest> for NoriaTrawler {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(
        &mut self,
        TrawlerRequest {
            user: acting_as,
            page: req,
            is_priming: priming,
            ..
        }: TrawlerRequest,
    ) -> Self::Future {
        let c = Arc::clone(&self.noria);
        let c = futures_util::future::ready(Ok::<_, Self::Error>(c));

        // TODO: traffic management
        // https://github.com/lobsters/lobsters/blob/master/app/controllers/application_controller.rb#L37
        /*
        let c = c.and_then(|c| {
            c.start_transaction(my::TransactionOptions::new())
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:date' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:hits' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 100 \
                         WHERE keystores.key = 'traffic:hits'",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 1521590012 \
                         WHERE keystores.key = 'traffic:date'",
                    )
                })
                .and_then(|t| t.commit())
        });
        */

        Box::pin(async move {
            let inner = async move {
                let (c, with_notifications) = match req {
                    LobstersRequest::User(uid) => endpoints::user::handle(c, acting_as, uid).await,
                    LobstersRequest::Frontpage => endpoints::frontpage::handle(c, acting_as).await,
                    LobstersRequest::Comments => endpoints::comments::handle(c, acting_as).await,
                    LobstersRequest::Recent => endpoints::recent::handle(c, acting_as).await,
                    LobstersRequest::Login => {
                        let c = c.await?;
                        let user = c
                            .view("login_1")
                            .await?
                            .lookup_first(&[format!("user{}", acting_as.unwrap()).into()], true)
                            .await?;

                        if user.is_none() {
                            let uid = acting_as.unwrap();
                            let mut tbl = c.table("users").await?;
                            let user = noria::row!(tbl, "username" => format!("user{}", uid));
                            tbl.insert(user).await?;
                        }

                        Ok((c, false))
                    }
                    LobstersRequest::Logout => Ok((c.await?, false)),
                    LobstersRequest::Story(id) => endpoints::story::handle(c, acting_as, id).await,
                    LobstersRequest::StoryVote(story, v) => {
                        endpoints::story_vote::handle(c, acting_as, story, v).await
                    }
                    LobstersRequest::CommentVote(comment, v) => {
                        endpoints::comment_vote::handle(c, acting_as, comment, v).await
                    }
                    LobstersRequest::Submit { id, title } => {
                        endpoints::submit::handle(c, acting_as, id, title, priming).await
                    }
                    LobstersRequest::Comment { id, story, parent } => {
                        endpoints::comment::handle(c, acting_as, id, story, parent, priming).await
                    }
                }?;

                // notifications
                if let Some(uid) = acting_as {
                    if with_notifications && !priming {
                        endpoints::notifications(c, uid).await?;
                    }
                }

                Ok(())
            };

            // XXX: there may be particular errors we want to ignore here that relate to there
            // being outstanding requests during exit.
            match inner.await {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            }
        })
    }
}

impl trawler::AsyncShutdown for NoriaTrawler {
    type Future = impl Future<Output = ()>;
    fn shutdown(self) -> Self::Future {
        async {}
    }
}

fn main() {
    let args = App::new("lobsters-noria")
        .version("0.1")
        .about("Benchmark a lobste.rs Rails installation using MySQL directly")
        .arg(
            Arg::with_name("scale")
                .long("scale")
                .takes_value(true)
                .default_value("1.0")
                .help("Reuest load scale factor for workload"),
        )
        .arg(
            Arg::with_name("in-flight")
                .long("in-flight")
                .takes_value(true)
                .default_value("50")
                .help("Number of allowed concurrent requests"),
        )
        .arg(
            Arg::with_name("prime")
                .long("prime")
                .help("Set if the backend must be primed with initial stories and comments."),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .takes_value(true)
                .default_value("30")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .takes_value(true)
                .default_value("10")
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("histogram")
                .long("histogram")
                .help("Use file-based serialized HdrHistograms")
                .takes_value(true)
                .long_help(
                    "If the file already exists, the existing histogram is extended.\
                     There are two histograms, written out in order: \
                     sojourn and remote.",
                ),
        )
        .arg(
            Arg::with_name("zookeeper")
                .short("z")
                .long("zookeeper")
                .takes_value(true)
                .required(true)
                .default_value("127.0.0.1:2181")
                .help("Address of Zookeeper instance"),
        )
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .default_value("trawler")
                .required(true)
                .takes_value(true)
                .help("Soup deployment ID."),
        )
        .get_matches();

    let in_flight = value_t_or_exit!(args, "in-flight", usize);
    let zk = format!(
        "{}/{}",
        args.value_of("zookeeper").unwrap(),
        args.value_of("deployment").unwrap()
    );
    let zk = ZookeeperAuthority::new(&zk).unwrap();

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(value_t_or_exit!(args, "scale", f64))
        .time(
            time::Duration::from_secs(value_t_or_exit!(args, "warmup", u64)),
            time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64)),
        )
        .in_flight(in_flight);

    if let Some(h) = args.value_of("histogram") {
        wl.with_histogram(h);
    }

    wl.run(NoriaTrawlerBuilder(Some(zk)), args.is_present("prime"));
}
