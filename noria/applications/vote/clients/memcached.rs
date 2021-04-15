use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use anyhow::anyhow;
use clap;
use memcached;
use memcached::proto::{MultiOperation, ProtoType};
use std::future::Future;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tower_service::Service;

enum Req {
    Populate(Vec<(String, String, String)>),
    Write(Vec<String>),
    Read(bool, Vec<String>),
}

pub(crate) struct Conn {
    c: mpsc::Sender<(Req, oneshot::Sender<memcached::proto::MemCachedResult<()>>)>,
    _jh: std::thread::JoinHandle<()>,
    addr: String,
    fast: bool,
}

impl Clone for Conn {
    fn clone(&self) -> Self {
        // FIXME: this is called once per load _generator_
        // In most cases, we'll have just one generator, and hence only one connection...
        Conn::new(&self.addr, self.fast).unwrap()
    }
}

impl Conn {
    fn new(addr: &str, fast: bool) -> memcached::proto::MemCachedResult<Self> {
        eprintln!(
            "OBS OBS OBS: the memcached client is effectively single-threaded -- see src FIXME"
        );

        let connstr = format!("tcp://{}", addr);
        let (reqs, mut rx) =
            mpsc::channel::<(Req, oneshot::Sender<memcached::proto::MemCachedResult<()>>)>(1);

        let jh = std::thread::spawn(move || {
            let mut c = memcached::Client::connect(&[(&connstr, 1)], ProtoType::Binary).unwrap();

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            while let Some((op, ret)) = rt.block_on(rx.recv()) {
                let res = match op {
                    Req::Populate(articles) => {
                        use std::collections::BTreeMap;

                        let vc = b"0";
                        let mut m = BTreeMap::new();
                        for &(ref tkey, ref title, ref vck) in &articles {
                            m.insert(tkey.as_bytes(), (title.as_bytes(), 0, 0));
                            m.insert(vck.as_bytes(), (&vc[..], 0, 0));
                        }
                        c.set_multi(m).map(|_| ())
                    }
                    Req::Write(keys) => {
                        use std::collections::HashMap;
                        let ids: HashMap<_, _> =
                            keys.iter().map(|key| (key.as_bytes(), (1, 0, 0))).collect();
                        //self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
                        drop(c.increment_multi(ids));
                        Ok(())
                    }
                    Req::Read(fast, keys) => {
                        let mut rows = 0;
                        let nkeys = keys.len();
                        let keys: Vec<_> = keys.iter().map(|k| k.as_bytes()).collect();
                        c.get_multi(&keys[..]).map(|vals| {
                            if fast {
                                // fast -- didn't fetch titles
                                for key in keys {
                                    if vals.get(key).is_some() {
                                        rows += 1;
                                    }
                                }
                            } else {
                                // slow -- also fetched titles
                                for (i, key) in keys.iter().enumerate() {
                                    if i % 2 == 1 {
                                        // already read
                                        continue;
                                    }

                                    // title (vc has key i+1)
                                    match (vals.get(&**key), vals.get(keys[i + 1])) {
                                        (Some(_), Some(_)) => {
                                            rows += 1;
                                        }
                                        _ => {}
                                    }
                                }
                            }

                            if fast {
                                assert_eq!(rows, nkeys);
                            } else {
                                assert_eq!(rows, nkeys / 2);
                            }
                        })
                    }
                };
                ret.send(res).unwrap();
            }
        });

        Ok(Conn {
            c: reqs,
            _jh: jh,
            fast,
            addr: addr.to_string(),
        })
    }
}

impl VoteClient for Conn {
    type Future = impl Future<Output = Result<Self, anyhow::Error>> + Send;
    fn new(params: Parameters, args: clap::ArgMatches<'_>) -> <Self as VoteClient>::Future {
        let addr = args.value_of("address").unwrap();
        let fast = args.is_present("fast");
        let conn = Conn::new(addr, fast);

        async move {
            let conn = conn?;

            if params.prime {
                // prepop
                let mut aid = 1;
                let bs = 1000;
                assert_eq!(params.articles % bs, 0);
                for _ in 0..params.articles / bs {
                    let articles: Vec<_> = (0..bs)
                        .map(|i| {
                            let article_id = aid + i;
                            (
                                format!("article_{}", article_id),
                                format!("Article #{}", article_id),
                                format!("article_{}_vc", article_id),
                            )
                        })
                        .collect();

                    let (tx, rx) = oneshot::channel();
                    conn.c
                        .send((Req::Populate(articles), tx))
                        .await
                        .unwrap_or_else(|_| panic!("SendError"));
                    rx.await.unwrap().unwrap();

                    aid += bs;
                }
            }

            Ok(conn)
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = anyhow::Error;
    type Future = impl Future<Output = Result<(), anyhow::Error>> + Send;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let keys: Vec<_> = if self.fast {
            // fast -- don't fetch titles
            req.0
                .into_iter()
                .flat_map(|article_id| vec![format!("article_{}_vc", article_id)])
                .collect()
        } else {
            // slow -- also fetch titles
            req.0
                .into_iter()
                .flat_map(|article_id| {
                    vec![
                        format!("article_{}", article_id),
                        format!("article_{}_vc", article_id),
                    ]
                })
                .collect()
        };

        let (tx, rx) = oneshot::channel();
        let res = self.c.try_send((Req::Read(self.fast, keys), tx));

        async move {
            if let Err(_) = res {
                anyhow!("backing thread failed for read");
            }

            rx.await.unwrap()?;
            Ok(())
        }
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = anyhow::Error;
    type Future = impl Future<Output = Result<(), anyhow::Error>> + Send;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let keys: Vec<_> = req
            .0
            .into_iter()
            .map(|article_id| format!("article_{}_vc", article_id))
            .collect();

        let (tx, rx) = oneshot::channel();
        let res = self.c.try_send((Req::Write(keys), tx));

        async move {
            if let Err(_) = res {
                anyhow!("backing thread failed for write");
            }

            rx.await.unwrap()?;
            Ok(())
        }
    }
}
