use crate::clients::localsoup::graph::RECIPE;
use crate::clients::{Parameters, VoteClient, VoteClientConstructor};
use clap;
use failure::Error;
use futures::Future;
use noria::{self, ControllerHandle, DataType, ZookeeperAuthority};
use tokio::runtime::current_thread::Runtime;

use std::time::Duration;
use std::{mem, thread};

pub(crate) struct Client {
    addr: String,
    c: Conn,
    r: noria::View,
    #[allow(dead_code)]
    w: noria::Table,
}

impl Client {
    fn new(addr: &str) -> Result<Self, Error> {
        let mut c = Conn::new(addr)?;
        let r = c.make_getter("ArticleWithVoteCount")?;
        let w = c.make_mutator("Vote")?;
        Ok(Client {
            addr: addr.to_string(),
            c,
            r,
            w,
        })
    }

    fn reconnect(&mut self) -> Result<(), Error> {
        let ch = self
            .c
            .rt
            .block_on(ControllerHandle::new(ZookeeperAuthority::new(&self.addr)?))?;
        let old_ch = mem::replace(&mut self.c.ch, ch);
        let res = try {
            let r = self.c.make_getter("ArticleWithVoteCount")?;
            let w = self.c.make_mutator("Vote")?;
            mem::replace(&mut self.r, r);
            mem::replace(&mut self.w, w);
        };

        if let Err(e) = res {
            mem::replace(&mut self.c.ch, old_ch);
            Err(e)
        } else {
            Ok(())
        }
    }
}

struct Conn {
    ch: ControllerHandle<ZookeeperAuthority>,
    rt: Runtime,
}

impl Conn {
    fn new(addr: &str) -> Result<Self, Error> {
        let mut rt = Runtime::new().unwrap();
        let ch = rt.block_on(ControllerHandle::new(ZookeeperAuthority::new(addr)?))?;
        Ok(Conn { ch, rt })
    }

    fn make_mutator(&mut self, base: &str) -> Result<noria::Table, Error> {
        Ok(self.rt.block_on(self.ch.table(base))?)
    }

    fn make_getter(&mut self, view: &str) -> Result<noria::View, Error> {
        Ok(self.rt.block_on(self.ch.view(view))?)
    }
}

#[derive(Clone)]
pub(crate) struct Constructor(String);

impl VoteClientConstructor for Constructor {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
        let zk = format!(
            "{}/{}",
            args.value_of("zookeeper").unwrap(),
            args.value_of("deployment").unwrap()
        );

        if params.prime {
            // for prepop, we need a mutator
            let mut c = Conn::new(&zk).unwrap();
            c.rt.block_on(c.ch.install_recipe(RECIPE)).unwrap();
            let mut m = c.make_mutator("Article").unwrap();
            let mut id = 0;
            while id < params.articles {
                let end = ::std::cmp::min(id + 1000, params.articles);
                c.rt.block_on(
                    m.perform_all(
                        (id..end).map(|i| {
                            vec![((i + 1) as i32).into(), format!("Article #{}", i).into()]
                        }),
                    ),
                )
                .unwrap();
                id = end;
            }
        }

        Constructor(zk)
    }

    fn make(&mut self) -> Self::Instance {
        Client::new(&self.0).unwrap()
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        while let Err(_) = self.w.perform_all(data.clone()).wait() {
            while self.reconnect().is_err() {
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let arg: Vec<_> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
            .collect();

        loop {
            match self.r.multi_lookup(arg.clone(), true).wait() {
                Ok(rows) => {
                    let rows = rows
                        .into_iter()
                        .map(|_rows| {
                            // TODO
                            //assert_eq!(rows.map(|rows| rows.len()), Ok(1));
                        })
                        .count();
                    assert_eq!(rows, ids.len());
                    break;
                }
                Err(_) => {
                    while self.reconnect().is_err() {
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }
    }
}
