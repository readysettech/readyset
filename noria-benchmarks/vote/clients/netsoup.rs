use clap;
use crate::clients::localsoup::graph::RECIPE;
use crate::clients::{Parameters, VoteClient, VoteClientConstructor};
use failure::Error;
use noria::{self, ControllerHandle, DataType, ZookeeperAuthority};

use std::thread;
use std::time::Duration;

pub(crate) struct Client {
    c: Constructor,
    r: noria::View<noria::ExclusiveConnection>,
    #[allow(dead_code)]
    w: noria::Table<noria::ExclusiveConnection>,
}

type Handle = ControllerHandle<ZookeeperAuthority>;

fn make_mutator(
    c: &mut Handle,
    view: &str,
) -> Result<noria::Table<noria::ExclusiveConnection>, Error> {
    Ok(c.table(view)?.into_exclusive()?)
}

fn make_getter(
    c: &mut Handle,
    view: &str,
) -> Result<noria::View<noria::ExclusiveConnection>, Error> {
    Ok(c.view(view)?.into_exclusive()?)
}

#[derive(Clone)]
pub(crate) struct Constructor(String);
impl Constructor {
    fn try_make(&mut self) -> Result<Client, Error> {
        let mut ch = Handle::new(ZookeeperAuthority::new(&self.0).unwrap()).unwrap();

        Ok(Client {
            c: self.clone(),
            r: make_getter(&mut ch, "ArticleWithVoteCount")?,
            w: make_mutator(&mut ch, "Vote")?,
        })
    }
}

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
            let mut ch = Handle::new(ZookeeperAuthority::new(&zk).unwrap()).unwrap();
            ch.install_recipe(RECIPE).unwrap();
            let mut m = make_mutator(&mut ch, "Article").unwrap();
            let mut id = 0;
            while id < params.articles {
                let end = ::std::cmp::min(id + 1000, params.articles);
                m.batch_insert(
                    (id..end)
                        .map(|i| vec![((i + 1) as i32).into(), format!("Article #{}", i).into()]),
                )
                .unwrap();
                id = end;
            }
        }

        Constructor(zk)
    }

    fn make(&mut self) -> Self::Instance {
        let mut ch = Handle::new(ZookeeperAuthority::new(&self.0).unwrap()).unwrap();

        Client {
            c: self.clone(),
            r: make_getter(&mut ch, "ArticleWithVoteCount").unwrap(),
            w: make_mutator(&mut ch, "Vote").unwrap(),
        }
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        while let Err(_) = self.w.insert_all(data.clone()) {
            loop {
                if let Ok(c) = self.c.try_make() {
                    *self = c;
                    break;
                }
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
            match self.r.multi_lookup(arg.clone(), true) {
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
                Err(_) => loop {
                    if let Ok(c) = self.c.try_make() {
                        *self = c;
                        break;
                    }
                    thread::sleep(Duration::from_millis(100));
                },
            }
        }
    }
}
