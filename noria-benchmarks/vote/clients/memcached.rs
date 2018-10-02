use clap;
use crate::clients::{Parameters, VoteClient, VoteClientConstructor};
use memcached;
use memcached::proto::{MultiOperation, ProtoType};

pub struct Constructor(String, bool);
pub struct Client(memcached::Client, bool);

impl VoteClientConstructor for Constructor {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
        let addr = args.value_of("address").unwrap();
        let fast = args.is_present("fast");

        if params.prime {
            // prepop
            let mut c =
                memcached::Client::connect(&[(&format!("tcp://{}", addr), 1)], ProtoType::Binary)
                    .unwrap();

            let mut aid = 1;
            let bs = 1000;
            assert_eq!(params.articles % bs, 0);
            for _ in 0..params.articles / bs {
                use std::collections::BTreeMap;
                let articles: Vec<_> = (0..bs)
                    .map(|i| {
                        let article_id = aid + i;
                        (
                            format!("article_{}", article_id),
                            format!("Article #{}", article_id),
                            format!("article_{}_vc", article_id),
                            b"0",
                        )
                    })
                    .collect();
                let mut m = BTreeMap::new();
                for &(ref tkey, ref title, ref vck, ref vc) in &articles {
                    m.insert(tkey.as_bytes(), (title.as_bytes(), 0, 0));
                    m.insert(vck.as_bytes(), (&vc[..], 0, 0));
                }
                c.set_multi(m).unwrap();

                aid += bs;
            }
        }

        Constructor(addr.to_string(), fast)
    }

    fn make(&mut self) -> Self::Instance {
        memcached::Client::connect(&[(&format!("tcp://{}", self.0), 1)], ProtoType::Binary)
            .map(|c| Client(c, self.1))
            .unwrap()
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        use std::collections::HashMap;
        let keys: Vec<_> = ids
            .into_iter()
            .map(|article_id| format!("article_{}_vc", article_id))
            .collect();
        let ids: HashMap<_, _> = keys.iter().map(|key| (key.as_bytes(), (1, 0, 0))).collect();
        //self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
        drop(self.0.increment_multi(ids));
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let mut rows = 0;
        if self.1 {
            // fast -- don't fetch titles
            let keys: Vec<_> = ids
                .into_iter()
                .flat_map(|article_id| vec![format!("article_{}_vc", article_id)])
                .collect();
            let keys: Vec<_> = keys.iter().map(|k| k.as_bytes()).collect();

            let vals = self.0.get_multi(&keys[..]).unwrap();
            for key in keys {
                if vals.get(key).is_some() {
                    rows += 1;
                }
            }
        } else {
            // slow -- also fetch titles
            let keys: Vec<_> = ids
                .into_iter()
                .flat_map(|article_id| {
                    vec![
                        format!("article_{}", article_id),
                        format!("article_{}_vc", article_id),
                    ]
                })
                .collect();
            let keys: Vec<_> = keys.iter().map(|k| k.as_bytes()).collect();

            let vals = self.0.get_multi(&keys[..]).unwrap();
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
        assert_eq!(rows, ids.len());
    }
}
