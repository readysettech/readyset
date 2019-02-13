use noria::{self, LocalAuthority, NodeIndex, PersistenceParameters, SyncHandle};
use tokio::prelude::*;

pub(crate) const RECIPE: &str = "# base tables
CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
CREATE TABLE Vote (article_id int, user int);

# read queries
QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
            FROM Article \
            LEFT JOIN (SELECT Vote.article_id, COUNT(user) AS votes \
                       FROM Vote GROUP BY Vote.article_id) AS VoteCount \
            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;";

pub struct Graph {
    stupid: bool,
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: SyncHandle<LocalAuthority>,
}

pub struct Builder {
    pub stupid: bool,
    pub partial: bool,
    pub sharding: Option<usize>,
    pub logging: bool,
    pub threads: Option<usize>,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            stupid: false,
            partial: true,
            sharding: None,
            logging: false,
            threads: None,
        }
    }
}

impl Builder {
    #[allow(unused)]
    pub fn start_sync(
        &self,
        persistence_params: PersistenceParameters,
    ) -> Result<Graph, failure::Error> {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let mut g = rt.block_on(self.start(rt.executor(), persistence_params))?;
        g.graph.wrap_rt(rt);
        Ok(g)
    }

    #[allow(unused)]
    pub fn start(
        &self,
        ex: tokio::runtime::TaskExecutor,
        persistence_params: PersistenceParameters,
    ) -> impl Future<Item = Graph, Error = failure::Error> {
        // XXX: why isn't PersistenceParameters inside self?
        let mut g = noria::Builder::default();
        if !self.partial {
            g.disable_partial();
        }
        g.set_sharding(self.sharding);
        g.set_persistence(persistence_params);
        if self.logging {
            g.log_with(noria::logger_pls());
        }
        if let Some(threads) = self.threads {
            g.set_threads(threads);
        }

        let graph = g
            .start_local()
            .map(move |wh| SyncHandle::from_executor(ex, wh));

        let logging = self.logging;
        let stupid = self.stupid;
        graph
            .and_then(|mut graph| graph.handle().install_recipe(RECIPE).map(move |_| graph))
            .and_then(|mut graph| graph.handle().inputs().map(move |x| (graph, x)))
            .and_then(|(mut graph, inputs)| {
                graph.handle().outputs().map(move |x| (graph, inputs, x))
            })
            .inspect(move |(_, inputs, outputs)| {
                if logging {
                    println!("inputs {:?}", inputs);
                    println!("outputs {:?}", outputs);
                }
            })
            .map(move |(graph, inputs, outputs)| Graph {
                vote: inputs["Vote"],
                article: inputs["Article"],
                end: outputs["ArticleWithVoteCount"],
                stupid,
                graph,
            })
    }
}

impl Graph {
    #[allow(dead_code)]
    pub fn transition(&mut self) {
        let stupid_recipe = "# base tables
               CREATE TABLE Rating (article_id int, user int, stars int);

               U: SELECT article_id, stars FROM Rating UNION SELECT article_id, 1 AS stars FROM Vote;
               Total: SELECT article_id, SUM(U.stars) AS score \
                           FROM U \
                           GROUP BY article_id;
               QUERY ArticleWithScore: SELECT Article.id, title, Total.score AS score \
                            FROM Article \
                            LEFT JOIN Total ON (Article.id = Total.article_id) \
                            WHERE Article.id = ?;";

        let smart_recipe = "# base tables
               CREATE TABLE Rating (article_id int, user int, stars int);

               RatingSum: SELECT article_id, SUM(Rating.stars) AS score FROM Rating GROUP BY article_id;
               U: SELECT article_id, score FROM RatingSum UNION SELECT article_id, votes AS score FROM VoteCount;
               Score: SELECT U.article_id, SUM(U.score) AS score \
                            FROM U GROUP BY U.article_id;
               QUERY ArticleWithScore: SELECT Article.id, title, Score.score \
                            FROM Article LEFT JOIN Score ON (Article.id = Score.article_id) \
                            WHERE Article.id = ?;";

        if self.stupid {
            self.graph.extend_recipe(stupid_recipe).unwrap();
        } else {
            self.graph.extend_recipe(smart_recipe).unwrap();
        }
    }
}
