use noria::{self, FrontierStrategy, Handle, LocalAuthority, NodeIndex, PersistenceParameters};

pub(crate) const RECIPE: &str = "# base tables
CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
CREATE TABLE Vote (article_id int, user int);

# read queries
CREATE VIEW VoteCount AS \
  SELECT Vote.article_id, COUNT(user) AS votes FROM Vote GROUP BY Vote.article_id;

QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
            FROM Article \
            LEFT JOIN VoteCount \
            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;";

pub struct Graph {
    stupid: bool,
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: Handle<LocalAuthority>,
}

pub struct Builder {
    pub stupid: bool,
    pub partial: bool,
    pub sharding: Option<usize>,
    pub logging: bool,
    pub threads: Option<usize>,
    pub purge: String,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            stupid: false,
            partial: true,
            sharding: None,
            logging: false,
            threads: None,
            purge: "none".to_string(),
        }
    }
}

impl Builder {
    #[allow(unused)]
    pub async fn start(
        &self,
        persistence_params: PersistenceParameters,
    ) -> Result<Graph, failure::Error> {
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
        match &*self.purge {
            "all" => {
                g.set_frontier_strategy(FrontierStrategy::AllPartial);
            }
            "reader" => {
                g.set_frontier_strategy(FrontierStrategy::Readers);
            }
            "none" => {}
            _ => unreachable!(),
        }

        let logging = self.logging;
        let stupid = self.stupid;
        let mut wh = g.start_local().await?;
        wh.ready().await?;
        wh.install_recipe(RECIPE).await?;
        wh.ready().await?;
        let inputs = wh.inputs().await?;
        wh.ready().await?;
        let outputs = wh.outputs().await?;

        if logging {
            println!("inputs {:?}", inputs);
            println!("outputs {:?}", outputs);
        }

        Ok(Graph {
            vote: inputs["Vote"],
            article: inputs["Article"],
            end: outputs["ArticleWithVoteCount"],
            stupid,
            graph: wh,
        })
    }
}

impl Graph {
    #[allow(dead_code)]
    pub async fn transition(&mut self) {
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
            self.graph.extend_recipe(stupid_recipe).await.unwrap();
        } else {
            self.graph.extend_recipe(smart_recipe).await.unwrap();
        }
    }
}
