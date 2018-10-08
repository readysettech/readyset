use noria::{
    self, ControllerBuilder, LocalAuthority, LocalControllerHandle, NodeIndex,
    PersistenceParameters,
};

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
    pub graph: LocalControllerHandle<LocalAuthority>,
}

pub struct Setup {
    pub stupid: bool,
    pub partial: bool,
    pub sharding: Option<usize>,
    pub logging: bool,
    pub threads: Option<usize>,
}

impl Default for Setup {
    fn default() -> Self {
        Setup {
            stupid: false,
            partial: true,
            sharding: None,
            logging: false,
            threads: None,
        }
    }
}

impl Setup {
    pub fn make(&self, persistence_params: PersistenceParameters) -> Graph {
        // XXX: why isn't PersistenceParameters inside self?
        let mut g = ControllerBuilder::default();
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
        let mut graph = g.build_local().unwrap();

        graph.install_recipe(RECIPE).unwrap();
        let inputs = graph.inputs().unwrap();
        let outputs = graph.outputs().unwrap();

        if self.logging {
            println!("inputs {:?}", inputs);
            println!("outputs {:?}", outputs);
        }

        Graph {
            vote: inputs["Vote"],
            article: inputs["Article"],
            end: outputs["ArticleWithVoteCount"],
            stupid: self.stupid,
            graph,
        }
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
