extern crate distributary;

use std::{thread, time};
use std::collections::HashMap;

use distributary::{Blender, DataType, Mutator, Recipe};

type Datas = Vec<Vec<DataType>>;
type Getter = Box<Fn(&DataType) -> Result<Datas, ()> + Send + Sync>;

struct Backend {
    getters: HashMap<String, Getter>,
    mutators: HashMap<String, Mutator>,
    recipe: Recipe,
    soup: Blender,
}

impl Backend {
    fn put(&mut self, kind: &str, data: Vec<DataType>) -> Result<(), String> {
        let mtr = self.mutators
            .entry(String::from(kind))
            .or_insert(self.soup.get_mutator(self.recipe.node_addr_for(kind)?));

        mtr.put(data);
        Ok(())
    }

    fn get(&mut self, kind: &str, key: DataType) -> Result<Datas, String> {
        let get_fn = self.getters
            .entry(String::from(kind))
            .or_insert(self.soup.get_getter(self.recipe.node_addr_for(kind)?).unwrap());

        match get_fn(&key) {
            Ok(records) => Ok(records),
            Err(_) => Err(format!("GET for {} failed!", kind)),
        }
    }
}

fn load_recipe() -> Result<Box<Backend>, String> {
    // inline recipe definition
    let sql = "# write types (SQL type names are ignored)
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # read expressions
               VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               ArticleWithVoteCount: SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    // set up Soup via recipe
    let mut soup = Blender::new();
    let recipe;
    {
        let mut mig = soup.start_migration();

        // remove comment lines
        let lines: Vec<String> = sql.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(String::from)
            .collect();
        let rp_txt = lines.join("\n");

        // install recipe
        recipe = match Recipe::from_str(&rp_txt) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig)?;
                recipe
            }
            Err(e) => return Err(e),
        };

        // Commit the migration (brings up new graph)
        mig.commit();
    }

    Ok(Box::new(Backend {
        getters: HashMap::default(),
        mutators: HashMap::default(),
        recipe: recipe,
        soup: soup,
    }))
}

fn main() {
    let mut backend = load_recipe().unwrap();

    // println!("Soup graph:\n{}", backend.soup);

    println!("Writing...");
    let aid = 1;
    let title = "test title";
    let url = "http://pdos.csail.mit.edu";
    let uid = 42;
    backend.put("Article", vec![aid.into(), title.into(), url.into()]).unwrap();
    backend.put("Vote", vec![aid.into(), uid.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", backend.get("ArticleWithVoteCount", aid.into()))
}
