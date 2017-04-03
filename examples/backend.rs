use std::collections::HashMap;
use distributary::{Blender, DataType, Mutator, Recipe};

type Datas = Vec<Vec<DataType>>;
type Getter = Box<Fn(&DataType) -> Result<Datas, ()> + Send + Sync>;

pub struct Backend {
    getters: HashMap<String, Getter>,
    mutators: HashMap<String, Mutator>,
    recipe: Recipe,
    pub soup: Blender,
}

impl Backend {
    pub fn new(soup: Blender, recipe: Recipe) -> Backend {
        Backend {
            getters: HashMap::default(),
            mutators: HashMap::default(),
            recipe: recipe,
            soup: soup,
        }
    }

    pub fn put(&mut self, kind: &str, data: &[DataType]) -> Result<(), String> {
        let mtr = self.mutators
            .entry(String::from(kind))
            .or_insert(self.soup.get_mutator(self.recipe.node_addr_for(kind)?));

        mtr.put(data);
        Ok(())
    }

    pub fn get<I>(&mut self, kind: &str, key: I) -> Result<Datas, String>
        where I: Into<DataType>
    {
        let get_fn =
            self.getters
                .entry(String::from(kind))
                .or_insert(self.soup.get_getter(self.recipe.node_addr_for(kind)?).unwrap());

        match get_fn(&key.into()) {
            Ok(records) => Ok(records),
            Err(_) => Err(format!("GET for {} failed!", kind)),
        }
    }
}
