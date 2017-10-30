use std::collections::HashMap;
use distributary::{Blender, DataType, Datas, Mutator, RemoteGetter};

pub struct Backend {
    getters: HashMap<String, RemoteGetter>,
    mutators: HashMap<String, Mutator>,
    pub soup: Blender,
}

impl Backend {
    pub fn new(soup: Blender) -> Backend {
        Backend {
            getters: HashMap::default(),
            mutators: HashMap::default(),
            soup: soup,
        }
    }

    pub fn put(&mut self, kind: &str, data: &[DataType]) -> Result<(), String> {
        if !self.mutators.contains_key(kind) {
            let mtr = self.soup.get_mutator(self.soup.outputs()[kind]).unwrap();
            self.mutators.insert(kind.to_owned(), mtr);
        }

        self.mutators.get_mut(kind).unwrap().put(data).unwrap();
        Ok(())
    }

    pub fn get<I>(&mut self, kind: &str, key: I) -> Result<Datas, String>
    where
        I: Into<DataType>,
    {
        if !self.getters.contains_key(kind) {
            let getter = self.soup.get_getter(self.soup.inputs()[kind]).unwrap();
            self.getters.insert(kind.to_owned(), getter);
        }

        match self.getters
            .get_mut(kind)
            .unwrap()
            .lookup(&key.into(), true)
        {
            Ok(records) => Ok(records),
            Err(_) => Err(format!("GET for {} failed!", kind)),
        }
    }
}
