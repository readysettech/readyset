use std::collections::HashMap;
use std::convert::TryFrom;

use noria_data::DataType;
use rand::prelude::*;

const CLASSES_PER_STUDENT: usize = 5;
pub const TAS_PER_CLASS: usize = 5;

pub struct Populate {
    nposts: i32,
    nusers: i32,
    nclasses: i32,
    private: f32,
    rng: ThreadRng,
    students: HashMap<DataType, Vec<DataType>>,
    tas: HashMap<DataType, Vec<DataType>>,
}

impl Populate {
    pub fn new(nposts: i32, nusers: i32, nclasses: i32, private: f32) -> Populate {
        Populate {
            nposts,
            nusers,
            nclasses,
            private,
            rng: rand::thread_rng(),
            students: HashMap::new(),
            tas: HashMap::new(),
        }
    }

    pub fn enroll_students(&mut self) {
        println!("Enrolling students...");
        for i in 0..self.nusers {
            let mut classes: Vec<DataType> = Vec::new();
            while classes.len() < CLASSES_PER_STUDENT && (classes.len() as i32 != self.nclasses) {
                let cid = self.cid();
                if !classes.contains(&cid) {
                    classes.push(cid.clone());
                }
            }

            self.students.insert(i.into(), classes);
        }

        println!("Enrolling TAs...");
        for i in 0..self.nclasses {
            let mut tas: Vec<DataType> = Vec::new();

            while tas.len() < TAS_PER_CLASS && (tas.len() as i32 != self.nusers) {
                let uid = self.uid();
                if !tas.contains(&uid) {
                    tas.push(uid.clone());
                }
            }

            self.tas.insert(i.into(), tas);
        }
    }

    pub fn get_roles(&mut self) -> Vec<Vec<DataType>> {
        println!("Populating roles...");
        let mut records = Vec::new();
        // add tas
        // we populate in this order so that each batch of writes
        // as many cids as possible. this is important for the
        // trigger node, since it creates one thread per batch
        // if the batch has a new id. having the same cid in one
        // batch would mean we create a new thread for each cid,
        // instead of one thread per batch of cid.
        for i in 0..TAS_PER_CLASS {
            for (cid, tas) in self.tas.iter() {
                let uid = tas[i].clone();
                let cid = cid.clone();
                let role = 1.into(); // ta
                records.push(vec![uid, cid, role]);
            }
        }

        // add students
        for (uid, classes) in self.students.iter() {
            for cid in classes {
                let uid = uid.clone();
                let cid = cid.clone();
                let role = 0.into(); // student
                records.push(vec![uid, cid, role]);
            }
        }

        records
    }

    pub fn get_users(&mut self) -> Vec<Vec<DataType>> {
        println!("Populating users...");
        let mut records = Vec::new();
        for i in 0..self.nusers {
            let uid = i.into();
            records.push(vec![uid]);
        }

        records
    }

    pub fn get_posts(&mut self) -> Vec<Vec<DataType>> {
        println!("Populating posts...");
        let mut records = Vec::new();
        for i in 0..self.nposts {
            let pid = i.into();
            let author = self.uid();
            let cid = self.cid_for(&author);
            // It is safe to transform an empty String into a DataType.
            #[allow(clippy::unwrap_used)]
            let content = DataType::try_from("").unwrap();
            let private = self.private();
            let anon = 1.into();
            records.push(vec![pid, cid, author, content, private, anon]);
        }

        records
    }

    pub fn get_classes(&mut self) -> Vec<Vec<DataType>> {
        println!("Populating classes...");
        let mut records = Vec::new();
        for i in 0..self.nclasses {
            let cid = i.into();
            records.push(vec![cid]);
        }

        records
    }

    /// Generate random uid within bounds
    fn uid(&mut self) -> DataType {
        self.rng.gen_range(0, self.nusers).into()
    }

    /// Generate random cid within bounds
    fn cid(&mut self) -> DataType {
        self.rng.gen_range(0, self.nclasses).into()
    }

    fn cid_for(&mut self, uid: &DataType) -> DataType {
        self.students[uid]
            .as_slice()
            .choose(&mut self.rng)
            .unwrap()
            .clone()
    }

    #[allow(dead_code)]
    /// Generate random pid within bounds
    fn pid(&mut self) -> DataType {
        self.rng.gen_range(0, self.nposts).into()
    }

    fn private(&mut self) -> DataType {
        let m: f32 = self.rng.gen_range(0.0, 1.0);
        ((m < self.private) as i32).into()
    }
}
