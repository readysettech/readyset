use noria::DataType;
use rand;
use rand::Rng;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;

pub struct SampleKeys {
    customer: Vec<Vec<DataType>>,
    item: Vec<Vec<DataType>>,
    order: Vec<Vec<DataType>>,
    order_line: Vec<Vec<DataType>>,
    shopping_cart: Vec<Vec<DataType>>,
    country: Vec<Vec<DataType>>,
    rng: rand::ThreadRng,
    item_write: f32,
    ol_write: f32,
}

impl SampleKeys {
    pub fn new(data_location: &str, item_write: f32, ol_write: f32) -> SampleKeys {
        let mut keys = SampleKeys {
            customer: vec![],
            item: vec![],
            order: vec![],
            order_line: vec![],
            shopping_cart: vec![],
            country: vec![],
            rng: rand::thread_rng(),
            item_write,
            ol_write,
        };

        keys.get_countries(data_location);
        keys.get_customers(data_location);
        keys.get_items(data_location);
        keys.get_orders(data_location);
        keys.get_order_lines(data_location);

        keys
    }

    pub fn generate_parameter(&mut self, query_name: &str, num: usize) -> Vec<DataType> {
        let mut params = Vec::new();
        for _ in 0..num {
            let p = match query_name {
                "getName" => self.customer_id(),
                "getBook" => self.item_id(),
                "getCustomer" => self.customer_uname(),
                "doSubjectSearch" => self.item_subject(),
                "getNewProducts" => self.item_subject(),
                "getUserName" => self.customer_id(),
                "getPassword" => self.customer_uname(),
                "getRelated1" => self.item_id(),
                "getMostRecentOrderId" => self.customer_uname(),
                "getMostRecentOrderOrder" => self.order_id(),
                "getMostRecentOrderLines" => self.ol_o_id(),
                "createEmptyCart" => self.bogus_key(),
                "addItem" => self.item_id(), // XXX(malte): dual parameter query, need SCL ID range
                "addRandomItemToCartIfNecessary" => self.shopping_cart_id(),
                "getCart" => self.shopping_cart_id(),
                "createNewCustomerMaxId" => self.bogus_key(),
                "getCDiscount" => self.customer_id(),
                "getCAddrId" => self.customer_id(),
                "getCAddr" => self.customer_id(),
                "enterAddressId" => self.country_name(),
                "enterAddressMaxId" => self.bogus_key(),
                "enterOrderMaxId" => self.bogus_key(),
                "getStock" => self.item_id(),
                "verifyDBConsistencyCustId" => self.bogus_key(),
                "verifyDBConsistencyItemId" => self.bogus_key(),
                "verifyDBConsistencyAddrId" => self.bogus_key(),
                "getBestSellers" => self.item_subject(),
                _ => unimplemented!(),
            };

            params.push(p);
        }

        params
    }

    #[allow(dead_code)]
    pub fn key_space(&mut self, query_name: &str) -> usize {
        let dedup = |mut vec: Vec<_>| -> usize {
            let set: HashSet<_> = vec.drain(..).collect(); // dedup
            set.len()
        };

        match query_name {
            "getName" => dedup(self.customer.iter().map(|x| x[0].clone()).collect()),
            "getBook" => dedup(self.item.iter().map(|x| x[0].clone()).collect()),
            "getCustomer" => dedup(self.customer.iter().map(|x| x[1].clone()).collect()),
            "doSubjectSearch" => dedup(self.item.iter().map(|x| x[1].clone()).collect()),
            "getNewProducts" => dedup(self.item.iter().map(|x| x[1].clone()).collect()),
            "getUserName" => dedup(self.customer.iter().map(|x| x[0].clone()).collect()),
            "getPassword" => dedup(self.customer.iter().map(|x| x[1].clone()).collect()),
            "getRelated1" => dedup(self.item.iter().map(|x| x[0].clone()).collect()),
            "getMostRecentOrderId" => dedup(self.customer.iter().map(|x| x[1].clone()).collect()),
            "getMostRecentOrderOrder" => dedup(self.order.iter().map(|x| x[0].clone()).collect()),
            "getMostRecentOrderLines" => {
                dedup(self.order_line.iter().map(|x| x[0].clone()).collect())
            }
            "createEmptyCart" => 0,
            // XXX(malte): dual parameter query, need SCL ID range:
            "addItem" => dedup(self.item.iter().map(|x| x[0].clone()).collect()),
            "addRandomItemToCartIfNecessary" => {
                dedup(self.shopping_cart.iter().map(|x| x[0].clone()).collect())
            }
            "getCart" => dedup(self.shopping_cart.iter().map(|x| x[0].clone()).collect()),
            "createNewCustomerMaxId" => 0,
            "getCDiscount" => dedup(self.customer.iter().map(|x| x[0].clone()).collect()),
            "getCAddrId" => dedup(self.customer.iter().map(|x| x[0].clone()).collect()),
            "getCAddr" => dedup(self.customer.iter().map(|x| x[0].clone()).collect()),
            "enterAddressId" => dedup(self.country.iter().map(|x| x[0].clone()).collect()),
            "enterAddressMaxId" => 0,
            "enterOrderMaxId" => 0,
            "getStock" => dedup(self.item.iter().map(|x| x[0].clone()).collect()),
            "verifyDBConsistencyCustId" => 0,
            "verifyDBConsistencyItemId" => 0,
            "verifyDBConsistencyAddrId" => 0,
            "getBestSellers" => dedup(self.item.iter().map(|x| x[1].clone()).collect()),
            _ => unimplemented!(),
        }
    }

    pub fn keys_size(&mut self, query_name: &str) -> usize {
        match query_name {
            "getName" => self.customer.len(),
            "getBook" => self.item.len(),
            "getCustomer" => self.customer.len(),
            "doSubjectSearch" => self.item.len(),
            "getNewProducts" => self.item.len(),
            "getUserName" => self.customer.len(),
            "getPassword" => self.customer.len(),
            "getRelated1" => self.item.len(),
            "getMostRecentOrderId" => self.customer.len(),
            "getMostRecentOrderOrder" => self.order.len(),
            "getMostRecentOrderLines" => self.order_line.len(),
            "createEmptyCart" => 0,
            "addItem" => self.item.len(),
            "addRandomItemToCartIfNecessary" => self.shopping_cart.len(),
            "getCart" => self.shopping_cart.len(),
            "createNewCustomerMaxId" => 0,
            "getCDiscount" => self.customer.len(),
            "getCAddrId" => self.customer.len(),
            "getCAddr" => self.customer.len(),
            "enterAddressId" => self.country.len(),
            "enterAddressMaxId" => 0,
            "enterOrderMaxId" => 0,
            "getStock" => self.item.len(),
            "verifyDBConsistencyCustId" => 0,
            "verifyDBConsistencyItemId" => 0,
            "verifyDBConsistencyAddrId" => 0,
            "getBestSellers" => self.item.len(),
            _ => unimplemented!(),
        }
    }

    fn get_orders(&mut self, data_location: &str) {
        let f = File::open(format!("{}/orders.tsv", data_location)).unwrap();
        let mut reader = BufReader::new(f);

        let mut s = String::new();
        while reader.read_line(&mut s).unwrap() > 0 {
            {
                let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
                let o_id = i32::from_str(fields[0]).unwrap();
                self.order.push(vec![o_id.into()]);
            }
            s.clear();
        }
    }

    fn get_items(&mut self, data_location: &str) {
        let f = File::open(format!("{}/items.tsv", data_location)).unwrap();
        let mut reader = BufReader::new(f);

        let mut s = String::new();
        while reader.read_line(&mut s).unwrap() > 0 {
            {
                let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
                let i_id = i32::from_str(fields[0]).unwrap();
                let i_subject = fields[5];
                self.item.push(vec![i_id.into(), i_subject.into()]);
            }
            s.clear();
        }
    }

    fn get_customers(&mut self, data_location: &str) {
        let f = File::open(format!("{}/customers.tsv", data_location)).unwrap();
        let mut reader = BufReader::new(f);

        let mut s = String::new();
        while reader.read_line(&mut s).unwrap() > 0 {
            {
                let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
                let c_id = i32::from_str(fields[0]).unwrap();
                let c_uname = fields[1];
                self.customer.push(vec![c_id.into(), c_uname.into()]);
            }
            s.clear();
        }
    }

    pub fn get_order_lines(&mut self, data_location: &str) {
        let f = File::open(format!("{}/order_line.data", data_location)).unwrap();
        let mut reader = BufReader::new(f);

        let mut s = String::new();
        while reader.read_line(&mut s).unwrap() > 0 {
            {
                let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
                let ol_o_id = i32::from_str(fields[1]).unwrap(); //

                self.order_line.push(vec![ol_o_id.into()]);
            }
            s.clear();
        }
    }

    fn get_countries(&mut self, data_location: &str) {
        let f = File::open(format!("{}/countries.tsv", data_location)).unwrap();
        let mut reader = BufReader::new(f);

        let mut s = String::new();
        while reader.read_line(&mut s).unwrap() > 0 {
            {
                let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
                let co_name = fields[1];
                self.country.push(vec![co_name.into()]);
            }
            s.clear();
        }
    }

    fn bogus_key(&self) -> DataType {
        0.into()
    }

    fn customer_id(&mut self) -> DataType {
        self.rng.choose(self.customer.as_slice()).unwrap()[0].clone()
    }

    fn customer_uname(&mut self) -> DataType {
        self.rng.choose(self.customer.as_slice()).unwrap()[1].clone()
    }

    fn item_id(&mut self) -> DataType {
        let nrecords = ((self.item.len() as f32) * self.item_write) as usize;
        let slice = self.item.chunks(nrecords).next().unwrap();
        self.rng.choose(slice).unwrap()[0].clone()
    }

    fn item_subject(&mut self) -> DataType {
        let nrecords = ((self.item.len() as f32) * self.item_write) as usize;
        let slice = self.item.chunks(nrecords).next().unwrap();
        self.rng.choose(slice).unwrap()[1].clone()
    }

    fn ol_o_id(&mut self) -> DataType {
        let nrecords = ((self.order_line.len() as f32) * self.ol_write) as usize;
        let slice = self.item.chunks(nrecords).next().unwrap();
        self.rng.choose(slice).unwrap()[0].clone()
    }

    fn order_id(&mut self) -> DataType {
        self.rng.choose(self.order.as_slice()).unwrap()[0].clone()
    }

    fn shopping_cart_id(&self) -> DataType {
        0.into()
    }

    fn country_name(&mut self) -> DataType {
        self.rng.choose(self.country.as_slice()).unwrap()[0].clone()
    }
}
