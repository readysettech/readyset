use ops;

pub struct Record {
    kind: String,
    values: Arc<Vec<Option<String>>>,
}

impl Indexable for Record {
    fn index(&self, i: usize) -> Option<&String> {
        self.values[i].as_ref()
    }
}

pub struct Condition {}

impl Condition {
    pub fn matches(&self, other: &Record) -> bool {}
}

pub struct Query {
    select: Vec<Option<String>>,
    having: Vec<Vec<Condition>>,
}

impl Query {
    pub fn new<S: Into<String>>(s: &[Option<S>], h: Vec<Vec<Condition>>) -> Query {
        Query {
            select: s.map(|s| s.and_then(|s| Some(s.into()))).collect(),
            having: h,
        }
    }

    pub fn over<T>(self, n: GraphNode<T>) -> QueryOver<T> {
        (q, n)
    }

    pub fn resolve<'a>(&self, field: &str, values: &[&'a str]) -> Option<&'a str> {
        self.select.iter().position(|s| s.any(|f| *f == field)).and_then(|fi| values[fi])
    }
}

impl Selector {
    pub fn process(&self, other: Cow<Record>) -> Option<Record> {}
}
