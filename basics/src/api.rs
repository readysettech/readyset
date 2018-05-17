use std::fmt;
use {BaseOperation, LocalNodeIndex};

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub src: LocalNodeIndex,
    pub dst: LocalNodeIndex,
}

impl Link {
    pub fn new(src: LocalNodeIndex, dst: LocalNodeIndex) -> Self {
        Link { src: src, dst: dst }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.src, self.dst)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Input {
    pub link: Link,
    pub data: Vec<BaseOperation>,
    // NOTE: would have to pull out TransactionState
    //pub txn: Option<TransactionState>,
}
