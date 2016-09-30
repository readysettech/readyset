use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;

use shortcut;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug)]
pub struct Identity {
    parent: flow::NodeIndex,
}

impl Identity {
    /// Construct a new identity operator.
    pub fn new(parent: flow::NodeIndex) -> Identity {
        Identity { parent: parent }
    }
}

impl From<Identity> for NodeType {
    fn from(b: Identity) -> NodeType {
        NodeType::IdentityNode(b)
    }
}

impl NodeOp for Identity {
    #[allow(unused_variables)]
    fn forward(&self,
               update: ops::Update,
               src: flow::NodeIndex,
               timestamp: i64,
               materialized_view: Option<&backlog::BufferedStore>,
               aqfs: &ops::AQ)
               -> Option<ops::Update> {
        Some(update)
    }

    fn query(&self, q: Option<&query::Query>, ts: i64, aqfs: &ops::AQ) -> ops::Datas {
        assert_eq!(aqfs.len(), 1);
        //assert!(aqfs.contains_key(self.parent));
        
        let args = q.unwrap().clone().having.into_iter().map(|c| {
            if let shortcut::Comparison::Equal(shortcut::Value::Const(v)) = c.cmp{
                shortcut::Value::Const(v)
            } else {
                unreachable!();
            }
        }).collect();
        aqfs[&self.parent](args, ts)
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index nothing
        HashMap::new()
    }

    #[allow(unused_variables)]
    fn resolve(&self, col: usize) -> Vec<(flow::NodeIndex, usize)> {
        vec![(self.parent, col)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    
    use ops::NodeOp;
    use std::collections::HashMap;

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let i = Identity::new(src);

        let aqfs = HashMap::new();

        let left = vec![1.into(), "a".into()];
        match i.forward(left.clone().into(), src, 0, None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(left, 0)]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let src = flow::NodeIndex::new(0);
        let i = Identity::new(src);

        let aqfs = HashMap::new();
        
    }
}
