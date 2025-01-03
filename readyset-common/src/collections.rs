use std::collections::{HashMap, HashSet, VecDeque};

use crate::Len;

macro_rules! impl_collection {
    ($ty:path) => {
        impl<T> Len for $ty {
            fn len(&self) -> usize {
                Self::len(self)
            }
        }
    };
}

impl_collection!(Vec<T>);
impl_collection!(HashSet<T>);
impl_collection!(VecDeque<T>);

macro_rules! impl_collection_kv {
    ($ty:path) => {
        impl<K, V> Len for $ty {
            fn len(&self) -> usize {
                Self::len(self)
            }
        }
    };
}

impl_collection_kv!(HashMap<K, V>);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_len() {
        let a = [1, 2, 3];

        let x = Vec::from(a);
        assert_eq!(Len::len(&x), 3);

        let x = VecDeque::from(a);
        assert_eq!(Len::len(&x), 3);

        let x = HashSet::from(a);
        assert_eq!(Len::len(&x), 3);

        let x = HashMap::from([(1, 1), (2, 2), (3, 3)]);
        assert_eq!(Len::len(&x), 3);
    }
}
