use std::cmp::Ordering;

use bit_vec::BitVec;

pub(crate) fn to_u64(vec: &BitVec) -> Option<u64> {
    match vec.len().cmp(&(u64::BITS as usize)) {
        Ordering::Greater => None,
        Ordering::Less => {
            let missing = u64::BITS as usize - vec.len();
            let mut new_vec = BitVec::from_elem(missing, false);
            new_vec.extend(vec);
            Some(u64::from_be_bytes(new_vec.to_bytes().try_into().ok()?))
        }
        Ordering::Equal => Some(u64::from_be_bytes(vec.to_bytes().try_into().ok()?)),
    }
}

pub(crate) fn to_i64(vec: &BitVec) -> Option<i64> {
    match vec.len().cmp(&(i64::BITS as usize)) {
        Ordering::Greater => None,
        Ordering::Less => {
            let missing = i64::BITS as usize - vec.len();
            let mut new_vec = BitVec::from_elem(missing, false);
            new_vec.extend(vec);
            Some(i64::from_be_bytes(new_vec.to_bytes().try_into().ok()?))
        }
        Ordering::Equal => Some(i64::from_be_bytes(vec.to_bytes().try_into().ok()?)),
    }
}

pub(crate) fn to_bytes_pad_left(vec: &BitVec) -> Vec<u8> {
    let extra = vec.len() % u8::BITS as usize;
    if extra > 0 {
        let missing = u8::BITS as usize - extra;
        let mut new_vec = BitVec::from_elem(missing, false);
        new_vec.extend(vec);
        return new_vec.to_bytes();
    };
    vec.to_bytes()
}

#[cfg(test)]
mod tests {
    use bit_vec::BitVec;

    use crate::bitvec::{to_bytes_pad_left, to_i64, to_u64};

    #[test]
    fn converts_example_bitvecs_to_u64() {
        let vec = BitVec::from_elem(3, true);
        assert_eq!(to_u64(&vec), Some(7));
    }

    #[test]
    fn converts_example_bitvecs_to_i64() {
        let vec = BitVec::from_elem(3, true);
        assert_eq!(to_i64(&vec), Some(7));
        let vec = BitVec::from_elem(64, true);
        assert_eq!(to_i64(&vec), Some(-1));
    }

    #[test]
    fn converts_example_bitvecs_to_bytes_pad_left() {
        let vec = BitVec::from_elem(64, true);
        assert_eq!(to_bytes_pad_left(&vec), u64::MAX.to_be_bytes());
        let vec = BitVec::from_elem(3, true);
        assert_eq!(to_bytes_pad_left(&vec), &[7]);
    }
}
