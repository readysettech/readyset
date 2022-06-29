use std::cmp::Ordering;
use std::fmt::{self, Display};

use ndarray::{ArrayBase, ArrayD, Data, IxDyn, RawData};
use proptest::arbitrary::Arbitrary;
use proptest::prop_oneof;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::{DataType, DataTypeKind};

/// Internal representation of PostgreSQL arrays
///
/// PostgreSQL arrays:
///
/// 1. Are n-dimensional, but always rectangular
/// 2. Default to a lower bound of 1, but support changing dimensionality
/// 3. Are always homogenously typed
///
/// This struct supports the first two features, but does not enforce the third.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Array {
    /// The lower bounds for each of the dimensions
    ///
    /// These values are subtracted from the corresponding indexes when doing indexing operations
    /// via [`Self::get`]
    lower_bounds: SmallVec<[i32; 2]>,

    /// The actual contents of the array, represented as a dynamically-dimensioned array (including
    /// shape)
    contents: ArrayD<DataType>,
}

impl Array {
    /// Returns the number of dimensions in the array.
    ///
    /// This function will never return 0
    #[inline]
    pub fn num_dimensions(&self) -> usize {
        self.contents.ndim()
    }

    /// Look up a value at the given index in the array, with indices supplied starting at the lower
    /// bounds provided when constructing the array.
    ///
    /// If the supplied slice of indexes has a length other than
    /// [`self.num_dimensions()`][Array::num_dimensions] or if any of the indices are out-of-bounds,
    /// returns `None`
    #[inline]
    pub fn get(&self, ixs: &[isize]) -> Option<&DataType> {
        let ixs = ixs
            .iter()
            .zip(&*self.lower_bounds)
            .map(|(ix, lb)| usize::try_from(*ix - (*lb as isize)).ok())
            .collect::<Option<Vec<_>>>()?;

        self.contents.get(ixs.as_slice())
    }

    /// Returns an iterator over all the values in the array, iterating the innermost dimension
    /// first.
    pub fn values(&self) -> impl Iterator<Item = &DataType> + '_ {
        self.contents.iter()
    }
}

impl Ord for Array {
    fn cmp(&self, other: &Self) -> Ordering {
        self.contents
            .shape()
            .cmp(other.contents.shape())
            .then_with(|| self.lower_bounds.cmp(&other.lower_bounds))
            .then_with(|| self.contents.iter().cmp(other.contents.iter()))
    }
}

impl PartialOrd for Array {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.lower_bounds.iter().any(|b| *b != 1) {
            for (lower_bound, len) in self.lower_bounds.iter().zip(self.contents.shape()) {
                let upper_bound = (*lower_bound as isize) + (*len as isize - 1);
                write!(f, "[{}:{}]", lower_bound, upper_bound)?;
            }
            write!(f, "=")?;
        }

        fn print_array<V>(f: &mut fmt::Formatter, arr: ArrayBase<V, IxDyn>) -> fmt::Result
        where
            V: RawData<Elem = DataType> + Data,
        {
            write!(f, "{{")?;
            if arr.ndim() == 1 {
                for (i, val) in arr.iter().enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }

                    if let Ok(s) = <&str>::try_from(val) {
                        write!(f, "\"{}\"", s.replace('"', "\\\""))?;
                    } else {
                        write!(f, "{}", val)?;
                    }
                }
            } else {
                let next_level = arr.outer_iter();
                for (i, arr) in next_level.enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }
                    print_array(f, arr)?;
                }
            }
            write!(f, "}}")
        }

        print_array(f, self.contents.view())
    }
}

impl Arbitrary for Array {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::collection::vec;
        use proptest::prelude::*;

        (
            (1_usize..=3_usize),
            any::<DataTypeKind>().prop_filter("Nested Array", |dtk| *dtk != DataTypeKind::Array),
        )
            .prop_flat_map(|(ndims, kind)| {
                (
                    vec(prop_oneof![Just(1i32), any::<i32>()], ndims),
                    vec(1_usize..=3usize, ndims),
                )
                    .prop_flat_map(move |(lbs, lens)| {
                        let total_elems: usize = lens.iter().copied().product();
                        vec(any_with::<DataType>(Some(kind)), total_elems).prop_map(move |vals| {
                            Self {
                                lower_bounds: lbs.clone().into(),
                                contents: ArrayD::from_shape_vec(IxDyn(&lens), vals).unwrap(),
                            }
                        })
                    })
            })
            .boxed()
    }
}

impl From<Vec<DataType>> for Array {
    fn from(vs: Vec<DataType>) -> Self {
        Self {
            lower_bounds: smallvec![1], // postgres arrays start at 1
            contents: ArrayD::from_shape_vec(IxDyn(&[vs.len()]), vs).unwrap(),
        }
    }
}

impl From<ArrayD<DataType>> for Array {
    fn from(contents: ArrayD<DataType>) -> Self {
        Self {
            lower_bounds: smallvec![1; contents.ndim()], // postgres arrays start at 1
            contents,
        }
    }
}

#[cfg(test)]
mod tests {
    use launchpad::ord_laws;

    use super::*;

    ord_laws!(Array);

    #[test]
    fn from_vec() {
        let vals = vec![DataType::from(1), DataType::from(2), DataType::from(3)];
        let arr = Array::from(vals.clone());
        assert_eq!(arr.num_dimensions(), 1);
        assert_eq!(arr.contents.into_raw_vec(), vals)
    }

    #[test]
    fn get_with_alternate_lower_bound() {
        let arr = Array {
            lower_bounds: smallvec![-5, 4],
            contents: ArrayD::from_shape_vec(
                IxDyn(&[2, 3]),
                vec![
                    // row 1
                    DataType::from(1),
                    DataType::from(2),
                    DataType::from(3),
                    // row 2
                    DataType::from(4),
                    DataType::from(5),
                    DataType::from(6),
                ],
            )
            .unwrap(),
        };

        assert_eq!(arr.get(&[-4, 5]), Some(&DataType::from(5)));
    }

    #[test]
    fn print_1d_array() {
        let arr = Array::from(vec![
            DataType::from("a"),
            DataType::from("b"),
            DataType::from("c"),
        ]);
        assert_eq!(arr.to_string(), r#"{"a","b","c"}"#);
    }

    #[test]
    fn print_2d_array() {
        let arr = Array::from(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    // row 1
                    DataType::from(1),
                    DataType::from(2),
                    // row 2
                    DataType::from(3),
                    DataType::from(4),
                ],
            )
            .unwrap(),
        );
        assert_eq!(arr.to_string(), "{{1,2},{3,4}}")
    }

    #[test]
    fn print_3d_array() {
        let arr = Array::from(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2, 1]),
                vec![
                    DataType::from(1),
                    DataType::from(2),
                    DataType::from(3),
                    DataType::from(4),
                ],
            )
            .unwrap(),
        );
        assert_eq!(arr.to_string(), "{{{1},{2}},{{3},{4}}}")
    }

    #[test]
    fn print_2d_array_with_alternate_lower_bound() {
        let arr = Array {
            lower_bounds: smallvec![-5, 4],
            contents: ArrayD::from_shape_vec(
                IxDyn(&[2, 3]),
                vec![
                    // row 1
                    DataType::from(1),
                    DataType::from(2),
                    DataType::from(3),
                    // row 2
                    DataType::from(4),
                    DataType::from(5),
                    DataType::from(6),
                ],
            )
            .unwrap(),
        };

        assert_eq!(arr.to_string(), "[-5:-4][4:6]={{1,2,3},{4,5,6}}");
    }
}
