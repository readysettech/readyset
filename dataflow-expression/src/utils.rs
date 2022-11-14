use readyset_data::{DfType, DfValue};

use crate::{BuiltinFunction, Expr};

/** These helpers initialize `Expr` variants with a type field. These are
 * not inteded for use outside of tests. A planned implementation of the type
 * inference system will make the type paramter of `Expr` generic, which
 * will allow variants to be constructed without any type information - leaving
 * that to the type inference system. These functions will then be removed */

/// Helper to create `Expr::Column`. Type is unknown by default. The correct type may need to
/// be populated when type is checked at runtime
///
/// Not intended for use outside of tests
pub fn make_column(index: usize) -> Expr {
    column_with_type(index, DfType::Unknown)
}

/// Create `Expr::Column` with type set to Int
///
/// Not intended for use outside of tests
pub fn make_int_column(index: usize) -> Expr {
    column_with_type(index, DfType::Int)
}

/// Create `Expr::Column` with `DfType` ty.
pub fn column_with_type(index: usize, ty: DfType) -> Expr {
    Expr::Column { index, ty }
}

/// Create `Expr::Literal` from `DfValue`. Type is inferred from `DfValue`.
///
/// Not intended for use outside of tests
pub fn make_literal(val: DfValue) -> Expr {
    Expr::Literal {
        val: val.clone(),
        ty: val.infer_dataflow_type(),
    }
}

/// Create `Expr::Call` from `BuiltinFunction`. Type is `Unknown`.
///
/// Not intended for use outside of tests
pub fn make_call(func: BuiltinFunction) -> Expr {
    Expr::Call {
        func: Box::new(func),
        ty: DfType::Unknown,
    }
}

/// Normalizes the formatting of a JSON string.
///
/// Not intended for use outside of tests.
#[cfg(test)]
#[track_caller]
pub fn normalize_json(json: &str) -> String {
    json.parse::<serde_json::Value>().unwrap().to_string()
}

/// Converts a sequence of strings to array expression syntax.
///
/// Not intended for use outside of tests.
#[cfg(test)]
pub fn strings_to_array_expr<S>(strings: S) -> String
where
    S: IntoIterator,
    S::Item: std::fmt::Display,
{
    use itertools::Itertools;

    format!(
        "array[{}]",
        strings.into_iter().map(|s| format!("'{s}'")).join(", "),
    )
}

/// Converts `index` to be a reverse index of `slice` if negative.
///
/// Note that the result must still be bounds-checked.
fn index_to_bidirectional<T>(slice: &[T], index: isize) -> usize {
    if index.is_negative() {
        // Addition of a negative value is subtraction of the absolute value.
        slice.len().wrapping_add(index as usize)
    } else {
        index as usize
    }
}

/// Indexes into a shared slice, using reverse indexing if negative.
pub fn index_bidirectional<T>(slice: &[T], index: isize) -> Option<&T> {
    slice.get(index_to_bidirectional(slice, index))
}

/// Indexes into a mutable slice, using reverse indexing if negative.
pub fn index_bidirectional_mut<T>(slice: &mut [T], index: isize) -> Option<&mut T> {
    slice.get_mut(index_to_bidirectional(slice, index))
}

/// Inserts `element` into `vec` at `index`, using reverse indexing if negative.
///
/// If `index` is out of bounds, `element` is inserted at the end (positive `index`) or beginning
/// (negative `index`).
pub fn insert_bidirectional<T>(vec: &mut Vec<T>, index: isize, element: T, insert_after: bool) {
    let negative_index = index.is_negative();
    let mut index = index_to_bidirectional(vec, index);

    if insert_after {
        // Use wrapping arithmetic because `index_to_bidirectional` can produce `usize::MAX` (-1)
        // for cases like `len = 1` and `index = -2`.
        index = index.wrapping_add(1);
    }

    // Clamp index to bounds.
    if index > vec.len() {
        if negative_index {
            index = 0;
        } else {
            index = vec.len();
        }
    }

    vec.insert(index, element);
}

/// Removes the value in `vec` at `index`, using reverse indexing if negative.
pub fn remove_bidirectional<T>(vec: &mut Vec<T>, index: isize) -> Option<T> {
    let index = index_to_bidirectional(vec, index);
    if index < vec.len() {
        Some(vec.remove(index))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;

    mod index_bidirectional {
        use super::*;

        /// Ensures correct bounds checking.
        #[test]
        fn bounds() {
            assert_eq!(index_bidirectional::<()>(&[], -1), None);
            assert_eq!(index_bidirectional::<()>(&[], 0), None);
            assert_eq!(index_bidirectional::<()>(&[], 1), None);

            assert_eq!(index_bidirectional(&[1], -2), None);
            assert_eq!(index_bidirectional(&[1], 1), None);
        }

        /// Ensures correct results for the simple/edge case.
        #[test]
        fn single() {
            assert_eq!(index_bidirectional(&[1], 0), Some(&1));
            assert_eq!(index_bidirectional(&[1], -1), Some(&1));
        }

        /// Ensures correct results for the common case.
        #[test]
        fn multi() {
            assert_eq!(index_bidirectional(&[1, 2, 3], 0), Some(&1));
            assert_eq!(index_bidirectional(&[1, 2, 3], 1), Some(&2));
            assert_eq!(index_bidirectional(&[1, 2, 3], 2), Some(&3));

            assert_eq!(index_bidirectional(&[1, 2, 3], -1), Some(&3));
            assert_eq!(index_bidirectional(&[1, 2, 3], -2), Some(&2));
            assert_eq!(index_bidirectional(&[1, 2, 3], -3), Some(&1));
        }
    }

    mod insert_bidirectional {
        use super::*;

        #[proptest]
        fn inputs(mut vec: Vec<u32>, index: isize, element: u32, insert_after: bool) {
            let prev_len = vec.len();
            insert_bidirectional(&mut vec, index, element, insert_after);
            assert_eq!(vec.len(), prev_len + 1);
        }

        #[track_caller]
        fn test(input: &[u32], index: isize, element: u32, insert_after: bool, expected: &[u32]) {
            let mut vec = input.to_vec();
            insert_bidirectional(&mut vec, index, element, insert_after);
            assert_eq!(vec, expected);
        }

        #[test]
        fn empty() {
            test(&[], 0, 42, false, &[42]);
            test(&[], 0, 42, true, &[42]);

            test(&[], 1, 42, false, &[42]);
            test(&[], 1, 42, true, &[42]);

            test(&[], -1, 42, false, &[42]);
            test(&[], -1, 42, true, &[42]);
        }

        #[test]
        fn positive_index() {
            let input = &[0, 1, 2];

            // Positive start:
            test(input, 0, 42, false, &[42, 0, 1, 2]);

            // Positive second:
            test(input, 1, 42, false, &[0, 42, 1, 2]);
            test(input, 0, 42, true, &[0, 42, 1, 2]);

            // Positive third:
            test(input, 2, 42, false, &[0, 1, 42, 2]);
            test(input, 1, 42, true, &[0, 1, 42, 2]);

            // Positive end:
            test(input, 3, 42, false, &[0, 1, 2, 42]);
            test(input, 2, 42, true, &[0, 1, 2, 42]);

            // Positive end, out-of-bounds:
            test(input, 3, 42, true, &[0, 1, 2, 42]);
            test(input, 4, 42, false, &[0, 1, 2, 42]);
            test(input, 4, 42, true, &[0, 1, 2, 42]);
        }

        #[test]
        fn negative_index() {
            let input = &[0, 1, 2];

            // Negative start:
            test(input, -3, 42, false, &[42, 0, 1, 2]);
            test(input, -4, 42, true, &[42, 0, 1, 2]);

            // Negative start, out-of-bounds:
            test(input, -4, 42, false, &[42, 0, 1, 2]);
            test(input, -5, 42, true, &[42, 0, 1, 2]);

            // Negative second:
            test(input, -2, 42, false, &[0, 42, 1, 2]);
            test(input, -3, 42, true, &[0, 42, 1, 2]);

            // Negative third:
            test(input, -1, 42, false, &[0, 1, 42, 2]);
            test(input, -2, 42, true, &[0, 1, 42, 2]);

            // Negative end:
            test(input, -1, 42, true, &[0, 1, 2, 42]);
        }
    }
}
