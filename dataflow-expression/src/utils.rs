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
}
