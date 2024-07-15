//! This module provides miscellaneous conveniences related to iterators.  It was initially
//! created to reduce our reliance on unstable compiler features.

use std::cmp::Ordering;

/// Check for equality between the elements of two iterators according to the supplied
/// comparator.
///
/// # Examples
///
/// ```
/// use readyset_util::iter::eq_by;
/// let xs = [1, 2, 3, 4];
/// let ys = [1, 4, 9, 16];
/// assert!(eq_by(xs, ys, |x, y| x * x == y));
/// ```
pub fn eq_by<I, J, F>(a: I, b: J, mut eq: F) -> bool
where
    I: IntoIterator,
    J: IntoIterator,
    F: FnMut(I::Item, J::Item) -> bool,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();

    loop {
        match (a.next(), b.next()) {
            (None, None) => break true,
            (Some(a), Some(b)) => {
                if !eq(a, b) {
                    break false;
                }
            }
            (_, _) => break false,
        }
    }
}

/// Lexicographically compare the elements of two iterators using the supplied comparator.
///
/// # Examples
///
/// ```
/// use std::cmp::Ordering;
///
/// use readyset_util::iter::cmp_by;
/// let xs = [1, 2, 3, 4];
/// let ys = [1, 4, 9, 16];
/// assert_eq!(cmp_by(xs, ys, |x, y| x.cmp(&y)), Ordering::Less);
/// assert_eq!(cmp_by(xs, ys, |x, y| (x * x).cmp(&y)), Ordering::Equal);
/// assert_eq!(cmp_by(xs, ys, |x, y| (2 * x).cmp(&y)), Ordering::Greater);
/// ```
pub fn cmp_by<I, J, F>(a: I, b: J, mut cmp: F) -> Ordering
where
    I: IntoIterator,
    J: IntoIterator,
    F: FnMut(I::Item, J::Item) -> Ordering,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();

    loop {
        match (a.next(), b.next()) {
            (None, None) => break Ordering::Equal,
            (Some(a), Some(b)) => match cmp(a, b) {
                Ordering::Equal => continue,
                ord => break ord,
            },
            (Some(_), None) => break Ordering::Greater,
            (None, Some(_)) => break Ordering::Less,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_by() {
        let xs = [1, 2, 0, 4];
        let ys = [1, 4, 9, 16];
        assert!(!eq_by(xs, ys, |x, y| x * x == y));
        let xs = [1, 2, 3];
        let ys = [1, 4, 9, 16];
        assert!(!eq_by(xs, ys, |x, y| x * x == y));
        let xs = [1, 2, 3, 4];
        let ys = [1, 4, 9];
        assert!(!eq_by(xs, ys, |x, y| x * x == y));
    }

    #[test]
    fn test_cmp_by() {
        let xs = [1, 2, 3];
        let ys = [1, 4, 9, 16];
        assert_eq!(cmp_by(xs, ys, |x, y| x.cmp(&y)), Ordering::Less);
        let xs = [1, 2, 3];
        let ys = [1, 2, 3];
        assert_eq!(cmp_by(xs, ys, |x, y| x.cmp(&y)), Ordering::Equal);
        let xs = [1, 2, 3];
        let ys = [];
        assert_eq!(cmp_by(xs, ys, |x, y| (2 * x).cmp(&y)), Ordering::Greater);
    }
}
