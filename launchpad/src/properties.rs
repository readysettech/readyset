//! Macros that generate proptest test suites checking laws of stdlib traits

/// Generate a suite of tests to check the laws of the [`Eq`] impl for the given type
///
/// This generates 3 tests:
///
/// * One to check reflexivity; that `∀ x. x == x`
/// * One to check symmetry; that `∀ x y. (x == y) == (y == x)`
/// * One to check transitivity; that `∀ x y z, x == y ∧ y == z → x == z`
///
/// # Examples
///
/// ```
/// #[derive(Debug, Eq, PartialEq)]
/// struct MyType;
///
/// #[cfg(test)]
/// mod tests {
///     use launchpad::eq_laws;
///
///     eq_laws!(MyType);
/// }
/// ```
#[macro_export]
macro_rules! eq_laws {
    ($ty: ty) => {
        #[allow(clippy::eq_op)]
        mod eq {
            use super::*;
            use test_strategy::proptest;

            #[proptest]
            fn reflexive(x: $ty) {
                assert!(x == x);
            }

            #[proptest]
            fn symmetric(x: $ty, y: $ty) {
                assert_eq!(x == y, y == x);
            }

            #[proptest]
            fn transitive(x: $ty, y: $ty, z: $ty) {
                if x == y && y == z {
                    assert!(x == z);
                }
            }
        }
    };
}

/// Generate a suite of tests to check the laws of the [`Ord`] impl for the given type
///
/// # Examples
///
/// ```
/// #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
/// struct MyType;
///
/// #[cfg(test)]
/// mod tests {
///     use launchpad::ord_laws;
///
///     ord_laws!(MyType);
/// }
/// ```
#[macro_export]
macro_rules! ord_laws {
    ($ty: ty) => {
        mod ord {
            use super::*;
            use test_strategy::proptest;

            #[proptest]
            fn partial_cmp_matches_cmp(x: $ty, y: $ty) {
                assert_eq!(x.partial_cmp(&y), Some(x.cmp(&y)));
            }

            #[proptest]
            fn dual(x: $ty, y: $ty) {
                if x < y {
                    assert!(y > x);
                }
                if y < x {
                    assert!(x > y);
                }
            }

            #[proptest]
            fn le_transitive(x: $ty, y: $ty, z: $ty) {
                if x < y && y < z {
                    assert!(x < z)
                }
            }

            #[proptest]
            fn gt_transitive(x: $ty, y: $ty, z: $ty) {
                if x > y && y > z {
                    assert!(x > z)
                }
            }

            #[proptest]
            fn trichotomy(x: $ty, y: $ty) {
                let less = x < y;
                let greater = x > y;
                let eq = x == y;

                if less {
                    assert!(!greater);
                    assert!(!eq);
                }

                if greater {
                    assert!(!less);
                    assert!(!eq);
                }

                if eq {
                    assert!(!less);
                    assert!(!greater);
                }
            }
        }
    };
}

/// Generate a test to check the laws of the [`Hash`] impl for the given type
///
/// # Examples
///
/// ```
/// #[derive(Eq, PartialEq, Hash)]
/// struct MyType;
///
/// #[cfg(test)]
/// mod tests {
///     use launchpad::hash_laws;
///
///     hash_laws!(MyType);
/// }
/// ```
#[macro_export]
macro_rules! hash_laws {
    ($ty: ty) => {
        mod hash {
            use super::*;
            use launchpad::hash::hash;
            use test_strategy::proptest;

            #[proptest]
            fn matches_eq(x: $ty, y: $ty) {
                assert_eq!(x == y, hash(&x) == hash(&y));
            }
        }
    };
}
