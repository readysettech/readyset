use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::mem;

use serde_json::{Number as JsonNumber, Value as JsonValue};

type JsonObject = serde_json::Map<String, JsonValue>;

/// Returns `true` if `parent` immediately contains all of the values in `child`.
///
/// Top-level arrays can be directly checked against scalars as if the scalar were a single-element
/// array. This does not apply to nested arrays.
///
/// Arrays are treated like sets. `parent` may store more values than `child`, and duplicate values
/// are allowed. See the [PostgreSQL docs for containment rules](https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT).
pub(crate) fn json_contains(parent: &JsonValue, child: &JsonValue) -> bool {
    // FIXME(ENG-2080): `serde_json::Number` does not compare exponents and decimals correctly when
    // `arbitrary_precision` is enabled.
    return json_contains_impl(parent, child, true);

    fn json_contains_impl(parent: &JsonValue, child: &JsonValue, scalar_to_array: bool) -> bool {
        // If `scalar_to_array` is enabled, arrays can be checked against a scalar as if the scalar
        // were a single-element array. This should only be `true` for top-level arrays.

        match (parent, child) {
            // Collections are only checked against their own type or (sometimes) scalars.
            (JsonValue::Array { .. }, JsonValue::Object { .. })
            | (JsonValue::Object { .. }, JsonValue::Array { .. }) => false,

            // Check arrays against arrays or scalars.
            (JsonValue::Array(parent), JsonValue::Array(child)) => {
                json_array_contains(parent, child)
            }
            (JsonValue::Array(parent), _) => {
                // We do not check with `parent.contains` because `JsonNumber` does not handle -0.0
                // when `arbitrary_precision` is enabled.
                scalar_to_array
                    && parent
                        .iter()
                        .any(|parent_value| json_contains_impl(parent_value, child, false))
            }

            // Check objects against objects.
            (JsonValue::Object(parent), JsonValue::Object(child)) => {
                json_object_contains(parent, child)
            }

            // `JsonNumber` does not handle -0.0 when `arbitrary_precision` is enabled.
            (JsonValue::Number(parent), JsonValue::Number(child)) => json_number_eq(parent, child),

            // Check scalars if same type.
            _ => parent == child,
        }
    }

    fn json_object_contains(parent: &JsonObject, child: &JsonObject) -> bool {
        child.iter().all(|(child_key, child_value)| {
            if let Some(parent_value) = parent.get(child_key) {
                json_contains_impl(parent_value, child_value, false)
            } else {
                child_value.is_null()
            }
        })
    }

    fn json_array_contains(parent: &[JsonValue], child: &[JsonValue]) -> bool {
        // Fast path to avoid creating `parent_scalars`.
        if child.is_empty() {
            return true;
        }

        // Check scalars before recursing into collections.
        let parent_scalars: HashSet<JsonScalar> =
            parent.iter().filter_map(JsonScalar::from_value).collect();

        if !child
            .iter()
            .filter_map(JsonScalar::from_value)
            .all(|child_scalar| parent_scalars.contains(&child_scalar))
        {
            return false;
        }

        // Check collections.
        child.iter().all(|child_value| match child_value {
            JsonValue::Array(child_value) => parent.iter().any(|parent_value| {
                parent_value
                    .as_array()
                    .map(|parent_value| json_array_contains(parent_value, child_value))
                    .unwrap_or_default()
            }),
            JsonValue::Object(child_value) => parent.iter().any(|parent_value| {
                parent_value
                    .as_object()
                    .map(|parent_value| json_object_contains(parent_value, child_value))
                    .unwrap_or_default()
            }),
            // Skip scalars since they're handled previously.
            _ => true,
        })
    }
}

/// Deletes all object fields that have null values from the given JSON value, recursively. Null
/// values that are not object fields are untouched.
pub(crate) fn json_strip_nulls(json: &mut JsonValue) {
    match json {
        JsonValue::Array(array) => array.iter_mut().for_each(json_strip_nulls),
        JsonValue::Object(object) => {
            object.retain(|_, v| !v.is_null());
            object.values_mut().for_each(json_strip_nulls);
        }
        _ => {}
    }
}

// `JsonNumber` does not handle -0.0 when `arbitrary_precision` is enabled, so we special case it
// via our own equality and hashing implementations.
//
// PERF: `as_f64` is potentially expensive when `arbitrary_precision` is enabled because it must
// parse the value each time. This can be resolved by handling -0.0 when parsing the JSON.

fn json_number_eq(a: &JsonNumber, b: &JsonNumber) -> bool {
    // If they compare correctly, then don't bother checking the -0.0 case.
    if a == b {
        return true;
    }

    if let Some(a) = a.as_f64() && a == 0.0 && let Some(b) = b.as_f64() && b == 0.0 {
        return true;
    }

    false
}

fn json_number_hash<H: Hasher>(n: &JsonNumber, state: &mut H) {
    if let Some(n) = n.as_f64() && n == 0.0 {
        // When `arbitrary_precision` is disabled, this is how 0.0 is hashed by `serde_json`.
        0.0f64.to_bits().hash(state);
    } else {
        n.hash(state);
    }
}

/// Meaningful JSON scalars that can be stored in a [`HashSet`].
///
/// # Considerations
///
/// - We do not treat null as meaningful because it's equivalent to an empty value.
///
/// - Scalars are the only types of values checked via [`HashSet`] because we do not want the
///   semantics of asking the table for a collection. This must be handled by us manually due to
///   `@>` semantics not taking element order into account.
///
/// - We do not use [`Ord`] because [`JsonNumber`] does not expose a way to sort values. While we
///   could manually check each case via its methods, doing so is fallible due to being
///   non-exhaustive.
///
/// - Rather than derive [`PartialEq`] and [`Hash`], we explicitly special-case the `-0.0` for
///   [`JsonNumber`] since it does not do so for us. Note that we do not handle its lack of
///   exponent/decimal comparison (ENG-2080).
#[derive(Eq)]
enum JsonScalar<'a> {
    Bool(bool),
    // Ref because not `Copy`.
    Number(&'a JsonNumber),
    String(&'a str),
}

impl<'a> JsonScalar<'a> {
    fn from_value(value: &'a JsonValue) -> Option<Self> {
        match value {
            JsonValue::Bool(b) => Some(JsonScalar::Bool(*b)),
            JsonValue::Number(n) => Some(JsonScalar::Number(n)),
            JsonValue::String(s) => Some(JsonScalar::String(s)),
            _ => None,
        }
    }
}

impl PartialEq for JsonScalar<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Number(a), Self::Number(b)) => json_number_eq(a, b),
            _ => false,
        }
    }
}

impl Hash for JsonScalar<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);

        match self {
            Self::Bool(b) => b.hash(state),
            Self::String(s) => s.hash(state),
            Self::Number(n) => json_number_hash(n, state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod json_scalar {
        use proptest::prelude::*;

        use super::*;

        /// Like [`JsonScalar`] except it can implement [`Arbitrary`] for testing.
        #[derive(Debug)]
        enum OwnedJsonScalar {
            Bool(bool),
            Number(JsonNumber),
            String(String),
        }

        impl OwnedJsonScalar {
            fn as_json_scalar(&self) -> JsonScalar<'_> {
                match self {
                    Self::Bool(b) => JsonScalar::Bool(*b),
                    Self::Number(n) => JsonScalar::Number(n),
                    Self::String(s) => JsonScalar::String(s),
                }
            }
        }

        impl PartialEq for OwnedJsonScalar {
            fn eq(&self, other: &Self) -> bool {
                self.as_json_scalar() == other.as_json_scalar()
            }
        }

        impl Hash for OwnedJsonScalar {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.as_json_scalar().hash(state);
            }
        }

        impl Arbitrary for OwnedJsonScalar {
            type Parameters = ();
            type Strategy = BoxedStrategy<Self>;

            fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
                let f64_strategy = {
                    use proptest::num::f64::*;
                    POSITIVE | NEGATIVE | ZERO
                };

                prop_oneof![
                    any::<bool>().prop_map(Self::Bool),
                    any::<i64>().prop_map(|i| Self::Number(i.into())),
                    any::<u64>().prop_map(|i| Self::Number(i.into())),
                    f64_strategy.prop_map(|f| Self::Number(JsonNumber::from_f64(f).unwrap())),
                    any::<String>().prop_map(Self::String),
                ]
                .boxed()
            }
        }

        launchpad::eq_laws!(OwnedJsonScalar);
        launchpad::hash_laws!(OwnedJsonScalar);
    }
}
