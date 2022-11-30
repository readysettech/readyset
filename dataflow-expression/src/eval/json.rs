use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::mem;

use readyset_data::DfValue;
use readyset_errors::{invalid_err, ReadySetError, ReadySetResult};
use serde_json::{Number as JsonNumber, Value as JsonValue};

use crate::utils;

type JsonObject = serde_json::Map<String, JsonValue>;

/// Removes a value from JSON using PostgreSQL `#-` semantics.
pub(crate) fn json_remove_path<'k>(
    json: &mut JsonValue,
    keys: impl IntoIterator<Item = &'k DfValue>,
) -> ReadySetResult<()> {
    if !json.is_array() && !json.is_object() {
        return Err(invalid_err!(
            "Cannot delete path in non-array, non-object JSON value"
        ));
    }

    // Nulls are not allowed as path keys. Otherwise, converting to `&str` should succeed because
    // type errors are handled during expression lowering.
    let keys: Vec<&str> = keys
        .into_iter()
        .map(<&str>::try_from)
        .collect::<ReadySetResult<_>>()?;

    let Some((&removal_key, search_keys)) = keys.split_last() else {
        // Nothing to remove.
        return Ok(());
    };

    match json_find_mut(json, search_keys)? {
        Some(JsonValue::Array(array)) => {
            let index = parse_json_index(removal_key)
                .ok_or_else(|| parse_json_index_error(search_keys.len()))?;

            utils::remove_bidirectional(array, index);
        }
        Some(JsonValue::Object(object)) => {
            object.remove(removal_key);
        }
        _ => {}
    }

    Ok(())
}

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

/// Extracts the JSON value from a path of key/index strings and returns a textual [`DfValue`] on
/// success.
///
/// Returns [`DfValue::None`] if the lookup fails or
/// [`ReadySetError`](readyset_errors::ReadySetError) for non-string keys.
///
/// All key/index path extraction operations behave the same in PostgreSQL except for the return
/// type, which is handled during expression lowering.
pub(crate) fn json_extract_key_path<'k>(
    mut json: &JsonValue,
    keys: impl IntoIterator<Item = &'k DfValue>,
) -> ReadySetResult<DfValue> {
    // `json` is reassigned to inner fields while looping through keys.

    for key in keys {
        // Null keys are allowed but always fail lookup.
        if key.is_none() {
            return Ok(DfValue::None);
        }

        // Type errors are handled during expression lowering.
        let key = <&str>::try_from(key)?;

        let inner = match json {
            JsonValue::Array(array) => parse_json_index(key)
                .and_then(|index| crate::utils::index_bidirectional(array, index)),

            JsonValue::Object(object) => object.get(key),

            _ => None,
        };

        match inner {
            Some(inner) => json = inner,
            None => return Ok(DfValue::None),
        }
    }

    Ok(json.to_string().into())
}

pub(crate) fn json_insert<'k>(
    target_json: &mut JsonValue,
    key_path: impl IntoIterator<Item = &'k DfValue>,
    inserted_json: JsonValue,
    insert_after: bool,
) -> ReadySetResult<()> {
    if !target_json.is_array() && !target_json.is_object() {
        return Err(invalid_err!(
            "Cannot insert path in non-array, non-object JSON value"
        ));
    }

    let key_path: Vec<&str> = key_path
        .into_iter()
        .map(TryInto::try_into)
        .collect::<ReadySetResult<_>>()?;

    let Some((&inner_key, search_keys)) = key_path.split_last() else {
        return Ok(());
    };

    match json_find_mut(target_json, search_keys)? {
        Some(JsonValue::Array(array)) => {
            let index = parse_json_index(inner_key)
                .ok_or_else(|| parse_json_index_error(search_keys.len()))?;

            utils::insert_bidirectional(array, index, inserted_json, insert_after);
        }
        Some(JsonValue::Object(object)) => {
            if let serde_json::map::Entry::Vacant(entry) = object.entry(inner_key) {
                entry.insert(inserted_json);
            }
        }
        _ => {}
    }

    Ok(())
}

/// Returns a mutable reference to an inner JSON value located at the end of a key path.
fn json_find_mut<K>(mut json: &mut JsonValue, key_path: K) -> ReadySetResult<Option<&mut JsonValue>>
where
    K: IntoIterator,
    K::Item: AsRef<str>,
{
    for (iter_count, key) in key_path.into_iter().enumerate() {
        let key = key.as_ref();

        let inner = match json {
            JsonValue::Array(array) => {
                // NOTE: PostgreSQL seems to only report parse failures in mutating JSON operations.
                // So an immutable equivalent of this function should not emit errors. However, this
                // assumption should be validated for each operation when implementing.
                let index =
                    parse_json_index(key).ok_or_else(|| parse_json_index_error(iter_count))?;

                utils::index_bidirectional_mut(array, index)
            }
            JsonValue::Object(object) => object.get_mut(key),
            _ => None,
        };

        match inner {
            Some(inner) => json = inner,
            None => return Ok(None),
        }
    }

    Ok(Some(json))
}

/// Parses an array index using PostgreSQL's rules.
fn parse_json_index(index: &str) -> Option<isize> {
    // NOTE: PostgreSQL ignores leading whitespace, and "+" prefix parsing is handled the same way
    // in both Rust and PostgreSQL.
    index.trim_start().parse::<isize>().ok()
}

/// Returns the appropriate error for reporting [`parse_json_index`] failure.
///
/// `iter_count` is the zero-based iteration offset, such as from [`Iterator::enumerate`].
fn parse_json_index_error(iter_count: usize) -> ReadySetError {
    invalid_err!(
        "path element at position {} is not an integer",
        iter_count + 1
    )
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
