use std::collections::{BTreeMap, HashSet};
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::{fmt, mem};

use readyset_data::{Array, DfValue};
use readyset_errors::{invalid_query_err, ReadySetError, ReadySetResult};
use serde::Serialize;
use serde_json::map::Entry as JsonEntry;
use serde_json::{Number as JsonNumber, Value as JsonValue};

use crate::utils;

type JsonObject = serde_json::Map<String, JsonValue>;

/// Determines how `jsonb_set_lax` should treat inserting SQL-null JSON strings.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum NullValueTreatment {
    #[default]
    UseJsonNull,
    DeleteKey,
    ReturnTarget,
    RaiseException,
}

impl FromStr for NullValueTreatment {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "delete_key" => Ok(Self::DeleteKey),
            "return_target" => Ok(Self::ReturnTarget),
            "use_json_null" => Ok(Self::UseJsonNull),
            "raise_exception" => Ok(Self::RaiseException),
            _ => Err(invalid_query_err!(
                "null_value_treatment must be \"delete_key\", \"return_target\", \
                \"use_json_null\", or \"raise_exception\""
            )),
        }
    }
}

impl fmt::Display for NullValueTreatment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::DeleteKey => "delete_key",
            Self::ReturnTarget => "return_target",
            Self::UseJsonNull => "use_json_null",
            Self::RaiseException => "raise_exception",
        })
    }
}

impl NullValueTreatment {
    #[cfg(test)]
    pub fn all() -> impl Iterator<Item = Self> {
        [
            Self::UseJsonNull,
            Self::DeleteKey,
            Self::ReturnTarget,
            Self::RaiseException,
        ]
        .into_iter()
    }
}

/// Serializes `json` to a pretty-printed [`String`] with 4-space indentation.
///
/// This differs from [`serde_json::to_string_pretty`] which uses 2 spaces.
pub(crate) fn json_to_pretty(json: &JsonValue) -> String {
    use serde_json::ser::{PrettyFormatter, Serializer};

    let indent = b"    ";

    // `serde_json` also uses 128 as an initial capacity.
    let mut buf = Vec::with_capacity(128);

    // We assume serialization won't fail similarly to how we assume `JsonValue` to `DfValue` string
    // won't fail serialization.
    #[allow(clippy::unwrap_used)]
    json.serialize(&mut Serializer::with_formatter(
        &mut buf,
        PrettyFormatter::with_indent(indent),
    ))
    .unwrap();

    // SAFETY: `JsonValue` does not emit invalid UTF-8 and `indent` is UTF-8.
    // `serde_json::to_string_pretty` makes this same assumption.
    unsafe { String::from_utf8_unchecked(buf) }
}

/// Calculates the maximum depth of a JSON value using [MySQL semantics](https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-depth).
///
/// - An empty array, empty object, or scalar value has depth 1.
/// - A nonempty array containing only elements of depth 1 or nonempty object containing only member
///   values of depth 1 has depth 2.
/// - Otherwise, a JSON document has depth greater than 2.
pub(crate) fn json_depth(json: &JsonValue) -> u64 {
    // Although recursing deeply nested JSON could theoretically blow the stack, in practice it
    // won't since `serde_json::Value` has a recursion limit of 127 when parsing. Even if this limit
    // is bypassed (e.g. enabling `unbounded_depth`), `Display` and `Drop` can also blow the stack
    // (see https://github.com/serde-rs/json/issues/440).

    fn max_depth<'j>(values: impl IntoIterator<Item = &'j JsonValue>) -> u64 {
        values
            .into_iter()
            .fold(0, |depth, value| depth.max(json_depth(value)))
    }

    match json {
        JsonValue::Array(array) => 1 + max_depth(array),
        JsonValue::Object(object) => 1 + max_depth(object.values()),
        _ => 1,
    }
}

/// Quotes and escapes a string to produce a valid JSON string literal for inclusion within a JSON
/// document.
pub(crate) fn json_quote(json: &str) -> String {
    let mut result = String::with_capacity(json.len() + 2);
    result.push('"');

    // Find characters to escape.
    let mut prev_index = 0;
    let mut search_iter = json.bytes().enumerate();

    while let Some((next_index, ch)) = search_iter.find(|(_, ch)| {
        // https://github.com/mysql/mysql-server/blob/8.0/sql-common/json_dom.cc#L1144
        matches!(ch, 0..=0x1f | b'"' | b'\\')
    }) {
        // Push previous characters.
        result.push_str(&json[prev_index..next_index]);

        // Push escaped character.
        if ch <= 0x1f {
            write!(result, "\\u{ch:04x}").unwrap();
        } else {
            result.push('\\');
            result.push(ch as char);
        }

        prev_index = next_index + 1;
    }

    // Push remaining characters.
    result.push_str(&json[prev_index..]);

    result.push('"');
    result
}

/// Removes a value from JSON using PostgreSQL `#-` semantics.
pub(crate) fn json_remove_path<'k>(
    json: &mut JsonValue,
    keys: impl IntoIterator<Item = &'k DfValue>,
) -> ReadySetResult<()> {
    if !json.is_array() && !json.is_object() {
        return Err(invalid_query_err!(
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

/// A collection of key/value pairs stored as either a vector (if duplicate keys are allowed) or a
/// hash map.
enum EitherPairs<K, V> {
    Vec(Vec<(K, V)>),
    Map(BTreeMap<K, V>),
}

impl<K, V> EitherPairs<K, V> {
    fn with_capacity(capacity: usize, allow_duplicate_keys: bool) -> Self {
        if allow_duplicate_keys {
            Self::Vec(Vec::with_capacity(capacity))
        } else {
            Self::Map(BTreeMap::new())
        }
    }

    fn insert(&mut self, k: K, v: V)
    where
        K: Ord,
    {
        match self {
            Self::Vec(vec) => {
                vec.push((k, v));
            }
            Self::Map(map) => {
                map.insert(k, v);
            }
        }
    }

    fn to_json_string(&self) -> serde_json::Result<String>
    where
        K: Ord + Serialize,
        V: Serialize,
    {
        match self {
            Self::Map(map) => serde_json::to_string(map),
            Self::Vec(vec) => serde_json::to_string(&utils::serialize_slice_as_map(vec)),
        }
    }
}

/// Constructs a [`DfValue`] with the textual representation of a JSON object from a sequence of
/// key/value pairs in an array.
///
/// If the array is 1D, the array is expected to have alternating key/value pairs. If the array is
/// 2D, each each inner array is expected to contain a single key/value pair.
pub(crate) fn json_object_from_pairs(
    kv_pairs: &Array,
    allow_duplicate_keys: bool,
) -> ReadySetResult<DfValue> {
    // PostgreSQL allows empty arrays of any number of dimensions.
    if kv_pairs.is_empty() {
        return Ok("{}".into());
    }

    // HACK: Values borrowed by `result` below need to be owned because `ArrayView::values` does not
    // have a lifetime over `&DfValue` but rather `&ArrayView`.
    let mut result_kv_pairs: Vec<(DfValue, DfValue)> = Vec::new();

    match kv_pairs.num_dimensions() {
        1 => {
            if kv_pairs.total_len() % 2 != 0 {
                return Err(invalid_query_err!(
                    "array must have even number of elements"
                ));
            }

            let mut kv_pairs = kv_pairs.values();

            while let (Some(k), Some(v)) = (kv_pairs.next(), kv_pairs.next()) {
                result_kv_pairs.push((k.clone(), v.clone()));
            }
        }
        2 => {
            let arity_error = || invalid_query_err!("array must have two columns");

            for outer_view in kv_pairs.outer_dimension() {
                let mut kv_pair = outer_view.values();

                let k = kv_pair.next().ok_or_else(arity_error)?;
                let v = kv_pair.next().ok_or_else(arity_error)?;

                if kv_pair.next().is_some() {
                    return Err(arity_error());
                }

                result_kv_pairs.push((k.clone(), v.clone()));
            }
        }
        _ => return Err(invalid_query_err!("wrong number of array dimensions")),
    }

    let mut result =
        EitherPairs::<&str, &str>::with_capacity(result_kv_pairs.len(), allow_duplicate_keys);

    for (k, v) in &result_kv_pairs {
        result.insert(<&str>::try_from(k)?, <&str>::try_from(v)?);
    }

    Ok(result.to_json_string()?.into())
}

pub(crate) fn json_object_from_keys_and_values(
    keys: &Array,
    values: &Array,
    allow_duplicate_keys: bool,
) -> ReadySetResult<DfValue> {
    // PostgreSQL allows empty arrays of any number of dimensions.
    if keys.is_empty() && values.is_empty() {
        return Ok("{}".into());
    }

    if keys.num_dimensions() != 1 || values.num_dimensions() != 1 {
        return Err(invalid_query_err!("wrong number of array dimensions"));
    }

    if keys.total_len() != values.total_len() {
        return Err(invalid_query_err!("mismatched array dimensions"));
    }

    let mut result =
        EitherPairs::<&str, &str>::with_capacity(keys.total_len(), allow_duplicate_keys);

    for (k, v) in keys.values().zip(values.values()) {
        result.insert(<&str>::try_from(k)?, <&str>::try_from(v)?);
    }

    Ok(result.to_json_string()?.into())
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

            // Check scalars if same type.
            _ => json_eq(parent, child),
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

/// Returns `true` if the two JSON documents are equal or have any common key/value pairs or array
/// elements.
pub(crate) fn json_overlaps(a: &JsonValue, b: &JsonValue) -> bool {
    // FIXME(ENG-2080): `serde_json::Number` does not compare exponents and decimals correctly when
    // `arbitrary_precision` is enabled.
    match (a, b) {
        (JsonValue::Object(a), JsonValue::Object(b)) => a
            .iter()
            .any(|(key, a)| b.get(key).map(|b| json_eq(a, b)).unwrap_or_default()),

        // PERF: O(1) best case, O(n*m) worst case.
        (JsonValue::Array(a), JsonValue::Array(b)) => {
            a.iter().any(|a| b.iter().any(|b| json_eq(a, b)))
        }

        // Compare scalars if same type.
        _ => json_eq(a, b),
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
        return Err(invalid_query_err!(
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

pub(crate) fn json_set<'k>(
    target_json: &mut JsonValue,
    key_path: impl IntoIterator<Item = &'k DfValue>,
    new_json: JsonValue,
    create_if_missing: bool,
) -> ReadySetResult<()> {
    if !target_json.is_array() && !target_json.is_object() {
        return Err(invalid_query_err!(
            "Cannot set path in non-array, non-object JSON value"
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

            if let Some(entry) = utils::index_bidirectional_mut(array, index) {
                *entry = new_json;
            } else if create_if_missing {
                let index = if index.is_negative() { 0 } else { array.len() };
                array.insert(index, new_json);
            }
        }
        Some(JsonValue::Object(object)) => match object.entry(inner_key) {
            JsonEntry::Occupied(mut entry) => {
                entry.insert(new_json);
            }
            JsonEntry::Vacant(entry) => {
                if create_if_missing {
                    entry.insert(new_json);
                }
            }
        },
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
    invalid_query_err!(
        "path element at position {} is not an integer",
        iter_count + 1
    )
}

// `JsonNumber` does not handle -0.0 when `arbitrary_precision` is enabled, so we special case it
// via our own equality and hashing implementations.
//
// PERF: `as_f64` is potentially expensive when `arbitrary_precision` is enabled because it must
// parse the value each time. This can be resolved by handling -0.0 when parsing the JSON.

/// Like [`JsonValue::eq`] except it handles `0.0 == -0.0`.
fn json_eq(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Object(a), JsonValue::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).map(|b| json_eq(a, b)).unwrap_or_default())
        }
        (JsonValue::Array(a), JsonValue::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_eq(a, b))
        }
        (JsonValue::Number(a), JsonValue::Number(b)) => json_number_eq(a, b),
        _ => a == b,
    }
}

/// Like [`JsonNumber::eq`] except it handles `0.0 == -0.0`.
fn json_number_eq(a: &JsonNumber, b: &JsonNumber) -> bool {
    // If they compare correctly, then don't bother checking the -0.0 case.
    //
    // FIXME(ENG-2080): `serde_json::Number` does not compare exponents and decimals correctly when
    // `arbitrary_precision` is enabled.
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
    use itertools::Itertools;
    use proptest::prelude::*;

    use super::*;

    mod json_quote {
        use test_strategy::proptest;

        use super::*;

        #[proptest]
        fn inputs(string: String) {
            let quoted = json_quote(&string);
            assert!(quoted.len() >= string.len() + 2);
        }

        #[test]
        fn mixed() {
            assert_eq!(json_quote("he\0ll\"o"), r#""he\u0000ll\"o""#);
            assert_eq!(json_quote("wo\u{1f}rl\\d"), r#""wo\u001frl\\d""#);
        }

        #[proptest]
        fn non_escaped(s: String) {
            let contains_escapable = s
                .chars()
                .any(|ch| matches!(ch, '\0'..='\u{1f}' | '"' | '\\'));

            prop_assume!(!contains_escapable);

            assert_eq!(json_quote(&s), format!("\"{s}\""));
        }

        #[test]
        fn escape_special() {
            fn escape(ch: char) -> String {
                let hexes = [
                    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c",
                    "0d", "0e", "0f", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
                    "1a", "1b", "1c", "1d", "1e", "1f",
                ];
                format!("\\u00{}", hexes[ch as usize])
            }

            fn test(chars: &[char]) {
                let input = chars.iter().join("");

                let expected = format!("\"{}\"", chars.iter().copied().map(escape).join(""));
                let result = json_quote(&input);

                assert_eq!(result, expected, "wrong result for {input:?}");
            }

            let iter = (0u8..=0x1f).map(|ch| ch as char);

            for a in iter.clone() {
                test(&[a]);

                for b in iter.clone() {
                    test(&[a, b]);

                    for c in iter.clone() {
                        test(&[a, b, c]);
                    }
                }
            }
        }

        #[test]
        fn escape_backslash() {
            fn test(strings: &[&str]) {
                let input = strings.join("");

                let expected = format!("\"{}\"", strings.iter().map(|s| format!("\\{s}")).join(""));
                let result = json_quote(&input);

                assert_eq!(result, expected, "wrong result for {input:?}");
            }

            let strings = ["\"", "\\"];

            for a in strings {
                test(&[a]);

                for b in strings {
                    test(&[a, b]);
                }
            }
        }

        #[test]
        fn unicode() {
            let strings = ["año", "straße", "🙂", "🦀"];

            for s in strings {
                assert_eq!(json_quote(s), format!("\"{s}\""));
            }
        }
    }

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

        readyset_util::eq_laws!(OwnedJsonScalar);
        readyset_util::hash_laws!(OwnedJsonScalar);
    }
}
