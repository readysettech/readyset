//! Shared formatting for the errors raised when an upstream row cannot be converted, so that both
//! connectors report the same thing: which column failed, which row it was, and why.
//!
//! Reading a value out of a row and choosing which columns identify it are engine-specific and stay
//! in the connectors.

use itertools::Itertools;
use readyset_data::DfValue;
use readyset_errors::ReadySetError;
use readyset_util::redacted::Sensitive;

/// Renders an error together with its source chain. Some upstream errors, notably
/// [`tokio_postgres::Error`], display only their own kind and leave the cause reachable solely via
/// [`std::error::Error::source`].
pub(crate) fn display_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        out.push_str(&format!(": {error}"));
        source = error.source();
    }
    out
}

/// Renders `name=value` pairs for identifying a row in an error message. A column whose value could
/// not be decoded is rendered as `name=<undecodable>`, since this describes rows that failed to
/// convert.
pub(crate) fn describe_columns<I>(columns: I) -> String
where
    I: IntoIterator<Item = (String, Option<DfValue>)>,
{
    columns
        .into_iter()
        .map(|(name, value)| match value {
            Some(value) => format!("{name}={:?}", Sensitive(&value)),
            None => format!("{name}=<undecodable>"),
        })
        .join(", ")
}

/// Describes the columns identifying a row, falling back to every column when the table has no key.
pub(crate) fn describe_identifier<F>(
    identifier_columns: &[usize],
    column_count: usize,
    describe: F,
) -> String
where
    F: Fn(&[usize]) -> String,
{
    if identifier_columns.is_empty() {
        let all = (0..column_count).collect::<Vec<_>>();
        format!("row values: [{}]", describe(&all))
    } else {
        format!("identifier: [{}]", describe(identifier_columns))
    }
}

/// The error for a row that could not be converted, identical across connectors.
pub(crate) fn conversion_failed(
    table: impl std::fmt::Display,
    row: usize,
    column: impl std::fmt::Display,
    identifier: &str,
    error: &(dyn std::error::Error + 'static),
) -> ReadySetError {
    ReadySetError::ReplicationFailed(format!(
        "Failed converting to DfValue, table: {table}, row: {row}, column: {column}, \
         {identifier}, err: {}",
        display_error_chain(error)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describes_decodable_and_undecodable_columns() {
        let described = describe_columns([
            ("id".to_string(), Some(DfValue::from(42))),
            ("ts".to_string(), None),
            ("note".to_string(), Some(DfValue::from("hi"))),
        ]);
        assert_eq!(
            described,
            "id=Int(42), ts=<undecodable>, note=TinyText(\"hi\" (Utf8))"
        );
    }

    #[test]
    fn identifier_falls_back_to_every_column() {
        let describe = |indices: &[usize]| format!("{indices:?}");
        assert_eq!(
            describe_identifier(&[1, 0], 3, describe),
            "identifier: [[1, 0]]"
        );
        assert_eq!(
            describe_identifier(&[], 3, describe),
            "row values: [[0, 1, 2]]"
        );
    }

    #[test]
    fn error_chain_includes_sources() {
        #[derive(Debug)]
        struct Inner;
        impl std::fmt::Display for Inner {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("inner cause")
            }
        }
        impl std::error::Error for Inner {}

        #[derive(Debug)]
        struct Outer(Inner);
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("outer")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        assert_eq!(display_error_chain(&Outer(Inner)), "outer: inner cause");
    }
}
