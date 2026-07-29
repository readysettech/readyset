//! Utility functions for dealing with the postgresql protocol

use postgres_types::{Field, Kind, Type};

/// Builds the anonymous `record` type describing a row with the given field types.
///
/// The type keeps `record`'s own OID, so a client sees the `record` pseudo-type, while
/// `Kind::Composite` carries the field types that the field-wise `record` encoding needs.
/// PostgreSQL names the fields of an anonymous record `f1`, `f2`, ...; the names never reach the
/// wire, but following the convention keeps the type faithful.
pub fn record_type(field_types: impl IntoIterator<Item = Type>) -> Type {
    Type::new(
        Type::RECORD.name().to_owned(),
        Type::RECORD.oid(),
        Kind::Composite(
            field_types
                .into_iter()
                .enumerate()
                .map(|(i, ty)| Field::new(format!("f{}", i + 1), ty))
                .collect(),
        ),
        Type::RECORD.schema().to_owned(),
    )
}

/// Returns true if the given postgresql type is representationally identical to the `oid` type.
///
/// This is true for the `oid` type itself, and all the various built-in types prefixed with "reg".
/// See [the postgresql docs for the oid type][oid] for more information
///
/// [oid]: https://www.postgresql.org/docs/current/datatype-oid.html
pub fn type_is_oid(typ: &Type) -> bool {
    matches!(
        *typ,
        Type::OID
            | Type::REGCLASS
            | Type::REGCOLLATION
            | Type::REGCONFIG
            | Type::REGDICTIONARY
            | Type::REGNAMESPACE
            | Type::REGOPER
            | Type::REGOPERATOR
            | Type::REGPROC
            | Type::REGPROCEDURE
            | Type::REGROLE
            | Type::REGTYPE
    )
}
