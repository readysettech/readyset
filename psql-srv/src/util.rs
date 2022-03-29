//! Utility functions for dealing with the postgresql protocol

use postgres_types::Type;

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
