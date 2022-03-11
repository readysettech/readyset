//! This serialization module only holds what is necessary to serialize the [`SqlToMirConverter`].
//! Sadly, it is not possible to have the [`SqlToMirConverter`] implement serdes without the whole
//! context (that is, the information to the MIR graphs, which is held by the [`SqlIncorporator`].
//!
//! Thus, this module holds only a way to tranform a [`SqlToMirConverter`] into a
//! [`SerializableSqlToMirConverter`], but deserialization has to occur as part of the
//! deserialization algorithm for [`SqlIncorporator`].
//!
//! See `noria/server/src/controller/sql/serde.rs` for more information.

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

use nom_sql::{ColumnSpecification, SqlIdentifier};
use noria_errors::ReadySetError;
use serde::{Deserialize, Serialize};

use crate::sql::mir::{Config, SqlToMirConverter};

/// A helper structure that holds all the information from a [`SqlToMirConverter`],
/// in a serializable form.
#[derive(Serialize, Deserialize)]
pub(in crate::controller::sql) struct SerializableSqlToMirConverter {
    // crate visibility is needed for the deserialization.
    pub(in crate::controller::sql) config: Config,
    pub(in crate::controller::sql) base_schemas:
        HashMap<SqlIdentifier, Vec<(usize, Vec<ColumnSpecification>)>>,
    pub(in crate::controller::sql) current: HashMap<SqlIdentifier, usize>,
    pub(in crate::controller::sql) nodes: HashSet<(SqlIdentifier, usize)>,
    pub(in crate::controller::sql) schema_version: usize,
}

impl<'a> TryFrom<&'a SqlToMirConverter> for SerializableSqlToMirConverter {
    type Error = ReadySetError;

    fn try_from(converter: &SqlToMirConverter) -> Result<Self, Self::Error> {
        let nodes = converter.nodes().keys().cloned().collect();

        Ok(SerializableSqlToMirConverter {
            config: converter.config.clone(),
            base_schemas: converter.base_schemas.clone(),
            current: converter.current.clone(),
            nodes,
            schema_version: converter.schema_version,
        })
    }
}
