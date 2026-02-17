//! This module provides the mechanism to transform the [`String`] queries issued to ReadySet
//! into a list of changes that need to happen (namely, [`ChangeList`]).
//!
//! The [`ChangeList`] structure provides a list of [`Change`]s, which shall be used by
//! the [`Recipe`] to apply them. Those [`Change`]s represent the following SQL statements:
//! - `CREATE TABLE`
//! - `CREATE CACHED QUERY`
//! - `CREATE VIEW`
//! - `DROP CACHED QUERY`
//! - `DROP TABLE`
//!
//! Said list of [`Change`]s are sorted in the same order as the queries came in. This guarantees
//! that within the same request we can have queries like these:
//! ```SQL
//! CREATE TABLE table_1 (id INT);
//! DROP TABLE table_1;
//! CREATE TABLE table_1 (id INT);
//! ```
//! Without this guarantee, we could mistakenly attempt to drop a table before creating it
//! (if we processed removals before additions), or avoid creating a table that shouldn't exist (if
//! we processed additions before removals).
// TODO(fran): Couple of things we need to change/rethink:
//  1. Rethink how we want to bubble up errors. Now we are splitting a bunch of queries in the
//   same string into an array of queries, so that we can report which one failed. Doing a
//   direct  parsing using something like `many1(..)` would be more efficient, but if the
//   parsing fails, we'll get the whole input (instead of just the failing query). For
//   example, if we parse: `CREATE TABLE table (id INT); ILLEGAL SQL; SELECT * FROM table`,
//   the error would be "couldn't parse `ILLEGAL SQL; SELECT * FROM table`", vs "couldn't
//   parse `ILLEGAL SQL;`".
//  2. Rethink how we are parsing queries in `parser.rs`: a. Some queries do not parse the
//     `statement_terminator` at the end, but some do. b. The `statement_terminator` matches
//     whitespaces, semicolons, line ending and eof. For
//    simplicity, it should only match semicolons (or semicolons and eof, at most).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use dataflow_expression::Dialect;
use itertools::Itertools;
use readyset_data::DfType;
use readyset_errors::{internal, unsupported, ReadySetError, ReadySetResult};
use readyset_sql::ast::{
    AlterTableDefinition, AlterTableStatement, CacheInner, CreateCacheStatement,
    CreateTableStatement, CreateViewStatement, DropTableStatement, DropViewStatement,
    NonReplicatedRelation, Relation, RenameTableStatement, SelectStatement, SqlIdentifier,
    SqlQuery, TableKey,
};
use readyset_sql_parsing::{parse_query_with_config, ParsingConfig, ParsingPreset};
use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteContext, AdapterRewriteParams};
use schema_catalog::SchemaGeneration;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;
use tracing::error;

use crate::consensus::CacheDDLRequest;

/// The specification for a list of changes that must be made
/// to the MIR and dataflow graphs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeList {
    /// The list of changes to be made.
    ///
    /// The changes are stored in the order they were issued.
    pub changes: Vec<Change>,

    /// The schema search path to use to resolve table references within the changelist
    pub schema_search_path: Vec<SqlIdentifier>,

    /// The SQL dialect to use for all types and expressions in queries added by this ChangeList
    pub dialect: Dialect,
}

/// Types that can be converted directly into a list of [`Change`]s. Used to type-overload
/// [`ChangeList::from_changes`]
pub trait IntoChanges {
    /// Convert this value into a list of [`Change`]s
    fn into_changes(self) -> Vec<Change>;
}

impl IntoChanges for CreateTableStatement {
    fn into_changes(self) -> Vec<Change> {
        vec![Change::CreateTable {
            statement: self,
            pg_meta: None,
        }]
    }
}

impl IntoChanges for DropTableStatement {
    fn into_changes(self) -> Vec<Change> {
        self.tables
            .into_iter()
            .map(|name| Change::Drop {
                name,
                if_exists: self.if_exists,
            })
            .collect()
    }
}

impl IntoChanges for DropViewStatement {
    fn into_changes(self) -> Vec<Change> {
        self.views
            .into_iter()
            .map(|name| Change::Drop {
                name,
                if_exists: self.if_exists,
            })
            .collect()
    }
}

impl IntoChanges for AlterTableStatement {
    fn into_changes(self) -> Vec<Change> {
        vec![Change::AlterTable(self)]
    }
}

impl IntoChanges for RenameTableStatement {
    /// For simplicity, we map MySQL `RENAME TABLE` statements to multiple `ALTER TABLE` statements.
    /// This way, Postgres (no `RENAME`) and MySQL don't diverge, and we can handle this the same
    /// way and in the same place as `ALTER TABLE RENAME`.
    fn into_changes(self) -> Vec<Change> {
        self.ops
            .into_iter()
            .map(|op| {
                let alter_statement = AlterTableStatement {
                    table: op.from,
                    only: false, // RENAME TABLE doesn't have an ONLY keyword
                    definitions: Ok(vec![AlterTableDefinition::RenameTable { new_name: op.to }]),
                };
                Change::AlterTable(alter_statement)
            })
            .collect()
    }
}

impl IntoChanges for Vec<Change> {
    fn into_changes(self) -> Vec<Change> {
        self
    }
}

impl IntoIterator for ChangeList {
    type Item = Change;
    type IntoIter = std::vec::IntoIter<Change>;

    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

impl ChangeList {
    /// Construct a new empty `ChangeList` with the given [`Dialect`]
    pub fn new(dialect: Dialect) -> Self {
        Self {
            changes: vec![],
            schema_search_path: vec![],
            dialect,
        }
    }

    /// Parse a `ChangeList` from the given vector of SQL strings, using the given [`Dialect`] for
    /// expression evaluation semantics (but it will be parsed with the canonical MySQL dialect).
    ///
    /// This method processes each string in the vector individually and constructs a list of
    /// changes. If any query fails to parse, the method returns an error.
    pub fn from_strings(queries: Vec<impl AsRef<str>>, dialect: Dialect) -> ReadySetResult<Self> {
        Self::from_strings_with_config(queries, dialect, ParsingPreset::for_prod().into_config())
    }

    pub fn from_strings_with_config(
        queries: Vec<impl AsRef<str>>,
        dialect: Dialect,
        parsing_config: ParsingConfig,
    ) -> ReadySetResult<Self> {
        Self::from_queries(
            queries
                .into_iter()
                .map(|query_str| {
                    parse_query_with_config(parsing_config, dialect.into(), &query_str)
                })
                .try_collect::<_, Vec<_>, _>()?,
            dialect,
        )
    }

    pub fn from_queries(
        queries: impl IntoIterator<Item = SqlQuery>,
        dialect: Dialect,
    ) -> ReadySetResult<Self> {
        let mut changes = Vec::new();
        for query in queries {
            match query {
                SqlQuery::CreateTable(statement) => changes.push(Change::CreateTable {
                    statement,
                    pg_meta: None,
                }),
                SqlQuery::CreateView(cvs) => changes.push(Change::CreateView(cvs)),
                SqlQuery::CreateCache(CreateCacheStatement {
                    name,
                    inner,
                    always,
                    ..
                }) => {
                    // We don't call the rewrite on the CreateCache like we do in the adapte
                    // because currently the rewrite only validates the stmt. If the validation
                    // fails, we should not persist the cache in the changelist.
                    let statement = match inner {
                        CacheInner::Statement { deep: Ok(stmt), .. } => stmt,
                        CacheInner::Statement { deep: Err(err), .. } => {
                            return Err(ReadySetError::UnparseableQuery(err))
                        }
                        CacheInner::Id(id) => {
                            error!(
                                %id,
                                "attempted to issue CREATE CACHE with an id"
                            );
                            internal!(
                                "CREATE CACHE should've had its ID resolved by \
                                         the adapter"
                            );
                        }
                    };
                    changes.push(Change::CreateCache(CreateCache {
                        name,
                        statement,
                        always,
                        schema_generation_used: None,
                    }))
                }
                SqlQuery::AlterTable(ats) => changes.push(Change::AlterTable(ats)),
                SqlQuery::DropTable(dts) => {
                    let if_exists = dts.if_exists;
                    changes.extend(
                        dts.tables
                            .into_iter()
                            .map(|name| Change::Drop { name, if_exists }),
                    )
                }
                SqlQuery::DropView(dvs) => {
                    changes.extend(dvs.views.into_iter().map(|name| Change::Drop {
                        name,
                        if_exists: dvs.if_exists,
                    }))
                }
                SqlQuery::DropCache(dcs) => changes.push(Change::Drop {
                    name: dcs.name,
                    if_exists: false,
                }),
                SqlQuery::RenameTable(rts) => {
                    changes.extend(rts.into_changes());
                }
                _ => unsupported!(
                    "Only DDL statements supported in ChangeList (got {})",
                    query.query_type()
                ),
            }
        }

        Ok(ChangeList {
            changes,
            schema_search_path: vec![],
            dialect,
        })
    }

    /// Construct a new `ChangeList` from the given single change and [`Dialect`]
    pub fn from_change(change: Change, dialect: Dialect) -> Self {
        Self::from_changes(vec![change], dialect)
    }

    /// Construct a new `ChangeList` with the given list of changes and [`Dialect`]
    pub fn from_changes<C>(changes: C, dialect: Dialect) -> Self
    where
        C: IntoChanges,
    {
        Self {
            changes: changes.into_changes(),
            schema_search_path: vec![],
            dialect,
        }
    }

    /// Return a reference to the configured schema search path for this `ChangeList`.
    pub fn schema_search_path(&self) -> &[SqlIdentifier] {
        &self.schema_search_path
    }

    /// Construct a new `ChangeList` from `self`, but with the given schema search path
    pub fn with_schema_search_path(self, schema_search_path: Vec<SqlIdentifier>) -> Self {
        Self {
            schema_search_path,
            ..self
        }
    }

    /// Construct a new `ChangeList` from `self`, but with the given schema generation
    pub fn with_schema_generation(self, schema_generation: SchemaGeneration) -> Self {
        let mut this = self;
        for change in &mut this.changes {
            if let Change::CreateCache(cache) = change {
                cache.schema_generation_used = Some(schema_generation);
            }
        }
        this
    }

    /// Return a mutable reference to the changes in this `ChangeList`
    pub fn changes_mut(&mut self) -> &mut Vec<Change> {
        &mut self.changes
    }

    /// Construct an iterator over references to the changes in this `ChangeList`
    pub fn changes(&self) -> impl Iterator<Item = &Change> + '_ {
        self.changes.iter()
    }
}

/// Describes a single change to make to a custom type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTypeChange {
    /// Set the variants of this custom type to the given list of variants.
    SetVariants {
        /// The new variants for the enum after the change
        new_variants: Vec<String>,
        /// Optional list of variants for the enum *before* the change, which can be used to
        /// determine if the change does not require resnapshotting all tables with the enum in
        /// their columns' types.
        original_variants: Option<Vec<String>>,
    },
}

/// Change to add a new cached query to the graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCache {
    /// The name of the cache. If not provided, a name will be generated based on the statement
    pub name: Option<Relation>,
    /// The `SELECT` statement for the body of the cache
    pub statement: Box<SelectStatement>,
    /// If set to `true`, execution of this cache will bypass transaction handling in the
    /// adapter
    pub always: bool,
    /// Schema generation that was used to rewrite this cache.
    ///
    /// This captures the generation of the `SchemaCatalog` at the time the query was rewritten
    /// by the adapter. The controller validates this matches its current generation before
    /// performing the migration.
    ///
    /// `None` indicates the generation was not set (e.g., in tests or legacy paths).
    pub schema_generation_used: Option<SchemaGeneration>,
}

/// Metadata about a PostgreSQL table
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresTableMetadata {
    /// The OID of the table
    pub oid: u32,
    /// A map from column name to the attribute number of that column
    pub column_oids: HashMap<SqlIdentifier, i16>,
}

impl Hash for PostgresTableMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
        let mut column_oids = self.column_oids.iter().collect::<Vec<_>>();
        column_oids.sort();
        column_oids.hash(state);
    }
}

/// Describes a singe change to be made to the MIR and dataflow graphs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change {
    /// Add a new base table to the graph, represented by the given `CREATE TABLE`` statement
    CreateTable {
        /// The `CREATE TABLE` statement
        statement: CreateTableStatement,
        /// Metadata about the table in Postgres, if it is a postgres table
        pg_meta: Option<PostgresTableMetadata>,
    },
    /// Record that a relation (a table or a view) with the given name exists in the upstream
    /// database, but is not being replicated.
    ///
    /// This is important both to have better error messages for queries that select from
    /// non-replicated relations, and to ensure we don't skip over these tables during schema
    /// resolution, resulting in queries that read from tables in the wrong schema.
    AddNonReplicatedRelation(NonReplicatedRelation),
    /// Add a new view to the graph, represented by the given `CREATE VIEW` statement
    CreateView(CreateViewStatement),
    /// Add a new cached query to the graph
    CreateCache(CreateCache),
    /// Alter an existing table in the graph, making changes according to the given `ALTER TABLE`
    /// statement
    AlterTable(AlterTableStatement),
    /// Add a new custom type
    ///
    /// Internally, custom types are just represented as aliases for a [`DfType`].
    CreateType {
        /// The name of the type
        name: Relation,
        /// The definition of the type itself.
        ///
        /// Structurally, this can be any type within ReadySet's internal type system, but in
        /// practice this is currently just enum types (and in the future will be expanded to also
        /// include composite types and range types)
        ty: DfType,
    },
    /// Alter an existing custom type
    AlterType {
        /// The oid of the type to change.
        oid: u32,
        /// The name of the type to change. This may be different than the previous name for the
        /// type, in which case the type will be renamed
        name: Relation,
        /// A specification for the change to make to the type
        change: AlterTypeChange,
    },
    /// Remove a relation from the graph.
    ///
    /// This could be one of:
    ///
    /// - A table in the graph
    /// - A non-replicated table previously created via [`Change::AddNonReplicatedTable`]
    /// - A view
    /// - A cached query
    /// - A custom type
    Drop {
        /// The name of the relation to remove.
        name: Relation,
        /// If `false`, then an error should be thrown if the relation is not found.
        if_exists: bool,
    },
}

impl Change {
    /// Creates a new [`Change::CreateCache`] from the given `name`,
    /// [`SelectStatement`], and unparsed String.
    pub fn create_cache<N>(
        name: N,
        statement: SelectStatement,
        always: bool,
        schema_generation_used: Option<SchemaGeneration>,
    ) -> Self
    where
        N: Into<Relation>,
    {
        Self::CreateCache(CreateCache {
            name: Some(name.into()),
            statement: Box::new(statement),
            always,
            schema_generation_used,
        })
    }

    /// Return true if this change requires noria to resnapshot the database in order to properly
    /// update the schema
    pub fn requires_resnapshot(&self) -> bool {
        match self {
            Change::AlterTable(alter_table) => {
                if let Ok(definitions) = &alter_table.definitions {
                    definitions.iter().any(|def| match def {
                        AlterTableDefinition::AddColumn { .. }
                        | AlterTableDefinition::AlterColumn { .. }
                        | AlterTableDefinition::DropColumn { .. }
                        | AlterTableDefinition::ChangeColumn { .. }
                        | AlterTableDefinition::RenameColumn { .. }
                        | AlterTableDefinition::RenameTable { .. }
                        | AlterTableDefinition::AddKey(TableKey::PrimaryKey { .. })
                        | AlterTableDefinition::AddKey(TableKey::UniqueKey { .. })
                        | AlterTableDefinition::DropForeignKey { .. }
                        | AlterTableDefinition::DropConstraint { .. } => true,
                        AlterTableDefinition::ReplicaIdentity(_)
                        | AlterTableDefinition::AddKey(TableKey::FulltextKey { .. })
                        | AlterTableDefinition::AddKey(TableKey::Key { .. })
                        | AlterTableDefinition::AddKey(TableKey::CheckConstraint { .. })
                        | AlterTableDefinition::AddKey(TableKey::ForeignKey { .. })
                        | AlterTableDefinition::Algorithm { .. }
                        | AlterTableDefinition::Lock { .. } => false,
                    })
                } else {
                    // We know it's an alter table, but we couldn't fully parse it.
                    // Trigger a resnapshot.
                    true
                }
            }
            Change::AlterType {
                change:
                    AlterTypeChange::SetVariants {
                        new_variants,
                        original_variants: Some(original_variants),
                    },
                ..
            } => {
                // Don't resnapshot enum changes that add new variants at the end
                new_variants.len() > original_variants.len()
                    && new_variants[..original_variants.len()] != *original_variants
            }
            Change::AlterType {
                change:
                    AlterTypeChange::SetVariants {
                        // If we don't *know* the original variants, we have to assume that the
                        // change requires a resnapshot
                        original_variants: None,
                        ..
                    },
                ..
            } => true,
            Change::CreateTable { .. }
            | Change::CreateView(_)
            | Change::CreateCache(_)
            | Change::CreateType { .. }
            | Change::Drop { .. }
            | Change::AddNonReplicatedRelation(_) => false,
        }
    }

    /// Parse a `Change` from the given [`CacheDDLRequest`], using the encapsulated [`Dialect`] and
    /// schema search path for expression evaluation semantics. This function performs the adapter
    /// rewrites on the parsed query string before passing it to the server via `/extend_recipe`.
    pub fn from_cache_ddl_request<C: AdapterRewriteContext>(
        ddl_req: &CacheDDLRequest,
        adapter_rewrite_params: AdapterRewriteParams,
        adapter_rewrite_context: &C,
        parsing_preset: ParsingPreset,
        schema_generation: Option<SchemaGeneration>,
    ) -> ReadySetResult<Self> {
        match parse_query_with_config(
            parsing_preset.into_config().log_on_mismatch(true).rate_limit_logging(false),
            ddl_req.dialect.into(),
            &ddl_req.unparsed_stmt,
        )? {
            SqlQuery::CreateCache(CreateCacheStatement {
                name,
                inner,
                always,
                ..
            }) => {
                let mut statement = match inner {
                    CacheInner::Statement { deep: Ok(stmt), .. } => stmt,
                    CacheInner::Statement { deep: Err(err), .. } => {
                        return Err(ReadySetError::UnparseableQuery(err))
                    }
                    CacheInner::Id(id) => {
                        error!(
                            %id,
                            "attempted to issue CREATE CACHE with an id"
                        );
                        internal!(
                            "CREATE CACHE should've had its ID resolved by \
                                the adapter"
                        );
                    }
                };

                adapter_rewrites::rewrite_query(
                    &mut statement,
                    adapter_rewrite_params,
                    adapter_rewrite_context,
                )?;

                Ok(Change::CreateCache(CreateCache {
                    name,
                    statement,
                    always,
                    schema_generation_used: schema_generation,
                }))
            },
            SqlQuery::DropCache(dcs) => Ok(Change::Drop {
                name: dcs.name,
                if_exists: false,
            }),
            parsed => unsupported!(
                "CacheDDLRequests can only contain `CREATE CACHE` or `DROP CACHE` statements (got {})",
                parsed.query_type()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_util::hash_laws;

    use super::*;

    hash_laws!(PostgresTableMetadata);

    mod requires_resnapshot {
        use super::*;

        #[test]
        fn alter_enum_without_original_variants() {
            let change = Change::AlterType {
                oid: 1234,
                name: "my_enum".into(),
                change: AlterTypeChange::SetVariants {
                    new_variants: vec!["a".into(), "b".into(), "c".into()],
                    original_variants: None,
                },
            };

            assert!(change.requires_resnapshot())
        }

        #[test]
        fn alter_enum_with_variant_at_end() {
            let change = Change::AlterType {
                oid: 1234,
                name: "my_enum".into(),
                change: AlterTypeChange::SetVariants {
                    new_variants: vec!["a".into(), "b".into(), "c".into()],
                    original_variants: Some(vec!["a".into(), "b".into()]),
                },
            };
            assert!(!change.requires_resnapshot())
        }

        #[test]
        fn alter_enum_with_variant_in_middle() {
            let change = Change::AlterType {
                oid: 1234,
                name: "my_enum".into(),
                change: AlterTypeChange::SetVariants {
                    new_variants: vec!["a".into(), "b".into(), "c".into()],
                    original_variants: Some(vec!["a".into(), "c".into()]),
                },
            };
            assert!(change.requires_resnapshot())
        }
    }
}
