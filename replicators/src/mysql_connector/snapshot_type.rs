use mysql_async::{self as mysql};
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{Column, CreateTableBody, Relation, SqlIdentifier, TableKey};
use readyset_sql::DialectDisplay;

use super::utils::MYSQL_BATCH_SIZE;

/// Find the first unique key with usable columns (no functional expressions).
/// A key is usable only if ALL its columns are simple column references.
/// Returns an error if the table has keys but none are usable.
/// Returns `(vec![], None)` if no keys exist at all (for FullTableScan fallback).
fn find_usable_unique_key(
    cts: &CreateTableBody,
) -> ReadySetResult<(Vec<&Column>, Option<SqlIdentifier>)> {
    let Some(keys) = &cts.keys else {
        return Ok((vec![], None));
    };

    for key in keys.iter() {
        if let TableKey::UniqueKey { .. } = key {
            // Only use UK if it has NO functional expressions (all simple columns)
            if !key.has_functional_expressions() {
                let cols = key.get_columns();
                if !cols.is_empty() {
                    return Ok((cols, key.index_name().clone()));
                }
            }
        }
    }

    // Check if ANY keys (PK or UK) exist but none are usable
    let has_keys = keys
        .iter()
        .any(|k| matches!(k, TableKey::PrimaryKey { .. } | TableKey::UniqueKey { .. }));
    if has_keys {
        Err(readyset_errors::unsupported_err!(
            "Table has primary/unique keys but all contain functional index expressions"
        ))
    } else {
        Ok((vec![], None))
    }
}

/// The type of snapshot to be taken
/// KeyBased: Snapshot based on the primary key or unique key
/// FullTableScan: Snapshot the entire table
pub(crate) enum SnapshotType {
    KeyBased {
        name: Option<SqlIdentifier>,
        keys: Vec<Column>,
        column_indices: Vec<usize>,
    },
    FullTableScan,
}

impl SnapshotType {
    pub fn new(table: &readyset_client::Table) -> ReadySetResult<Self> {
        let cts = match table.schema() {
            Some(cts) => cts,
            None => {
                return Ok(SnapshotType::FullTableScan);
            }
        };

        // Try to find a usable key:
        // 1. Try primary key first (if it has NO functional expressions)
        // 2. Fall back to unique keys (skip any with functional expressions)
        // 3. Error if keys exist but none are usable
        // 4. Use FullTableScan if no keys at all
        let (keys, name) = if let Some(pk) = cts.get_primary_key() {
            // Only use PK if ALL columns are simple (no functional expressions)
            if !pk.has_functional_expressions() {
                let columns = pk.get_columns();
                if !columns.is_empty() {
                    (columns, Some(SqlIdentifier::from("PRIMARY")))
                } else {
                    find_usable_unique_key(cts)?
                }
            } else {
                // Primary key has functional expressions, try unique keys
                find_usable_unique_key(cts)?
            }
        } else {
            // No primary key, try unique keys
            find_usable_unique_key(cts)?
        };

        // If no usable key was found (empty keys), use FullTableScan
        if keys.is_empty() {
            return Ok(SnapshotType::FullTableScan);
        }

        let mut column_indices = Vec::new();
        for k in &keys {
            let col_name = k.name.to_lowercase();
            let Some(col) = cts.fields.iter().enumerate().find_map(|(i, f)| {
                if col_name == f.column.name.to_lowercase() {
                    Some(i)
                } else {
                    None
                }
            }) else {
                return Err(readyset_errors::ReadySetError::NoSuchColumn(col_name));
            };
            column_indices.push(col);
        }
        assert_eq!(keys.len(), column_indices.len());

        Ok(SnapshotType::KeyBased {
            name,
            keys: keys.into_iter().cloned().collect(),
            column_indices,
        })
    }

    /// Generate the queries to be used for snapshotting the table, given the snapshot type
    ///
    /// Arguments:
    /// * `table` - The table to snapshot
    ///
    /// Returns:
    /// * A tuple containing the count query, the initial query, the bound based query
    ///   and the collation query
    pub fn get_queries(
        &self,
        table_name: &Relation,
        snapshot_query_comment: Option<String>,
    ) -> (String, String, String, String) {
        let force_index = match self {
            SnapshotType::KeyBased { name, .. } => {
                if let Some(name) = name {
                    format!(
                        "FORCE INDEX ({})",
                        readyset_sql::Dialect::MySQL.quote_identifier(name)
                    )
                } else {
                    "".to_string()
                }
            }
            SnapshotType::FullTableScan => "".to_string(),
        };
        let schema = match table_name.schema.as_ref() {
            Some(schema) => {
                format!("AND TABLE_SCHEMA = '{schema}'")
            }
            None => "".to_string(),
        };
        let collation_query = format!(
            "SELECT cl.ID FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS cl USING (COLLATION_NAME) WHERE c.TABLE_NAME = '{}' {} ORDER BY c.ORDINAL_POSITION",
            table_name.name,
            schema
        );
        let count_query = format!(
            "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_NAME = '{}' {}",
            table_name.name, schema
        );

        let snapshot_query_comment = snapshot_query_comment
            .map(|s| s.replace("/*", "").replace("*/", ""))
            .filter(|s| !s.is_empty())
            .map(|s| format!(" /*{s} */"))
            .unwrap_or_default();

        let (initial_query, bound_based_query) = match self {
            SnapshotType::KeyBased { ref keys, .. } => {
                let keys = keys
                    .iter()
                    .map(|key| key.display(readyset_sql::Dialect::MySQL).to_string())
                    .collect::<Vec<_>>();
                // ORDER BY col1 ASC, col2 ASC, col3 ASC
                let order_by = keys.join(" ASC, ") + " ASC";

                // ((col1 > ?) OR (col1 = ? AND col2 > ?) OR ( col1 = ? AND col2 = ? AND col3 > ?))
                let next_bound = (0..keys.len())
                    .map(|i| {
                        let conditions = (0..i).fold(String::new(), |mut acc, j| {
                            acc.push_str(&format!("{} = ? AND ", keys[j]));
                            acc
                        });
                        format!("({}{} > ?)", conditions, keys[i])
                    })
                    .collect::<Vec<_>>()
                    .join(" OR ");

                let next_bound = format!("({next_bound})");

                let initial_query = format!(
                    "SELECT{snapshot_query_comment} * FROM {} {} ORDER BY {} LIMIT {}",
                    table_name.display(readyset_sql::Dialect::MySQL),
                    force_index,
                    order_by,
                    MYSQL_BATCH_SIZE
                );
                let bound_based_query = format!(
                    "SELECT{snapshot_query_comment} * FROM {} {} WHERE {} ORDER BY {} LIMIT {}",
                    table_name.display(readyset_sql::Dialect::MySQL),
                    force_index,
                    next_bound,
                    order_by,
                    MYSQL_BATCH_SIZE
                );
                (initial_query, bound_based_query)
            }
            SnapshotType::FullTableScan => {
                let initial_query = format!(
                    "SELECT{snapshot_query_comment} * FROM {}",
                    table_name.display(readyset_sql::Dialect::MySQL)
                );
                (initial_query.clone(), initial_query)
            }
        };
        (
            count_query,
            initial_query,
            bound_based_query,
            collation_query,
        )
    }

    /// Given a row, compute the lower bound for the next query based on the keys and return it.
    /// Note that the lower bound is used twice. One get all the values greater or
    /// equal to the lower bound and the other to exclude the lower bound itself.
    ///
    /// Arguments:
    /// * `row` - The row to compute the lower bound from
    pub fn get_lower_bound(&self, row: &mysql::Row) -> ReadySetResult<Vec<mysql::Value>> {
        match self {
            SnapshotType::KeyBased {
                column_indices: cols,
                ..
            } => {
                // Calculate the required capacity using the triangular number formula
                let capacity = cols.len() * (cols.len() + 1) / 2;

                let mut new_lower_bound = Vec::with_capacity(capacity);

                // Collect the key values from the row
                let row_key_values: Vec<_> =
                    cols.iter().map(|col| row.get(*col).unwrap()).collect();

                // Push the key values into the new_lower_bound vector
                for i in 0..cols.len() {
                    new_lower_bound.extend_from_slice(&row_key_values[..=i]);
                }

                // Update the lower_bound with the new values
                Ok(new_lower_bound)
            }
            SnapshotType::FullTableScan => {
                unreachable!("Full table scan does not require a lower bound")
            }
        }
    }

    /// Check if the snapshot type is key based
    ///
    /// Returns:
    /// * True if the snapshot type is key based, false otherwise
    pub fn is_key_based(&self) -> bool {
        matches!(self, SnapshotType::KeyBased { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_query_no_comment() {
        let snapshot_type = SnapshotType::KeyBased {
            name: Some(SqlIdentifier::from("PRIMARY")),
            keys: vec![Column::from(SqlIdentifier::from("id"))],
            column_indices: vec![0],
        };
        let table_name = Relation {
            schema: Some(SqlIdentifier::from("test")),
            name: SqlIdentifier::from("test"),
        };
        let (count_query, initial_query, bound_based_query, collation_query) =
            snapshot_type.get_queries(&table_name, None);
        assert_eq!(count_query, "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_NAME = 'test' AND TABLE_SCHEMA = 'test'");
        assert_eq!(
            initial_query,
            "SELECT * FROM `test`.`test` FORCE INDEX (`PRIMARY`) ORDER BY `id` ASC LIMIT 100000"
        );
        assert_eq!(bound_based_query, "SELECT * FROM `test`.`test` FORCE INDEX (`PRIMARY`) WHERE ((`id` > ?)) ORDER BY `id` ASC LIMIT 100000");
        assert_eq!(collation_query, "SELECT cl.ID FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS cl USING (COLLATION_NAME) WHERE c.TABLE_NAME = 'test' AND TABLE_SCHEMA = 'test' ORDER BY c.ORDINAL_POSITION");

        let snapshot_type = SnapshotType::FullTableScan;
        let (_count_query, initial_query, bound_based_query, _collation_query) =
            snapshot_type.get_queries(&table_name, None);
        assert_eq!(bound_based_query, initial_query);
        assert_eq!(initial_query, "SELECT * FROM `test`.`test`");
    }

    #[test]
    fn snapshot_query_with_comment() {
        let snapshot_type = SnapshotType::KeyBased {
            name: Some(SqlIdentifier::from("PRIMARY")),
            keys: vec![Column::from(SqlIdentifier::from("id"))],
            column_indices: vec![0],
        };
        let table_name = Relation {
            schema: Some(SqlIdentifier::from("test")),
            name: SqlIdentifier::from("test"),
        };

        let (_count_query, initial_query, bound_based_query, _collation_query) =
            snapshot_type.get_queries(&table_name, Some("+ PT_KILL_BYPASS".to_string()));
        assert_eq!(initial_query, "SELECT /*+ PT_KILL_BYPASS */ * FROM `test`.`test` FORCE INDEX (`PRIMARY`) ORDER BY `id` ASC LIMIT 100000");
        assert_eq!(bound_based_query, "SELECT /*+ PT_KILL_BYPASS */ * FROM `test`.`test` FORCE INDEX (`PRIMARY`) WHERE ((`id` > ?)) ORDER BY `id` ASC LIMIT 100000");

        let snapshot_type = SnapshotType::FullTableScan;
        let (_count_query, initial_query, bound_based_query, _collation_query) =
            snapshot_type.get_queries(&table_name, Some("+ PT_KILL_BYPASS".to_string()));
        assert_eq!(
            initial_query,
            "SELECT /*+ PT_KILL_BYPASS */ * FROM `test`.`test`"
        );
        assert_eq!(bound_based_query, initial_query);
    }
}
