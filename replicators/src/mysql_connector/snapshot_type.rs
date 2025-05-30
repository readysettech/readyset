use mysql_async::{self as mysql};
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{Column, SqlIdentifier};
use readyset_sql::DialectDisplay;

use super::utils::MYSQL_BATCH_SIZE;

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

        let (keys, name) = if let Some(pk) = cts.get_primary_key() {
            (pk.get_columns(), &Some(SqlIdentifier::from("PRIMARY")))
        } else if let Some(uk) = cts.get_first_unique_key() {
            (uk.get_columns(), uk.index_name())
        } else {
            return Ok(SnapshotType::FullTableScan);
        };

        let mut column_indices = Vec::new();
        for k in keys {
            let name = k.name.to_lowercase();
            let Some(col) = cts.fields.iter().enumerate().find_map(|(i, f)| {
                if name == f.column.name.to_lowercase() {
                    Some(i)
                } else {
                    None
                }
            }) else {
                return Err(readyset_errors::ReadySetError::NoSuchColumn(name));
            };
            column_indices.push(col);
        }
        assert_eq!(keys.len(), column_indices.len());

        Ok(SnapshotType::KeyBased {
            name: name.clone(),
            keys: keys.to_vec(),
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
    pub fn get_queries(&self, table: &readyset_client::Table) -> (String, String, String, String) {
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
        let schema = match table.table_name().schema.as_ref() {
            Some(schema) => {
                format!("AND TABLE_SCHEMA = '{schema}'")
            }
            None => "".to_string(),
        };
        let collation_query = format!(
            "SELECT cl.ID FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS cl USING (COLLATION_NAME) WHERE c.TABLE_NAME = '{}' {} ORDER BY c.ORDINAL_POSITION",
            table.table_name().name,
            schema
        );
        let count_query = format!(
            "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_NAME = '{}' {}",
            table.table_name().name,
            schema
        );
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
                    "SELECT * FROM {} {} ORDER BY {} LIMIT {}",
                    table.table_name().display(readyset_sql::Dialect::MySQL),
                    force_index,
                    order_by,
                    MYSQL_BATCH_SIZE
                );
                let bound_based_query = format!(
                    "SELECT * FROM {} {} WHERE {} ORDER BY {} LIMIT {}",
                    table.table_name().display(readyset_sql::Dialect::MySQL),
                    force_index,
                    next_bound,
                    order_by,
                    MYSQL_BATCH_SIZE
                );
                (initial_query, bound_based_query)
            }
            SnapshotType::FullTableScan => {
                let initial_query = format!(
                    "SELECT * FROM {}",
                    table.table_name().display(readyset_sql::Dialect::MySQL)
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
    pub fn get_lower_bound(&mut self, row: &mysql::Row) -> ReadySetResult<Vec<mysql::Value>> {
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
