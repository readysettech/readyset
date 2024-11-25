use mysql_async::{self as mysql, Value};
use nom_sql::{Column, DialectDisplay, SqlIdentifier};
use readyset_errors::ReadySetResult;

use super::utils::MYSQL_BATCH_SIZE;

/// The type of snapshot to be taken
/// KeyBased: Snapshot based on the primary key or unique key
/// FullTableScan: Snapshot the entire table
pub enum SnapshotType {
    KeyBased {
        name: Option<SqlIdentifier>,
        keys: Vec<Column>,
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

        Ok(SnapshotType::KeyBased {
            name: name.clone(),
            keys: keys.to_vec(),
        })
    }

    /// Generate the queries to be used for snapshotting the table, given the snapshot type
    ///
    /// Arguments:
    /// * `table` - The table to snapshot
    ///
    /// Returns:
    /// * A tuple containing the count query, the initial query, and the bound based query
    pub fn get_queries(&self, table: &readyset_client::Table) -> (String, String, String) {
        let force_index = match self {
            SnapshotType::KeyBased { name, .. } => {
                if let Some(name) = name {
                    format!(
                        "FORCE INDEX ({})",
                        nom_sql::Dialect::MySQL.quote_identifier(name)
                    )
                } else {
                    "".to_string()
                }
            }
            SnapshotType::FullTableScan => "".to_string(),
        };
        let schema = match table.table_name().schema.as_ref() {
            Some(schema) => {
                format!("AND TABLE_SCHEMA = '{}'", schema)
            }
            None => "".to_string(),
        };
        let count_query = format!(
            "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_NAME = '{}' {}",
            table.table_name().name,
            schema
        );
        let (initial_query, bound_based_query) = match self {
            SnapshotType::KeyBased { ref keys, .. } => {
                let keys = keys
                    .iter()
                    .map(|key| key.display(nom_sql::Dialect::MySQL).to_string())
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

                let next_bound = format!("({})", next_bound);

                let initial_query = format!(
                    "SELECT * FROM {} {} ORDER BY {} LIMIT {}",
                    table.table_name().display(nom_sql::Dialect::MySQL),
                    force_index,
                    order_by,
                    MYSQL_BATCH_SIZE
                );
                let bound_based_query = format!(
                    "SELECT * FROM {} {} WHERE {} ORDER BY {} LIMIT {}",
                    table.table_name().display(nom_sql::Dialect::MySQL),
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
                    table.table_name().display(nom_sql::Dialect::MySQL)
                );
                (initial_query.clone(), initial_query)
            }
        };
        (count_query, initial_query, bound_based_query)
    }

    /// Given a row, compute the lower bound for the next query based on the keys and return it.
    /// Note that the lower bound is used twice. One get all the values greater or
    /// equal to the lower bound and the other to exclude the lower bound itself.
    ///
    /// Arguments:
    /// * `row` - The row to compute the lower bound from
    pub fn get_lower_bound(&mut self, row: &mysql::Row) -> ReadySetResult<Vec<mysql::Value>> {
        match self {
            SnapshotType::KeyBased { ref keys, .. } => {
                // Calculate the required capacity using the triangular number formula
                let capacity = keys.len() * (keys.len() + 1) / 2;

                let mut new_lower_bound = Vec::with_capacity(capacity);

                // Collect the key values from the row
                let row_key_values: Vec<Value> = keys
                    .iter()
                    .map(|key| row.get(key.name.as_ref()).unwrap())
                    .collect();

                // Push the key values into the new_lower_bound vector
                for i in 0..keys.len() {
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
