use itertools::Itertools;
use mysql_async as mysql;
use nom_sql::{Column, DialectDisplay};
use readyset_errors::{internal_err, ReadySetResult};

use super::utils::MYSQL_BATCH_SIZE;

/// The type of snapshot to be taken
/// KeyBased: Snapshot based on the primary key or unique key
/// FullTableScan: Snapshot the entire table
pub enum SnapshotType {
    KeyBased {
        keys: Vec<Column>,
        lower_bound: Option<Vec<mysql::Value>>,
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

        let keys = if let Some(pk) = cts.get_primary_key() {
            pk.get_columns()
        } else if let Some(uk) = cts.get_first_unique_key() {
            uk.get_columns()
        } else {
            return Ok(SnapshotType::FullTableScan);
        };

        Ok(SnapshotType::KeyBased {
            keys: keys.to_vec(),
            lower_bound: None,
        })
    }

    /// Get the lower bound for the next query
    /// Returns:
    /// * The lower bound
    /// Errors if the snapshot type is FullTableScan or the lower bound is not set
    pub fn get_lower_bound(&mut self) -> ReadySetResult<Vec<mysql::Value>> {
        match self {
            SnapshotType::KeyBased {
                ref mut lower_bound,
                ..
            } => {
                if let Some(lb) = lower_bound.take() {
                    Ok(lb)
                } else {
                    Err(internal_err!("Lower bound not set"))
                }
            }
            SnapshotType::FullTableScan => Err(internal_err!(
                "Full table scan does not require a lower bound"
            )),
        }
    }

    /// Generate the queries to be used for snapshotting the table, given the snapshot type
    ///
    /// Arguments:
    /// * `table` - The table to snapshot
    ///
    /// Returns:
    /// * A tuple containing the count query, the initial query, and the bound based query
    pub fn get_queries(&self, table: &readyset_client::Table) -> (String, String, String) {
        //TODO(marce): COUNT(1) Or COUNT(PK) might have better performance
        let count_query = format!(
            "SELECT COUNT(*) FROM {}",
            table.table_name().display(nom_sql::Dialect::MySQL)
        );
        let (initial_query, bound_based_query) = match self {
            SnapshotType::KeyBased { ref keys, .. } => {
                let keys = keys
                    .iter()
                    .map(|key| key.display(nom_sql::Dialect::MySQL).to_string())
                    .collect::<Vec<_>>();
                // ORDER BY col1 ASC, col2 ASC, col3 ASC
                let order_by = keys.join(" ASC, ") + " ASC";

                // col1 >= ? AND col2 >= ? AND col3 >= ?
                let next_bound = keys.join(" >= ? AND ").to_string() + " >= ?";

                // (col1, col2, col3) != (?, ?, ?)
                let exclude_lower_bound = keys.join(", ").to_string()
                    + ") != ("
                    + &keys
                        .iter()
                        .map(|_| '?')
                        .collect::<Vec<_>>()
                        .iter()
                        .join(", ");
                let initial_query = format!(
                    "SELECT * FROM {} ORDER BY {} LIMIT {}",
                    table.table_name().display(nom_sql::Dialect::MySQL),
                    order_by,
                    MYSQL_BATCH_SIZE
                );
                let bound_based_query = format!(
                    "SELECT * FROM {} WHERE {} AND ({}) ORDER BY {} LIMIT {}",
                    table.table_name().display(nom_sql::Dialect::MySQL),
                    next_bound,
                    exclude_lower_bound,
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

    /// Given a row, compute the lower bound for the next query based on the keys and update the
    /// lower bound. Note that the lower bound is used twice. One get all the values greater or
    /// equal to the lower bound and the other to exclude the lower bound itself.
    ///
    /// Arguments:
    /// * `row` - The row to compute the lower bound from
    pub fn set_lower_bound(&mut self, row: &mysql::Row) {
        match self {
            SnapshotType::KeyBased {
                ref keys,
                ref mut lower_bound,
            } => {
                let mut new_lower_bound = keys
                    .iter()
                    .map(|key| row.get(key.name.as_str()).unwrap())
                    .collect::<Vec<mysql::Value>>();
                new_lower_bound.append(&mut new_lower_bound.clone());
                *lower_bound = Some(new_lower_bound);
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
