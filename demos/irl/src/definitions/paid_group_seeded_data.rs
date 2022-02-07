use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct PaidGroupSeededData {
    // All columns from `paid_group_seeded_data`
    id: i64,             // bigint unsigned NOT NULL AUTO_INCREMENT,
    group_id: i64,       // int NOT NULL,
    price_in_cents: i64, // int NOT NULL,
    created_at: String,  // timestamp NULL DEFAULT NULL,
    updated_at: String,  // timestamp NULL DEFAULT NULL,
    was_processed: i64,  // tinyint NOT NULL DEFAULT '0',
}

impl FromRow for PaidGroupSeededData {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(PaidGroupSeededData {
            id: get!(row, 0),
            group_id: get!(row, 1),
            price_in_cents: get!(row, 2),
            created_at: get!(row, 3),
            updated_at: get!(row, 4),
            was_processed: get!(row, 5),
        })
    }
}
