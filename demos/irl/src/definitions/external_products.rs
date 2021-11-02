use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct ExternalProduct {
    // All columns from `external_products`
    id: i64,                     // int unsigned NOT NULL AUTO_INCREMENT,
    payment_account_id: i64,     // int NOT NULL,
    price_in_cents: i64,         // int NOT NULL,
    currency: String,            // varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    payment_type: i64,           // tinyint NOT NULL,
    table_name: String,          // varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    row_id: i64,                 // int NOT NULL,
    external_product_id: String, // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    external_price_id: String,   // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    is_for_acceptance: i64,      // tinyint DEFAULT NULL,
    created_at: String,          // timestamp NULL DEFAULT NULL,
    updated_at: String,          // timestamp NULL DEFAULT NULL,
}

impl FromRow for ExternalProduct {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(ExternalProduct {
            id: get!(row, 0),
            payment_account_id: get!(row, 1),
            price_in_cents: get!(row, 2),
            currency: get!(row, 3),
            payment_type: get!(row, 4),
            table_name: get!(row, 5),
            row_id: get!(row, 6),
            external_product_id: get!(row, 7),
            external_price_id: get!(row, 8),
            is_for_acceptance: get!(row, 9),
            created_at: get!(row, 10),
            updated_at: get!(row, 11),
        })
    }
}
