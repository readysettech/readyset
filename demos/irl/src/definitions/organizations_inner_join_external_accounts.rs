use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct OrganizationInnerJoinExternalAccounts {
    // All columns from `organizations`
    id: i64,                // int unsigned NOT NULL AUTO_INCREMENT,
    name: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    alpha_two_code: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    state_province: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    domains: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    country: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String, // timestamp NULL DEFAULT NULL,
    updated_at: String, // timestamp NULL DEFAULT NULL,
    logo: String,    // varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    user_id: i64,    // int DEFAULT NULL,
    o_type: i64,     // tinyint DEFAULT '0',
    content_level: i64, // tinyint(1) NOT NULL DEFAULT '0',
    slug: String,    // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    // Columns from `external_accounts`
    ea_user_id: i64,         // int unsigned NOT NULL,
    ea_organization_id: i64, // int DEFAULT NULL,
}

impl FromRow for OrganizationInnerJoinExternalAccounts {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(OrganizationInnerJoinExternalAccounts {
            id: get!(row, 0),
            name: get!(row, 1),
            alpha_two_code: get!(row, 2),
            state_province: get!(row, 3),
            domains: get!(row, 4),
            country: get!(row, 5),
            created_at: get!(row, 6),
            updated_at: get!(row, 7),
            logo: get!(row, 8),
            user_id: get!(row, 9),
            o_type: get!(row, 10),
            content_level: get!(row, 11),
            slug: get!(row, 12),
            ea_user_id: get!(row, 13),
            ea_organization_id: get!(row, 14),
        })
    }
}
