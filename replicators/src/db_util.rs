//! Database Utilities
//! Contains helpers for determining the schemas and tables of a database for use in replication
use std::collections::HashMap;

use nom_sql::Dialect;
use readyset_sql_passes::anonymize::{Anonymize, Anonymizer};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};

#[derive(Debug)]
pub struct DatabaseSchemas {
    schemas: HashMap<String, CreateSchema>,
}

impl DatabaseSchemas {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn extend_create_schema_for_table(
        &mut self,
        create_schema_name: String,
        table_name: String,
        create_table: String,
        dialect: Dialect,
    ) {
        let create_schema = self
            .schemas
            .entry(create_schema_name.clone())
            .or_insert_with(|| CreateSchema::new(create_schema_name, dialect));
        create_schema.add_table_create(table_name, create_table);
    }

    pub fn extend_create_schema_for_view(
        &mut self,
        create_schema_name: String,
        view_name: String,
        create_view: String,
        dialect: Dialect,
    ) {
        let create_schema = self
            .schemas
            .entry(create_schema_name.clone())
            .or_insert_with(|| CreateSchema::new(create_schema_name, dialect));
        create_schema.add_view_create(view_name, create_view);
    }

    pub async fn send_schemas(self, telemetry_sender: &TelemetrySender) {
        for (_, schema) in self.schemas.into_iter() {
            schema.send_schemas(telemetry_sender).await;
        }
    }
}

impl ToString for DatabaseSchemas {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug)]
pub struct CreateSchema {
    /// name of the schema
    name: String,
    /// Map of table name to table create statements
    table_creates: HashMap<String, String>,
    /// Map of view name to view create statements
    view_creates: HashMap<String, String>,
    /// Postgres or MySQL
    dialect: Dialect,
}

impl CreateSchema {
    pub fn new(name: String, dialect: Dialect) -> Self {
        Self {
            name,
            dialect,
            table_creates: HashMap::new(),
            view_creates: HashMap::new(),
        }
    }

    pub fn add_table_create(&mut self, table_name: String, table_create: String) {
        self.table_creates.insert(table_name, table_create);
    }

    pub fn add_view_create(&mut self, view_name: String, view_create: String) {
        self.view_creates.insert(view_name, view_create);
    }

    /// This relies on `sysvar_sql_quote_show_create` being set for mysql, which it is by default
    /// Anonymizes any symbols in the schema
    /// Symbols are identified by being in `` quotes.
    fn anonymize(&mut self) {
        let mut anonymizer = Anonymizer::new();

        self.anonymize_name(&mut anonymizer);
        self.anonymize_tables(&mut anonymizer);
        self.anonymize_views(&mut anonymizer);
    }

    fn anonymize_name(&mut self, anonymizer: &mut Anonymizer) {
        self.name.anonymize(anonymizer);
    }

    fn anonymize_tables(&mut self, anonymizer: &mut Anonymizer) {
        for (_, table) in self.table_creates.iter_mut() {
            tracing::trace!("create table: {table:?}");
            if let Dialect::PostgreSQL = self.dialect {
                // HACK: strip out the backticks from these since they aren't valid in PostgreSQL
                // until we handle formatting by dialect correctly
                strip_backticks(table);
            }
            *table = match nom_sql::parse_create_table(self.dialect, table.clone()) {
                Ok(mut parsed_table) => {
                    parsed_table.anonymize(anonymizer);
                    parsed_table.to_string()
                }
                // If we fail to parse, fully anonymize the statement.
                Err(_) => "<anonymized: create table failed to parse>".to_string(),
            }
        }
    }

    fn anonymize_views(&mut self, anonymizer: &mut Anonymizer) {
        for (_, view) in self.view_creates.iter_mut() {
            tracing::trace!("create view: {view:?}");
            *view = match nom_sql::parse_create_view(self.dialect, view.clone()) {
                Ok(mut parsed_view) => {
                    parsed_view.anonymize(anonymizer);
                    parsed_view.to_string()
                }
                // If we fail to parse, fully anonymize the statement.
                Err(_) => "<anonymized: create view failed to parse>".to_string(),
            }
        }
    }

    pub async fn send_create(create: String, telemetry_sender: &TelemetrySender) {
        match telemetry_sender.send_event_with_payload(
            TelemetryEvent::Schema,
            TelemetryBuilder::new().schema(create).build(),
        ) {
            Ok(_) => {}
            Err(_) => tracing::warn!("Failed to send create schema telemetry"),
        }
    }

    pub async fn send_schemas(mut self, telemetry_sender: &TelemetrySender) {
        self.anonymize();

        // Sengment has a 32KB max payload, so send the schemas 1 at a time.
        for (_, table) in self.table_creates.into_iter() {
            Self::send_create(table, telemetry_sender).await;
        }

        for (_, view) in self.view_creates.into_iter() {
            Self::send_create(view, telemetry_sender).await;
        }
    }
}

impl ToString for CreateSchema {
    fn to_string(&self) -> String {
        format!("{self:?}")
    }
}

fn strip_backticks(table: &mut String) {
    table.remove_matches("`");
}

#[cfg(test)]
mod tests {
    use nom_sql::SqlIdentifier;

    use super::*;

    #[test]
    fn test_anonymize_full_string() {
        let mut anonymizer = Anonymizer::new();
        let mut name = SqlIdentifier::from("table1");
        anonymizer.replace(&mut name);
        assert_eq!(SqlIdentifier::from("anon_id_0"), name);

        // A second run with the same input produces the same mapping
        let mut name = SqlIdentifier::from("table1");
        anonymizer.replace(&mut name);
        assert_eq!(SqlIdentifier::from("anon_id_0"), name);

        // But another input produces the next output
        let mut name = SqlIdentifier::from("table2");
        anonymizer.replace(&mut name);
        assert_eq!(SqlIdentifier::from("anon_id_1"), name);
    }

    #[test]
    fn test_anonymize_create_schema() {
        readyset_tracing::init_test_logging();

        let mut create_schema = CreateSchema::new("foobar".to_string(), Dialect::MySQL);

        let users_create_table = "CREATE TABLE `Users` ( `UserId` int NOT NULL,
                                   `PostId` int DEFAULT NULL,
                                   PRIMARY KEY (`UserId`),
                                   KEY `PostId` (`PostId`),
                                   CONSTRAINT `Users_ibfk_1` FOREIGN KEY (`PostId`) REFERENCES `Posts` (`PostID`)
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
            .to_string();
        create_schema.add_table_create("Users".to_string(), users_create_table);

        let posts_create_table = "CREATE TABLE `Posts` (
                                  `PostID` int NOT NULL,
                                  PRIMARY KEY (`PostID`)
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
            .to_string();
        create_schema.add_table_create("Posts".to_string(), posts_create_table);

        let view_create =
            "CREATE VIEW `foobar` FROM SELECT * FROM `Posts` WHERE `1` = `1`".to_string();
        create_schema.add_view_create("foobar".to_string(), view_create);

        let should_be_scrubbed = vec![
            "`foobar`",
            "`Users`",
            "`UserId`",
            "`Users_ibfk_1`",
            "`Posts`",
            "`PostId`",
            "`1`",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        create_schema.anonymize();

        // We don't anonymize the name keys, so iterate over the values for assertion
        for (_, table) in create_schema.table_creates {
            for scrubbed in should_be_scrubbed.iter() {
                tracing::info!("{:?}", table.to_string());
                assert!(!table.to_string().contains(scrubbed.as_str()));
            }
        }

        for (_, view) in create_schema.view_creates {
            for scrubbed in should_be_scrubbed.iter() {
                tracing::info!("{:?}", view.to_string());
                assert!(!view.to_string().contains(scrubbed.as_str()));
            }
        }
    }

    #[test]
    fn create_table_parse_failed_fully_anonymized() {
        readyset_tracing::init_test_logging();

        let mut create_schema = CreateSchema::new("table_fail_parse".to_string(), Dialect::MySQL);
        let users_create_table = "CREATE TABLE not valid syntax will fail to parse".to_string();
        create_schema.add_table_create("Users".to_string(), users_create_table);
        create_schema.anonymize();
        for (_, table_create) in create_schema.table_creates {
            assert_eq!(
                "<anonymized: create table failed to parse>".to_string(),
                table_create
            );
        }
    }

    #[test]
    fn create_view_parse_failed_fully_anonymized() {
        readyset_tracing::init_test_logging();

        let mut create_schema = CreateSchema::new("view_fail_parse".to_string(), Dialect::MySQL);
        let view_create = "CREATE VIEW not valid syntax will fail to parse".to_string();
        create_schema.add_view_create("foobar".to_string(), view_create);
        create_schema.anonymize();
        for (_, view_create) in create_schema.view_creates {
            assert_eq!(
                "<anonymized: create view failed to parse>".to_string(),
                view_create
            );
        }
    }
}
