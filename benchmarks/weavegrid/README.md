# Weavegrid demo

## Data generation
The demo supports generating data for the weavegrid data model,
with the CREATE TABLE ddl defined in `weavegrid_db.sql`. Relevant
views can be created with `weavegrid_create_views.sql`. 

Data generation supports specifying the number of rows to generate
for each table through the `--table_rows` argument. 

Sample usage:

1. `mysql -h 127.0.0.1 < weavegrid_db.sql`

2. `mysql -h 127.0.0.1 < weavegrid_create_views.sql`

3. `cargo run --bin data_generator -- --database-url mysql://127.0.0.1 --table_rows 1000`

4. Query the views as needed.

