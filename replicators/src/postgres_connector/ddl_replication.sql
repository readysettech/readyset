CREATE SCHEMA IF NOT EXISTS readyset;

CREATE TABLE IF NOT EXISTS readyset.ddl_replication_log (
    "id" SERIAL PRIMARY KEY,
    -- create_table, drop_table, alter_table, create_view, drop_view
    "event_type" TEXT NOT NULL,
    "schema_name" TEXT NOT NULL ,
    "object_name" TEXT NOT NULL,
    "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

----

CREATE OR REPLACE FUNCTION readyset.replicate_drop()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO readyset.ddl_replication_log (event_type, schema_name, object_name)
    SELECT
      CASE
      WHEN object_type = 'table' THEN 'drop_table'
      WHEN object_type = 'view' THEN 'drop_view'
      END,
      schema_name,
      object_name
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type IN ('table', 'view');
END $$;

DROP EVENT TRIGGER IF EXISTS readyset_replicate_drop;
CREATE EVENT TRIGGER readyset_replicate_drop
    ON sql_drop
    WHEN TAG IN ('DROP TABLE', 'DROP VIEW')
    EXECUTE PROCEDURE readyset.replicate_drop();
