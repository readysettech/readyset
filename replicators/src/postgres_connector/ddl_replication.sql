CREATE SCHEMA IF NOT EXISTS readyset;

CREATE TABLE IF NOT EXISTS readyset.ddl_replication_log (
    "id" SERIAL PRIMARY KEY,
    -- create_table, drop_table, alter_table, create_view, drop_view
    "event_type" TEXT NOT NULL,
    "schema_name" TEXT,
    "object_name" TEXT NOT NULL,
    "create_table_ddl" TEXT, -- Only set for event_type='create_table'
    "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

----

CREATE OR REPLACE FUNCTION readyset.replicate_create_table()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO readyset.ddl_replication_log
        (event_type, schema_name, object_name, create_table_ddl)
    SELECT
        'create_table',
        object.schema_name,
        replace(
            replace(object.object_identity, object.SCHEMA_NAME || '.', ''),
            -- un-quote identifiers if necessary
            '"',
            ''
        ),
        format(
            'CREATE TABLE "%s" (%s %s)',
            (SELECT relname FROM pg_class WHERE oid = object.objid),
            (
                SELECT string_agg(
                    format(
                        '%s %s%s',
                        attr.attname,
                        pg_catalog.format_type(attr.atttypid, attr.atttypmod),
                        CASE WHEN attr.attnotnull THEN ' NOT NULL' ELSE '' END
                    ),
                    ', '
                    ORDER BY attr.attnum
                )
                FROM pg_catalog.pg_attribute attr
                WHERE attr.attrelid = object.objid
                AND attr.attnum > 0
                AND NOT attr.attisdropped
            ),
            ', ' || (
                SELECT string_agg(
                    DISTINCT pg_catalog.pg_get_constraintdef(con.oid, TRUE),
                    ', '
                )
                FROM
                    pg_catalog.pg_class cls,
                    pg_catalog.pg_class cls_index,
                    pg_catalog.pg_index idx
                LEFT JOIN pg_catalog.pg_constraint con
                    ON con.conrelid = idx.indrelid
                    AND con.conindid = idx.indexrelid
                    AND con.contype IN ('f', 'p', 'u')
                WHERE cls.oid = object.objid
                    AND cls.oid = idx.indrelid
                    AND idx.indexrelid = cls_index.oid
                    AND pg_catalog.pg_get_constraintdef(con.oid, TRUE)
                        IS NOT NULL
            )
        )
    FROM pg_event_trigger_ddl_commands() object
    WHERE object.object_type = 'table';
END $$;

DROP EVENT TRIGGER IF EXISTS readyset_replicate_create_table;
CREATE EVENT TRIGGER readyset_replicate_create_table
    ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE PROCEDURE readyset.replicate_create_table();

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
