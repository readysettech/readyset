CREATE SCHEMA IF NOT EXISTS readyset;

----

CREATE OR REPLACE FUNCTION readyset.replicate_create_table()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    create_message text;
BEGIN
    SELECT
    json_build_object(
        'operation', 'create_table',
        'schema', object.schema_name,
        'object', replace(
            replace(object.object_identity, object.SCHEMA_NAME || '.', ''),
            -- un-quote identifiers if necessary
            '"',
            ''
        ),
        'statement', format(
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
    )
    INTO create_message
    FROM pg_event_trigger_ddl_commands() object
    WHERE object.object_type = 'table';

    PERFORM pg_logical_emit_message(true, 'readyset', create_message);
END $$;


CREATE OR REPLACE FUNCTION readyset.replicate_alter_table()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    alter_message text;
    query text;
BEGIN
    SELECT current_query() INTO query;
    SELECT
    json_build_object(
        'operation', 'alter_table',
        'schema', object.schema_name,
        'object', replace(
            replace(object.object_identity, object.SCHEMA_NAME || '.', ''),
            -- un-quote identifiers if necessary
            '"',
            ''
        ),
        'statement', query
    )
    INTO alter_message
    FROM pg_event_trigger_ddl_commands() object
    WHERE object.object_type = 'table';

    PERFORM pg_logical_emit_message(true, 'readyset', alter_message);
END $$;

----

CREATE OR REPLACE FUNCTION readyset.replicate_create_view()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    create_message text;
BEGIN
    SELECT
    json_build_object(
        'operation', 'create_view',
        'schema', object.schema_name,
        'object', cls.relname,
        'statement', format(
            'CREATE VIEW "%s"."%s" AS %s',
            v.schemaname,
            cls.relname,
            v.definition
        )
    )
    INTO create_message
    FROM pg_event_trigger_ddl_commands() object
    JOIN pg_catalog.pg_class cls ON object.objid = cls.oid
    JOIN pg_catalog.pg_views v
         ON object.schema_name = v.schemaname
         AND cls.relname = v.viewname
    WHERE object.object_type = 'view';

    PERFORM pg_logical_emit_message(true, 'readyset', create_message);
END $$;

----

CREATE OR REPLACE FUNCTION readyset.replicate_drop()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    drop_message text;
BEGIN
    SELECT
    json_build_object(
        'operation', CASE
                       WHEN object_type = 'table' THEN 'drop_table'
                       WHEN object_type = 'view' THEN 'drop_view'
                     END,
        'schema', schema_name,
        'object', object_name
    )
    INTO drop_message
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type IN ('table', 'view');

    PERFORM pg_logical_emit_message(true, 'readyset', drop_message);
END $$;

----

DROP EVENT TRIGGER IF EXISTS readyset_replicate_create_table;
CREATE EVENT TRIGGER readyset_replicate_create_table
    ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE PROCEDURE readyset.replicate_create_table();

DROP EVENT TRIGGER IF EXISTS readyset_replicate_alter_table;
CREATE EVENT TRIGGER readyset_replicate_alter_table
    ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE')
    EXECUTE PROCEDURE readyset.replicate_alter_table();

DROP event TRIGGER IF EXISTS readyset_replicate_create_view;
CREATE EVENT TRIGGER readyset_replicate_create_view
    ON ddl_command_end
    WHEN TAG IN ('CREATE VIEW')
    EXECUTE PROCEDURE readyset.replicate_create_view();

DROP EVENT TRIGGER IF EXISTS readyset_replicate_drop;
CREATE EVENT TRIGGER readyset_replicate_drop
    ON sql_drop
    WHEN TAG IN ('DROP TABLE', 'DROP VIEW')
    EXECUTE PROCEDURE readyset.replicate_drop();
