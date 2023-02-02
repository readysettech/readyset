CREATE SCHEMA IF NOT EXISTS readyset;
GRANT USAGE ON SCHEMA readyset TO PUBLIC;

CREATE OR REPLACE FUNCTION readyset.is_pre14()
RETURNS boolean
LANGUAGE plpgsql
AS $$
    DECLARE ver integer;
BEGIN
    SELECT current_setting('server_version_num') INTO ver;
    RETURN ver < 140000;
END $$;
----

DO $$
BEGIN
IF readyset.is_pre14() THEN
    CREATE TABLE IF NOT EXISTS readyset.ddl_replication_log AS
    WITH t (ddl) AS (VALUES ('')) SELECT * FROM t;
    ALTER TABLE readyset.ddl_replication_log REPLICA IDENTITY FULL;
END IF;
END $$;

CREATE OR REPLACE FUNCTION readyset.replicate_create_table()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    create_message text;
    needs_replica_identity bool;
BEGIN
    SELECT count(*) = 0
    INTO needs_replica_identity
    FROM pg_index i
    JOIN pg_event_trigger_ddl_commands() o ON i.indrelid = o.objid
    JOIN pg_class c ON o.objid = c.oid
    WHERE i.indisprimary
    AND c.relreplident = 'd'
    AND o.object_type = 'table'
    AND o.schema_name != 'pg_temp';

    IF needs_replica_identity THEN
        -- Prevent our ALTER TABLE trigger from running on this command
        SET LOCAL readyset.current_command_is_replica_identity TO true;
        EXECUTE format(
            'ALTER TABLE "%s"."%s" REPLICA IDENTITY FULL',
            (SELECT schema_name FROM pg_event_trigger_ddl_commands()),
            (SELECT relname
             FROM pg_class, pg_event_trigger_ddl_commands() object
             WHERE oid = object.objid)
        );
        SET LOCAL readyset.current_command_is_replica_identity TO false;
    END IF;

    SELECT
    json_build_object(
        'schema', object.schema_name,
        'data', json_build_object('CreateTable', json_build_object(
            'name', (SELECT relname FROM pg_class WHERE oid = object.objid),
            'columns', (
                SELECT json_agg(json_build_object(
                    'name', attr.attname,
                    'column_type', pg_catalog.format_type(
                        attr.atttypid,
                        attr.atttypmod
                    ),
                    'not_null', attr.attnotnull
                ) ORDER BY attr.attnum)
                FROM pg_catalog.pg_attribute attr
                WHERE attr.attrelid = object.objid
                AND attr.attnum > 0
                AND NOT attr.attisdropped
            ),
            'constraints', (
                SELECT coalesce(
                    json_agg(json_build_object('definition', def)),
                    '[]'
                )
                FROM (
                    SELECT DISTINCT
                        pg_catalog.pg_get_constraintdef(con.oid, TRUE) AS def
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
                ) def
            )
        ))
    )
    INTO create_message
    FROM pg_event_trigger_ddl_commands() object
    WHERE object.object_type = 'table'
    AND object.schema_name != 'pg_temp';

    IF readyset.is_pre14() THEN
        UPDATE readyset.ddl_replication_log SET "ddl" = create_message;
    ELSE
        PERFORM pg_logical_emit_message(true, 'readyset', create_message);
    END IF;
END $$;


CREATE OR REPLACE FUNCTION readyset.replicate_alter_table()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    alter_message text;
    query text;
BEGIN
    IF coalesce(
        nullif(
            current_setting(
                'readyset.current_command_is_replica_identity',
                true
            ),
            ''
        )::boolean,
        false
    ) THEN
        RETURN;
    END IF;

    SELECT current_query() INTO query;

    SELECT
    json_build_object(
        'schema', object.schema_name,
        'data', json_build_object(
            'AlterTable',
            json_build_object(
                'name', cls.relname,
                'statement', query
            )
        )
    )
    INTO alter_message
    FROM pg_event_trigger_ddl_commands() object
    JOIN pg_class cls ON object.objid = cls.oid
    WHERE object.object_type = 'table';

    IF readyset.is_pre14() THEN
        UPDATE readyset.ddl_replication_log SET "ddl" = alter_message;
    ELSE
        PERFORM pg_logical_emit_message(true, 'readyset', alter_message);
    END IF;
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
        'schema', object.schema_name,
        'data', json_build_object('CreateView', format(
            'CREATE VIEW "%s"."%s" AS %s',
            v.schemaname,
            cls.relname,
            v.definition
        ))
    )
    INTO create_message
    FROM pg_event_trigger_ddl_commands() object
    JOIN pg_catalog.pg_class cls ON object.objid = cls.oid
    JOIN pg_catalog.pg_views v
         ON object.schema_name = v.schemaname
         AND cls.relname = v.viewname
    WHERE object.object_type = 'view';

    IF readyset.is_pre14() THEN
        UPDATE readyset.ddl_replication_log SET "ddl" = create_message;
    ELSE
        PERFORM pg_logical_emit_message(true, 'readyset', create_message);
    END IF;
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
        'schema', schema_name,
        'data', json_build_object('Drop', object_name)
    )
    INTO drop_message
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type IN ('table', 'view', 'type')
    AND schema_name != 'pg_temp';

    IF readyset.is_pre14() THEN
        UPDATE readyset.ddl_replication_log SET "ddl" = drop_message;
    ELSE
        PERFORM pg_logical_emit_message(true, 'readyset', drop_message);
    END IF;
END $$;

----

CREATE OR REPLACE FUNCTION readyset.replicate_create_type()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    create_message text;
BEGIN
    SELECT
        json_build_object(
            'schema', schema_name,
            'data', json_build_object('CreateType', json_build_object(
                'oid', t.oid::int,
                'array_oid', t.typarray::int,
                'name', t.typname,
                'variants', json_agg(json_build_object(
                    'oid', e.oid::int,
                    'label', e.enumlabel
                ) ORDER BY e.enumsortorder ASC)
            ))
        )
    INTO create_message
    FROM pg_event_trigger_ddl_commands() object
    JOIN pg_catalog.pg_type t
      ON t.oid = object.objid
    JOIN pg_catalog.pg_enum e
      ON e.enumtypid = t.oid
    GROUP BY object.objid, object.schema_name, t.oid, t.typname, t.typarray;

    IF readyset.is_pre14() THEN
        UPDATE readyset.ddl_replication_log SET "ddl" = create_message;
    ELSE
        PERFORM pg_logical_emit_message(true, 'readyset', create_message);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION readyset.pre_alter_type()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- PostgreSQL doesn't give us any information about *which* types are being
  -- modified in `ddl_command_start`, so we just put all variants for all enums
  -- in a temp table, under the assumption that (hopefully) there aren't going
  -- to be that many in practice
  CREATE TEMP TABLE pg_enum_original
  AS SELECT * FROM pg_catalog.pg_enum;
END $$;

CREATE OR REPLACE FUNCTION readyset.replicate_alter_type()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    alter_message text;
BEGIN
    SELECT
        json_build_object(
            'schema', object.schema_name,
            'data', json_build_object('AlterType', json_build_object(
                'name', t.typname,
                'oid', t.oid::INT,
                'variants', json_agg(json_build_object(
                    'oid', e.oid::int,
                    'label', e.enumlabel
                ) ORDER BY e.enumsortorder ASC),
                'original_variants', (SELECT json_agg(json_build_object(
                    'oid', e_orig.oid::int,
                    'label', e_orig.enumlabel
                ) ORDER BY e_orig.enumsortorder ASC)
                FROM pg_enum_original e_orig
                WHERE e_orig.enumtypid = t.oid)
            ))
        )
    INTO alter_message
    FROM pg_event_trigger_ddl_commands() object
    JOIN pg_catalog.pg_type t
      ON t.oid = object.objid
    JOIN pg_catalog.pg_enum e
      ON e.enumtypid = t.oid
    GROUP BY object.objid, object.schema_name, t.oid, t.typname;

    DROP TABLE pg_enum_original;

    IF readyset.is_pre14() THEN
        UPDATE readyset.ddl_replication_log SET "ddl" = alter_message;
    ELSE
        PERFORM pg_logical_emit_message(true, 'readyset', alter_message);
    END IF;
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
    WHEN TAG IN ('DROP TABLE', 'DROP VIEW', 'DROP TYPE')
    EXECUTE PROCEDURE readyset.replicate_drop();

DROP EVENT TRIGGER IF EXISTS readyset_replicate_create_type;
CREATE EVENT TRIGGER readyset_replicate_create_type
    ON ddl_command_end
    WHEN TAG IN ('CREATE TYPE')
    EXECUTE PROCEDURE readyset.replicate_create_type();

DROP EVENT TRIGGER IF EXISTS readyset_pre_alter_type;
CREATE EVENT TRIGGER readyset_pre_alter_type
    ON ddl_command_start
    WHEN TAG IN ('ALTER TYPE')
    EXECUTE PROCEDURE readyset.pre_alter_type();

DROP EVENT TRIGGER IF EXISTS readyset_replicate_alter_type;
CREATE EVENT TRIGGER readyset_replicate_alter_type
    ON ddl_command_end
    WHEN TAG IN ('ALTER TYPE')
    EXECUTE PROCEDURE readyset.replicate_alter_type();
