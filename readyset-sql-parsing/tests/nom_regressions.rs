use readyset_sql::Dialect;
use readyset_sql_parsing::{ParsingPreset, parse_expr, parse_query_with_config};

mod utils;

#[test]
fn alter_both() {
    check_parse_both!("ALTER TABLE employees ADD Email varchar(255), ADD snailmail TEXT");
    check_parse_both!("ALTER TABLE flags CHANGE time created_at DATETIME NOT NULL");
    check_parse_both!("ALTER TABLE t REPLICA IDENTITY DEFAULT");
    check_parse_both!("ALTER TABLE t REPLICA IDENTITY FULL");
    check_parse_both!("ALTER TABLE t REPLICA IDENTITY USING INDEX asdf");
    check_parse_both!("ALTER TABLE t RENAME COLUMN x TO y");
}

#[test]
fn alter_mysql() {
    check_parse_mysql!("ALTER TABLE `t` ADD COLUMN `c` INT(32)");
    check_parse_mysql!("ALTER TABLE `t` ADD COLUMN `c` INT");
    check_parse_mysql!("ALTER TABLE `t` ADD COLUMN `c` INT, ADD COLUMN `d` TEXT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c CASCADE");
    check_parse_mysql!("ALTER TABLE `t` ALTER COLUMN c SET DEFAULT 'foo'");
    check_parse_mysql!("ALTER TABLE `t` ALTER COLUMN c DROP DEFAULT");
    check_parse_mysql!("ALTER TABLE t MODIFY f VARCHAR(255) NOT NULL PRIMARY KEY");
    check_parse_mysql!("ALTER TABLE t CHANGE f `modify` DATETIME");
    check_parse_mysql!("ALTER TABLE `t` CHANGE COLUMN `f` `modify` DATETIME");
    check_parse_mysql!(
        "alter table `posts_likes` add primary key `posts_likes_post_id_user_id_primary`(`post_id`, `user_id`)"
    );
    check_parse_mysql!("alter table `flags` add index `flags_created_at_index`(`created_at`)");
    check_parse_mysql!(
        "alter table `flags` add constraint `flags_post_id_foreign` foreign key (`post_id`) references `posts` (`id`) on delete cascade"
    );
    check_parse_mysql!(
        "alter table `discussion_user` add `subscription` enum('follow', 'ignore') null"
    );
    check_parse_mysql!(
        "ALTER TABLE `discussion_user` ADD COLUMN `subscription` ENUM('follow', 'ignore') NULL"
    );
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c, ALGORITHM = INPLACE, LOCK = DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM=INPLACE,LOCK=DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c, ALGORITHM INPLACE, LOCK DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM INPLACE,LOCK DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM INSTANT,LOCK DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM INPLACE");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM INSTANT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM = INPLACE");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,ALGORITHM = INSTANT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,LOCK = DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` DROP COLUMN c,LOCK DEFAULT");
    check_parse_mysql!("ALTER TABLE `t` ADD COLUMN c INT, ALGORITHM = INSTANT");
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON DELETE CASCADE"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON DELETE SET NULL"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON DELETE RESTRICT"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON DELETE NO ACTION"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON DELETE SET DEFAULT"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON UPDATE CASCADE"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON UPDATE SET NULL"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON UPDATE RESTRICT"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON UPDATE NO ACTION"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON UPDATE SET DEFAULT"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON DELETE CASCADE ON UPDATE CASCADE"
    );
    check_parse_mysql!(
        "ALTER TABLE `t` ADD CONSTRAINT `c` FOREIGN KEY `fk` (`a`) REFERENCES `t` (`a`) ON UPDATE CASCADE ON DELETE CASCADE"
    );
    check_parse_mysql!("ALTER TABLE t ADD INDEX key_name (t1.c1, t2.c2)");
    check_parse_mysql!("ALTER TABLE t ADD INDEX key_name (t1.c1, t2.c2)");
    check_parse_mysql!("ALTER TABLE t ADD KEY key_name (t1.c1, t2.c2) USING BTREE");
    check_parse_mysql!(
        "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY key_name (c1, c2) REFERENCES t1 (c3, c4)"
    );
}

#[test]
fn alter_postgres() {
    check_parse_postgres!(r#"ALTER TABLE "t" ADD COLUMN "c" INT(32)"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD COLUMN "c" INT"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD COLUMN "c" INT, ADD COLUMN "d" TEXT"#);
    check_parse_postgres!(r#"ALTER TABLE "t" DROP COLUMN c"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ALTER COLUMN c SET DEFAULT 'foo'"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ALTER COLUMN c DROP DEFAULT"#);
    check_parse_postgres!(r#"ALTER TABLE ONLY "t" DROP COLUMN c"#);
    check_parse_postgres!(r#"ALTER TABLE "t" DROP CONSTRAINT c CASCADE"#);
    check_parse_postgres!(r#"ALTER TABLE "t" DROP CONSTRAINT c RESTRICT"#);
    check_parse_postgres!(r#"ALTER TABLE "t" DROP CONSTRAINT c"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD PRIMARY KEY ("a") DEFERRABLE"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD PRIMARY KEY ("a") DEFERRABLE INITIALLY DEFERRED"#);
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD PRIMARY KEY ("a") DEFERRABLE INITIALLY IMMEDIATE"#
    );
    check_parse_postgres!(r#"ALTER TABLE "t" ADD PRIMARY KEY ("a") NOT DEFERRABLE"#);
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD PRIMARY KEY ("a") NOT DEFERRABLE INITIALLY IMMEDIATE"#
    );
    check_parse_postgres!(r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a")"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE NULLS DISTINCT ("a")"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE NULLS NOT DISTINCT ("a")"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a")"#);
    check_parse_postgres!(r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a") DEFERRABLE"#);
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a") DEFERRABLE INITIALLY DEFERRED"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a") DEFERRABLE INITIALLY IMMEDIATE"#
    );
    check_parse_postgres!(r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a") NOT DEFERRABLE"#);
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" UNIQUE ("a") NOT DEFERRABLE INITIALLY IMMEDIATE"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON UPDATE CASCADE"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON UPDATE SET NULL"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON UPDATE RESTRICT"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON UPDATE NO ACTION"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON UPDATE SET DEFAULT"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON DELETE CASCADE"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON DELETE SET NULL"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON DELETE RESTRICT"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON DELETE NO ACTION"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON DELETE SET DEFAULT"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON DELETE CASCADE ON UPDATE CASCADE"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE "t" ADD CONSTRAINT "c" FOREIGN KEY "fk" ("a") REFERENCES "t" ("a") ON UPDATE CASCADE ON DELETE CASCADE"#
    );
    check_parse_postgres!("ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY key_name (t1.c1, t2.c2)");
    check_parse_postgres!("ALTER TABLE t ADD CONSTRAINT c UNIQUE (t1.c1, t2.c2)");
    check_parse_postgres!("ALTER TABLE t ADD CONSTRAINT c UNIQUE (t1.c1, t2.c2) USING HASH");
    check_parse_postgres!("ALTER TABLE t ADD FOREIGN KEY (c1, c2) REFERENCES t1 (c3, c4)");
    check_parse_postgres!("ALTER TABLE t RENAME x TO y");
}

#[test]
fn alter_readyset() {
    check_parse_both!("ALTER READYSET RESNAPSHOT TABLE t;");
    check_parse_both!("ALTER READYSET ADD TABLES t;");
    check_parse_both!("ALTER READYSET ADD TABLES t1, t2;");
    check_parse_both!("ALTER READYSET ENTER MAINTENANCE MODE;");
    check_parse_both!("ALTER READYSET EXIT MAINTENANCE MODE;");
}

#[test]
fn column_mysql() {
    for col in [
        "foo INT AS (1 + 1) STORED",
        "foo INT GENERATED ALWAYS AS (1 + 1) STORED",
        "foo INT GENERATED ALWAYS AS (1 + 1) VIRTUAL",
        "foo INT GENERATED ALWAYS AS (1 + 1)",
        "`col1` INT GENERATED ALWAYS AS (1 + 1) VIRTUAL NOT NULL",
        "`col1` INT GENERATED ALWAYS AS (1 + 1) VIRTUAL NOT NULL PRIMARY KEY",
        "`created_at` timestamp NOT NULL DEFAULT current_timestamp()",
        "`c` INT(32) NULL",
        "`c` bool DEFAULT FALSE",
        "`c` bool DEFAULT true",
        "`lastModified` DATETIME(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE CURRENT_TIMESTAMP",
        "`lastModified` DATETIME(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE CURRENT_TIMESTAMP(6)",
        "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP (6) ",
        "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP( 6 )",
        "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6 ) ",
        "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP ( 6)",
        "c varchar(255) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'",
        "c timestamp NOT NULL DEFAULT NOW()",
    ] {
        check_parse_mysql!(format!("CREATE TABLE t1 ({})", col));
        check_parse_mysql!(format!("ALTER TABLE t1 ADD {}", col));
    }
}

#[test]
fn column_postgres() {
    check_parse_postgres!(
        r#"CREATE TABLE t1 ("created_at" timestamp NOT NULL DEFAULT current_timestamp())"#
    );
    check_parse_postgres!(
        r#"ALTER TABLE t1 ADD "created_at" timestamp NOT NULL DEFAULT current_timestamp()"#
    );
}

#[test]
fn comment_postgres() {
    check_parse_postgres!("COMMENT ON TABLE test IS 'this is a comment'");
    check_parse_postgres!("COMMENT ON COLUMN test_table.test_column IS 'this is a comment'");
    check_parse_postgres!(r#"COMMENT ON COLUMN "test_table"."test_column" IS 'this is a comment'"#);
    check_parse_postgres!(r#"COMMENT ON TABLE "test_table" IS 'this is a comment'"#);
}

#[test]
fn expr_both() {
    for expr in [
        "coalesce(a,b,c)",
        "coalesce (a,b,c)",
        "coalesce(a ,b,c)",
        "coalesce(a, b,c)",
        "max(min(foo))",
        "max(cast(foo as int))",
        "ifnull(x, 0)",
        "substr(a from 1 for 7)",
        "substring(a from 1 for 7)",
        "substr(a from 1)",
        "substr(a for 7)",
        "substring(a,1,7)",
        "count(*)",
        "count (*)",
        "count ( * )",
        "cast(lp.start_ddtm as date)",
        r#"coalesce("a",b,c)"#,
        r#"coalesce ("a",b,c)"#,
        r#"coalesce("a" ,b,c)"#,
        r#"coalesce("a", b,c)"#,
        "coalesce('a',b,c)",
        "coalesce ('a',b,c)",
        "coalesce('a' ,b,c)",
        "coalesce('a', b,c)",
        "lower(AbC)",
        "upper(AbC)",
        "lower('AbC')",
        "upper('AbC')",
        "x",
        "(x + 1) IN (2)",
        "1 + 2 * 3",
        "(1 + (2 * 3))",
        "x between y and z or w",
        "(x between y and z) or w",
        "(table_1.column_2 NOT BETWEEN 1 AND 5 OR table_1.column_2 NOT BETWEEN 1 AND 5)",
        "(table_1.column_2 NOT BETWEEN 1 AND 5) OR (table_1.column_2 NOT BETWEEN 1 AND 5)",
        "x + 1 in (2)",
        "((x + 1) in (2))",
        "CAST(-128 AS TEXT)",
        "DATE('2024-01-01 23:59:59')",
        "x + 3",
        "( x - 2 )",
        "( x * 5 )",
        "x * 3 = 21",
        "(x - 7 = 15)",
        "( x + 2) = 15",
        "( x + 2) =(x*3)",
        "foo >= 42",
        "foo <= 5",
        "foo = ''",
        "not bar = 12 or foobar = 'a'",
        "bar in (select col from foo)",
        "exists (  select col from foo  )",
        "not exists (select col from foo)",
        "paperId in (select paperId from PaperConflict) and size > 0",
        "name ILIKE $1",
        "(foo = $1 or bar = 12) and foobar = 'a'",
        "foo = $1 and bar = 12 or foobar = 'a'",
        "bar in (0, 1)",
        "bar IS NULL",
        "bar IS NOT NULL",
        "foo between 1 and 2",
        "foo not between 1 and 2",
        "f(foo, bar) between 1 and 2",
        "foo between (1 + 2) and 3 + 5",
        "x and not y",
        "-256",
        "x + -y",
        "NOT -id",
        "NOT -1",
        "nullable",
        "id not in (1,2)",
    ] {
        check_parse_both!(format!("SELECT {expr}"));
        check_parse_both!(format!("SELECT {expr} expr"));
        check_parse_both!(format!("SELECT {expr} AS expr"));
        check_parse_both!(format!("SELECT * FROM t WHERE {expr}"));
    }
}

#[test]
fn expr_mysql() {
    for expr in [
        "group_concat(x separator ', ')",
        "group_concat('a')",
        "group_concat (a)",
        "group_concat ( a )",
        "cast(`lp`.`start_ddtm` as date)",
        "CAST(-128 AS UNSIGNED)",
        "`id` NOT IN (1, 2)",
        "CASE WHEN (`foo` = 0) THEN `foo` ELSE 1 END",
        "CASE WHEN (`foo` = 0) THEN `foo` END",
        "CASE WHEN (`foo` = 0) THEN `foo` WHEN (`foo` = 7) THEN `foo` END",
        "group_concat(`x` separator 'a')",
        "group_concat(`x` separator '''')",
        "group_concat(`x`)",
        "name ILIKE ?",
        "(foo = ? or bar = 12) and foobar = 'a'",
        "foo = ? and bar = 12 or foobar = 'a'",
        "--1",
        "`h`.`x` is null and month(`a`.`b`) between `t2`.`start` and `t2`.`end` and dayofweek(`c`.`d`) between 1 and 3",
        "((`h`.`x` is null) and (month(`a`.`b`) between `t2`.`start` and `t2`.`end`) and (dayofweek(`c`.`d`) between 1 and 3))",
        "`read_ribbons`.`is_following` = 1 \
            AND `comments`.`user_id` <> `read_ribbons`.`user_id` \
            AND `saldo` >= 0 \
            AND ( `parent_comments`.`user_id` = `read_ribbons`.`user_id` \
            OR ( `parent_comments`.`user_id` IS NULL \
            AND `stories`.`user_id` = `read_ribbons`.`user_id` ) ) \
            AND ( `parent_comments`.`id` IS NULL \
            OR `saldo` >= 0 ) \
            AND `read_ribbons`.`user_id` = ?",
        "foo = 42",
    ] {
        check_parse_mysql!(format!("SELECT {expr}"));
        check_parse_mysql!(format!("SELECT {expr} expr"));
        check_parse_mysql!(format!("SELECT {expr} AS expr"));
        check_parse_mysql!(format!("SELECT * FROM t WHERE {expr}"));
    }
}

#[test]
fn expr_postgres() {
    for expr in [
        r#"cast("lp"."start_ddtm" as date)"#,
        r#"lower('AbC' COLLATE "es_ES")"#,
        r#"upper('AbC' COLLATE "es_ES")"#,
        "'a'::abc < all('{b,c}')",
        "515*128::TEXT",
        "(515*128)::TEXT",
        r#"foo = "hello""#,
        "id not in (1,2)",
        r#""id" NOT IN (1, 2)"#,
        r#"CASE WHEN ("foo" = 0) THEN "foo" ELSE 1 END"#,
        r#"CASE WHEN ("foo" = 0) THEN "foo" END"#,
        r#"CASE WHEN ("foo" = 0) THEN "foo" WHEN ("foo" = 7) THEN "foo" END"#,
        "x = ANY('foo')",
        "x = SOME ('foo')",
        "x = aLL ('foo')",
        "x <= ANY('foo')",
        "1 = ANY('{1,2}') = true",
        "1 = ANY('{1,2}') and y = 4",
        r#""h"."x" is null and month("a"."b") between "t2"."start" and "t2"."end" and dayofweek("c"."d") between 1 and 3"#,
        r#"(("h"."x" is null) and (month("a"."b") between "t2"."start" and "t2"."end") and (dayofweek("c"."d") between 1 and 3))"#,
        "'[1, 2, 3]'::json -> 0 = '[3, 2, 1]'::json -> 2",
        "('[1, 2, 3]'::json -> 0) = ('[3, 2, 1]'::json -> 2)",
        "'[[1, 2, 3]]'::json -> 1 - 1 -> 1",
        "'[[1, 2, 3]]'::json -> (1 - 1) -> 1",
        "'[[1, 2, 3]]'::json -> 1 * 0 -> 1",
        "'[[1, 2, 3]]'::json -> (1 * 0) -> 1",
        "'[1, 2, 3]'::json ->> 0 like '[3, 2, 1]'::json ->> 2",
        "('[1, 2, 3]'::json ->> 0) like ('[3, 2, 1]'::json ->> 2)",
        "lhs IN (ROW(rhs1, rhs2))",
        r#"'{"abc": 42}' ? 'abc'"#,
        r#"'{"abc": 42}' ?| ARRAY['abc', 'def']"#,
        r#"'{"abc": 42}' ?& ARRAY['abc', 'def']"#,
        r#"'["a", "b"]'::jsonb || '["c", "d"]'"#,
        "'[1, 2, 2]' @> '2'",
        "'2' <@ '[1, 2, 2]'",
        r#""read_ribbons"."is_following" = 1
            AND "comments"."user_id" <> "read_ribbons"."user_id"
            AND "saldo" >= 0
            AND ( "parent_comments"."user_id" = "read_ribbons"."user_id"
            OR ( "parent_comments"."user_id" IS NULL
            AND "stories"."user_id" = "read_ribbons"."user_id" ) )
            AND ( "parent_comments"."id" IS NULL
            OR "saldo" >= 0 )
            AND "read_ribbons"."user_id" = $1"#,
        "foo = 42",
        "foo = 'hello'",
        "'[1, 2, 3]' -> 2",
        "'[1, 2, 3]' ->> 2",
        "'[1, 2, 3]' #> array['2']",
        "'[1, 2, 3]' #>> array['2']",
        "'[1, 2, 3]' #- array['2']",
        "ARRAY[[1, '2'::int], array[3]]",
        "ARRAY[[1,('2'::INT)]]",
    ] {
        check_parse_postgres!(format!("SELECT {expr}"));
        check_parse_postgres!(format!("SELECT {expr} as expr"));
        check_parse_postgres!(format!("SELECT {expr} expr"));
        check_parse_postgres!(format!("SELECT * FROM t WHERE {expr}"));
    }

    // See the FIXME(sqlparser) comments in `AutoParameterizeVisitor::visit_expr`: nom-sql strips
    // the extra parens on the right side of the `IN`
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT lhs IN ((rhs1, rhs2))",
        "nom-sql AST differs from sqlparser-rs AST"
    );
}

#[test]
fn expr_postgres_with_alias() {
    // For some reason, nom-sql can't handle these with an explicit alias
    for expr in ["-128::INTEGER", "200*postgres.column::DOUBLE PRECISION"] {
        check_parse_postgres!(format!("SELECT {expr}"));
        check_parse_fails!(
            Dialect::PostgreSQL,
            format!("SELECT {expr} AS expr"),
            "nom-sql error"
        );
        check_parse_fails!(
            Dialect::PostgreSQL,
            format!("SELECT {expr} expr"),
            "nom-sql error"
        );
    }
}
#[test]
fn extract() {
    for component in [
        "CENTURY",
        "DECADE",
        "DOW",
        "DOY",
        "EPOCH",
        "HOUR",
        "ISODOW",
        "ISOYEAR",
        "JULIAN",
        "MICROSECONDS",
        "MILLENNIUM",
        "MILLISECONDS",
        "MINUTE",
        "MONTH",
        "QUARTER",
        "SECOND",
        "TIMEZONE_HOUR",
        "TIMEZONE_MINUTE",
        "TIMEZONE",
        "WEEK",
        "YEAR",
    ] {
        check_parse_both!(format!(r#"SELECT extract({component} FROM col) FROM t"#));
        check_parse_mysql!(format!("SELECT extract({component} FROM `col`) FROM t"));
        check_parse_postgres!(format!(r#"SELECT extract({component} FROM "col") FROM t"#));
    }
}

#[test]
fn compound_selects() {
    check_parse_both!("SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating;");
    check_parse_both!("(SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating);");
    check_parse_both!(
        "SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating UNION DISTINCT SELECT 42, 5 FROM Vote;"
    );
    check_parse_both!("SELECT id, 1 FROM Vote UNION ALL SELECT id, stars from Rating;");
    check_parse_both!(
        "SELECT id FROM foo WHERE col = 'abc' UNION SELECT id FROM foo WHERE othercol >= 3 ORDER BY id ASC LIMIT 1;"
    );
    check_parse_both!(
        "SELECT id FROM foo WHERE col = 'abc' UNION (SELECT id FROM foo WHERE othercol >= 3) ORDER BY id ASC LIMIT 1;"
    );
    check_parse_mysql!(
        "(select `discussions`.* from `discussions` where (`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) or `tags`.`parent_id` is null))))) and (`discussions`.`is_private` = ? or (((`discussions`.`is_approved` = ? and (`discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))))))) and (`discussions`.`hidden_at` is null or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and (`discussions`.`comment_count` > ? or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and not exists (select 1 from `discussion_user` where `discussions`.`id` = `discussion_id` and `user_id` = ? and `subscription` = ?) and `discussions`.`id` not in (select `discussion_id` from `discussion_tag` where 0 = 1) order by `last_posted_at` desc limit 21) union (select `discussions`.* from `discussions` where (`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) or `tags`.`parent_id` is null))))) and (`discussions`.`is_private` = ? or (((`discussions`.`is_approved` = ? and (`discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))))))) and (`discussions`.`hidden_at` is null or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and (`discussions`.`comment_count` > ? or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and `is_sticky` = ? limit 21) order by is_sticky and not exists (select 1 from `discussion_user` as `sticky` where `sticky`.`discussion_id` = `id` and `sticky`.`user_id` = ? and `sticky`.`last_read_post_number` >= `last_post_number`) and last_posted_at > ? desc, `last_posted_at` desc limit 21"
    );
}

#[test]
fn create_both() {
    check_parse_both!(
        "CREATE TABLE if Not  ExistS users (id bigint(20), name varchar(255), email varchar(255));"
    );
    check_parse_both!("CREATE TABLE t(x integer);");
    check_parse_both!("CREATE TABLE db1.t(x integer);");
    check_parse_both!(
        "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), PRIMARY KEY (id));"
    );
    check_parse_both!("CREATE VIEW v AS SELECT * FROM users UNION SELECT * FROM old_users;");
    check_parse_both!("CREATE TABLE IF NOT EXISTS t (x int)");
    check_parse_both!("create table t(x double precision)");
    check_parse_both!(r#"CREATE VIEW v AS SELECT * FROM users WHERE username = "bob";"#);
    check_parse_both!("CREATE CACHE foo FROM SELECT id FROM users WHERE name = $1");
    check_parse_both!("CREATE CACHE FROM SELECT id FROM users WHERE name = $1");
    check_parse_both!("CREATE CACHE foo FROM q_0123456789ABCDEF");
    check_parse_both!("CREATE CACHE FROM q_0123456789ABCDEF");
    check_parse_both!("CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT id FROM users WHERE name = $1");
    check_parse_both!("CREATE CACHE ALWAYS FROM SELECT id FROM users WHERE name = $1");
    check_parse_both!("CREATE CACHE CONCURRENTLY FROM SELECT id FROM users WHERE name = $1");
    check_parse_both!("CREATE CACHE ALWAYS CONCURRENTLY FROM SELECT id FROM users WHERE name = $1");
    check_parse_both!(
        "CREATE CACHE CONCURRENTLY ALWAYS foo FROM SELECT id FROM users WHERE name = $1"
    );
    check_parse_both!(
        "CREATE CACHE CONCURRENTLY ALWAYS foo FROM SELECT id FROM users WHERE (name = $1)"
    );
    check_parse_both!(
        "CREATE TABLE employees (
            emp_no      INT             NOT NULL,
            birth_date  DATE            NOT NULL,
            first_name  VARCHAR(14)     NOT NULL,
            last_name   VARCHAR(16)     NOT NULL,
            gender      ENUM ('M','F')  NOT NULL,
            hire_date   DATE            NOT NULL,
            PRIMARY KEY (emp_no)
        )"
    );
    check_parse_both!("CREATE TABLE IF NOT EXISTS t (x int)");
    check_parse_both!("create table t(x double precision)");
    check_parse_both!("CREATE VIEW v AS SELECT * FROM users WHERE username = 'bob';");
    check_parse_both!(
        "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip
                varchar(40) NOT NULL default '') ENGINE=MyISAM;"
    );
}

#[test]
fn create_mysql() {
    check_parse_mysql!(
        "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), UNIQUE KEY id_k (id));"
    );
    check_parse_mysql!(
        "CREATE TABLE users (
            id int,
            group_id int,
            primary key (id),
            constraint users_group foreign key (group_id) references `groups` (id)
        ) AUTO_INCREMENT=1000"
    );
    check_parse_mysql!(
        "CREATE TABLE addresses (
            id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            street VARCHAR(255) NOT NULL,
            city VARCHAR(255) NOT NULL,
            state VARCHAR(255) NOT NULL,
            zip VARCHAR(255) NOT NULL,
            type enum('SHIPPING','BILLING','LIVING') NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        ) AUTO_INCREMENT = 10"
    );
    check_parse_mysql!(
        "CREATE TABLE orders (
            order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
            purchaser INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
            FOREIGN KEY ordered_product (product_id) REFERENCES products(id)
        )"
    );
    check_parse_mysql!("CREATE TABLE IF NOT EXISTS `t` (`x` INT)");
    check_parse_mysql!(
        "CREATE ALGORITHM=UNDEFINED DEFINER=`mysqluser`@`%` SQL SECURITY DEFINER VIEW `myquery2` AS SELECT * FROM employees"
    );
    check_parse_mysql!(
        "CREATE TABLE `django_admin_log` (
            `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
            `action_time` datetime NOT NULL,
            `user_id` integer NOT NULL,
            `content_type_id` integer,
            `object_id` longtext,
            `object_repr` varchar(200) NOT NULL,
            `action_flag` smallint UNSIGNED NOT NULL,
            `change_message` longtext NOT NULL);"
    );
    check_parse_mysql!(
        "CREATE TABLE `auth_group` (
            `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
            `name` varchar(80) NOT NULL UNIQUE)"
    );
    check_parse_mysql!("CREATE TABLE `auth_group` (
            `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
            `name` varchar(80) NOT NULL UNIQUE) ENGINE=InnoDB AUTO_INCREMENT=495209 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");
    check_parse_mysql!("CREATE VIEW `v` AS SELECT * FROM `t`;");
    check_parse_mysql!("CREATE VIEW `v` AS SELECT * FROM `t`");
    check_parse_mysql!("CREATE CACHE foo FROM SELECT id FROM users WHERE name = ?");
    check_parse_mysql!("CREATE CACHE FROM SELECT id FROM users WHERE name = ?");
    check_parse_mysql!("CREATE CACHE foo FROM q_0123456789ABCDEF");
    check_parse_mysql!("CREATE CACHE FROM q_0123456789ABCDEF");
    check_parse_mysql!("CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT id FROM users WHERE name = ?");
    check_parse_mysql!("CREATE CACHE ALWAYS FROM SELECT id FROM users WHERE name = ?");
    check_parse_mysql!("CREATE CACHE CONCURRENTLY FROM SELECT id FROM users WHERE name = ?");
    check_parse_mysql!("CREATE CACHE ALWAYS CONCURRENTLY FROM SELECT id FROM users WHERE name = ?");
    check_parse_mysql!(
        "CREATE CACHE CONCURRENTLY ALWAYS foo FROM SELECT id FROM users WHERE name = ?"
    );
    check_parse_mysql!(
        "CREATE CACHE CONCURRENTLY ALWAYS `foo` FROM SELECT `id` FROM `users` WHERE (`name` = ?)"
    );
    check_parse_mysql!(
        "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip
            varchar(40) NOT NULL default '') ENGINE=MyISAM;"
    );
    check_parse_mysql!(
        "CREATE TABLE `interwiki` (
            iw_prefix varchar(32) NOT NULL,
            iw_url blob NOT NULL,
            iw_api blob NOT NULL,
            iw_wikiid varchar(64) NOT NULL,
            iw_local bool NOT NULL,
            iw_trans tinyint NOT NULL default 0
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8"
    );
    check_parse_mysql!(
        "CREATE TABLE `externallinks` (
            `el_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
            `el_from` int(8) unsigned NOT NULL DEFAULT '0',
            `el_from_namespace` int(11) NOT NULL DEFAULT '0',
            `el_to` blob NOT NULL,
            `el_index` blob NOT NULL,
            `el_index_60` varbinary(60) NOT NULL,
            PRIMARY KEY (`el_id`),
            KEY `el_from` (`el_from`,`el_to`(40)),
            KEY `el_to` (`el_to`(60),`el_from`),
            KEY `el_index` (`el_index`(60)),
            KEY `el_backlinks_to` (`el_from_namespace`,`el_to`(60),`el_from`),
            KEY `el_index_60` (`el_index_60`,`el_id`),
            KEY `el_from_index_60` (`el_from`,`el_index_60`,`el_id`)
        )"
    );
    check_parse_mysql!(
        "CREATE TABLE dept_manager (
            dept_no      CHAR(4)         NOT NULL,
            emp_no       INT             NOT NULL,
            from_date    DATE            NOT NULL,
            to_date      DATE            NOT NULL,
            KEY         (emp_no),
            KEY         (dept_no),
            FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ,
            FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ,
            PRIMARY KEY (emp_no,dept_no)
        )"
    );
    check_parse_mysql!(
        "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER
            VIEW `dept_emp_latest_date` AS
            SELECT emp_no, MAX(from_date) AS from_date, MAX(to_date) AS to_date
            FROM dept_emp
            GROUP BY emp_no"
    );
    check_parse_mysql!("CREATE TABLE `groups` ( id integer );");
    check_parse_mysql!("CREATE TABLE `select` ( id integer );");
    check_parse_mysql!("CREATE TABLE `access_tokens` (
            `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
            `token` varchar(40) COLLATE utf8mb4_unicode_ci NOT NULL,
            `user_id` int(10) unsigned NOT NULL,
            `last_activity_at` datetime NOT NULL,
            `created_at` datetime NOT NULL,
            `type` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
            `title` varchar(150) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            `last_ip_address` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            `last_user_agent` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `access_tokens_token_unique` (`token`),
            KEY `access_tokens_user_id_foreign` (`user_id`),
            KEY `access_tokens_type_index` (`type`),
            CONSTRAINT `access_tokens_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");
    check_parse_mysql!(
        "create table `mentions_posts` (`post_id` int unsigned not null, `mentions_id` int unsigned not null) default character set utf8mb4 collate 'utf8mb4_unicode_ci'"
    );
    check_parse_mysql!("CREATE TABLE `action_mailbox_inbound_emails` (
            `id` bigint NOT NULL AUTO_INCREMENT, `status` int NOT NULL DEFAULT '0',
            `message_id` varchar(255) NOT NULL,
            `message_checksum` varchar(255) NOT NULL,
            `created_at` datetime(6) NOT NULL,
            `updated_at` datetime(6) NOT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `index_action_mailbox_inbound_emails_uniqueness` (`message_id`,`message_checksum`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3");
    check_parse_mysql!(
        "CREATE TABLE `ar_internal_metadata` (
            `key` character varying NOT NULL,
            `value` character varying,
            `created_at` timestamp(6) without time zone NOT NULL,
            `updated_at` timestamp(6) without time zone NOT NULL,
            PRIMARY KEY (`key`));"
    );
    check_parse_mysql!(
        "CREATE TABLE `uploads` (
            `id` integer NOT NULL,
            `user_id` integer NOT NULL,
            `original_filename` character varying NOT NULL,
            `filesize` bigint NOT NULL,
            `width` integer,
            `height` integer,
            `url` character varying NOT NULL,
            `created_at` timestamp without time zone NOT NULL,
            `updated_at` timestamp without time zone NOT NULL,
            `sha1` character varying(40),
            `origin` character varying(1000),
            `retain_hours` integer,
            `extension` character varying(10),
            `thumbnail_width` integer,
            `thumbnail_height` integer,
            `etag` character varying,
            `secure` boolean NOT NULL,
            `access_control_post_id` bigint,
            `original_sha1` character varying,
            `animated` boolean,
            `verification_status` integer NOT NULL,
            `security_last_changed_at` timestamp without time zone,
            `security_last_changed_reason` character varying,
            PRIMARY KEY (`id`));"
    );
    check_parse_mysql!(
        "CREATE TABLE `spree_zones` (
            `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `name` varchar(255), `description` varchar(255),
            `default_tax` tinyint(1) DEFAULT FALSE,
            `zone_members_count` int DEFAULT 0,
            `created_at` datetime(6), `updated_at` datetime(6)) ENGINE=InnoDB;"
    );
    check_parse_mysql!(
        "CREATE TABLE `table` (
            `enabled` bit(1) NOT NULL DEFAULT b'0'
        )"
    );
    check_parse_mysql!("CREATE TABLE foo (
            `lastModified` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE CURRENT_TIMESTAMP(6)
        );");
    check_parse_mysql!(
        "CREATE TABLE `t` (
            `id` int,
            `name` varchar(20),
            `name2` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    );
    check_parse_mysql!(
        "CREATE TABLE `comments` (
            `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `hat_id` int,
            fulltext INDEX `index_comments_on_comment`  (`comment`),
            INDEX `confidence_idx`  (`confidence`),
            UNIQUE INDEX `short_id`  (`short_id`),
            INDEX `story_id_short_id`  (`story_id`, `short_id`),
            INDEX `thread_id`  (`thread_id`),
            INDEX `index_comments_on_user_id`  (`user_id`))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    );
}

#[test]
fn create_postgres() {
    check_parse_postgres!(r#"CREATE TABLE IF NOT EXISTS "t" ("x" INT)"#);
    check_parse_postgres!(
        r#"CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "user_id" integer NOT NULL,
            "content_type_id" integer,
            "object_id" longtext,
            "object_repr" varchar(200) NOT NULL,
            "action_flag" smallint UNSIGNED NOT NULL,
            "change_message" longtext NOT NULL
        );"#
    );
    check_parse_postgres!(
        r#"CREATE TABLE "auth_group" (
            "id" integer NOT NULL PRIMARY KEY,
            "name" varchar(80) NOT NULL UNIQUE
        )"#
    );
    check_parse_postgres!(
        r#"CREATE TABLE "auth_group" (
            "id" integer NOT NULL PRIMARY KEY,
            "name" varchar(80) NOT NULL UNIQUE
        )"#
    );
    check_parse_postgres!(
        r#"CREATE TABLE "auth_group" (
            "id" INT NOT NULL PRIMARY KEY,
            "name" VARCHAR(80) NOT NULL UNIQUE
        )"#
    );
    check_parse_postgres!(r#"CREATE TABLE groups ( id integer );"#);
    check_parse_postgres!(r#"CREATE TABLE "select" ( id integer );"#);
    check_parse_postgres!(r#"CREATE VIEW "v" AS SELECT * FROM "t";"#);
    check_parse_postgres!(r#"CREATE VIEW "v" AS SELECT * FROM "t""#);
    check_parse_postgres!(
        r#"CREATE CACHE "foo" FROM SELECT "id" FROM "users" WHERE ("name" = $1)"#
    );
    check_parse_postgres!(
        r#"CREATE TABLE "interwiki" (
            iw_prefix varchar(32) NOT NULL,
            iw_url blob NOT NULL,
            iw_api blob NOT NULL,
            iw_wikiid varchar(64) NOT NULL,
            iw_local bool NOT NULL,
            iw_trans tinyint NOT NULL default 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"#
    );
    check_parse_postgres!(
        r#"CREATE TABLE "externallinks" (
            "el_id" int NOT NULL,
            "el_from" int NOT NULL DEFAULT '0',
            "el_from_namespace" int NOT NULL DEFAULT '0',
            "el_to" text NOT NULL,
            "el_index" text NOT NULL,
            "el_index_60" text NOT NULL,
            PRIMARY KEY ("el_id"),
            CONSTRAINT "el_index_60" UNIQUE ("el_index_60","el_id"),
            CONSTRAINT "el_from_index_60" UNIQUE ("el_from","el_index_60","el_id")
        )"#
    );
}

#[test]
#[ignore = "various issues, see comments"]
fn create_mysql_unsupported() {
    // REA-5841, REA-5842
    check_parse_mysql!(
        "CREATE TABLE customers (
                id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                last_name VARCHAR(255) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL UNIQUE KEY )
                AUTO_INCREMENT=1001"
    );
    check_parse_mysql!(
        "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255),
            UNIQUE KEY id_k (id) USING HASH);"
    );
    check_parse_mysql!(
        "CREATE TABLE users (
            age INTEGER,
            KEY age_key (age) USING BTREE
        )"
    );
    // REA-5843
    check_parse_mysql!(
        "CREATE TABLE `auth_group` (
            `id` INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            `name` VARCHAR(80) NOT NULL UNIQUE COLLATE utf8mb4_unicode_ci)
            ENGINE=InnoDB, AUTO_INCREMENT=495209, DEFAULT CHARSET=utf8mb4, COLLATE=utf8mb4_unicode_ci"
    );
    // REA-5844
    check_parse_mysql!(
        "CREATE TABLE `user` (
            user_id int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
            user_name varchar(255) binary NOT NULL default '',
            user_real_name character varying(255) binary NOT NULL default '',
            user_password tinyblob NOT NULL,
            user_newpassword tinyblob NOT NULL,
            user_newpass_time binary(14),
            user_email tinytext NOT NULL,
            user_touched binary(14) NOT NULL default '',
            user_token binary(32) NOT NULL default '',
            user_email_authenticated binary(14),
            user_email_token binary(32),
            user_email_token_expires binary(14),
            user_registration binary(14),
            user_editcount int,
            user_password_expires varbinary(14) DEFAULT NULL
        ) ENGINE=InnoDB, DEFAULT CHARSET=utf8"
    );

    // I don't see any reason to support parsing `CREATE DATABASE/SCHEMA`
    for sql in [
        "Create databasE if  not Exists  noria       ",
        "CREATE DATABASE IF NOT EXISTS noria",
        "Create databasE   noria",
        "CREATE DATABASE noria",
        "Create schema   noria       ",
        "CREATE SCHEMA noria",
        "Create scheMA if  not Exists  noria",
        "CREATE SCHEMA IF NOT EXISTS noria",
        "create databasE If  NOT exists  noria    default  character   Set =   utf16",
        "CREATE DATABASE IF NOT EXISTS noria DEFAULT CHARACTER SET = utf16",
        "create schema noria      character   Set =   utf16   Collate utf16_collation   Encryption=  Y     ",
        "CREATE SCHEMA noria CHARACTER SET = utf16 COLLATE = utf16_collation ENCRYPTION = Y",
        "create databasE If  not exists  noria   DEfault  Collate utf16_collation  Character   Set =   utf16      ",
        "CREATE DATABASE IF NOT EXISTS noria DEFAULT COLLATE = utf16_collation CHARACTER SET = utf16",
        "Create schema noria   default Encryption=Y       Collate utf16_collation   ",
        "CREATE SCHEMA noria DEFAULT ENCRYPTION = Y COLLATE = utf16_collation",
        "Create databasE if  not exists  noria     default Encryption=N   defaULT Collate utf16_collation   default   character   Set =   utf16    ",
        "CREATE DATABASE IF NOT EXISTS noria DEFAULT ENCRYPTION = N DEFAULT COLLATE = utf16_collation DEFAULT CHARACTER SET = utf16",
        "Create databasE noria   Collate utf16_collation   Encryption=N character set =   utf16",
        "CREATE DATABASE noria COLLATE = utf16_collation ENCRYPTION = N CHARACTER SET = utf16",
    ] {
        check_parse_mysql!(sql);
    }
}

#[test]
fn constraint_check() {
    for constraint in [
        "CHECK (x > 1)",
        "CHECK (x > 1) NOT ENFORCED",
        // REA-5846
        // "CONSTRAINT CHECK (x > 1)",
        "CONSTRAINT foo CHECK (x > 1)",
        "CONSTRAINT foo CHECK (x > 1) NOT ENFORCED",
    ] {
        check_parse_both!(format!("CREATE TABLE t (x INT, {constraint})"));
    }
}

#[test]
fn constraint_check_on_column() {
    // nom-sql doesn't support CHECK constraints on columns
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TABLE t (x INT CHECK (x > 1))",
        "nom-sql error"
    );
    // REA-5845
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TABLE t (x INT CHECK (x > 1) NOT ENFORCED)",
        "both parsers failed"
    );
}

#[test]
fn create_table_options_mysql() {
    for options in [
        "DEFAULT CHARSET 'utf8mb4' COLLATE 'utf8mb4_unicode_520_ci'",
        "DEFAULT CHARSET 'utf8mb4' COLLATE utf8mb4_unicode_520_ci",
        "DEFAULT CHARSET utf8mb4 COLLATE 'utf8mb4_unicode_520_ci'",
        "DEFAULT  CHARSET  utf8mb4  COLLATE  utf8mb4_unicode_520_ci",
        "ENGINE=InnoDB AUTO_INCREMENT=44782967 \
            DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
        "DEFAULT CHARSET  utf8mb4",
        "DEFAULT CHARACTER   SET  utf8mb4",
        "COMMENT 'foobar'",
        "COMMENT='foobar'",
        "COMMENT='foo''bar'",
        r#"COMMENT='foo""bar'"#,
        "DATA DIRECTORY = '/var/lib/mysql/'",
        "DATA DIRECTORY '/var/lib/mysql/'",
        "DEFAULT CHARSET=utf8mb4",
        "DEFAULT CHARSET utf8mb4",
        " CHARSET=utf8mb4",
        "CHARSET utf8mb4",
        "CHARSET=utf8mb4 DEFAULT COLLATE=utf8mb4_unicode_520_ci",
        "CHARSET=utf8mb4 COLLATE utf8mb4_unicode_520_ci",
    ] {
        check_parse_mysql!(format!("CREATE TABLE foo (x TEXT) {options}"));
    }
}

#[test]
fn create_table_options_unsupported() {
    // REA-5843
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TABLE foo (x TEXT) AUTO_INCREMENT=1,ENGINE=,KEY_BLOCK_SIZE=8",
        "sqlparser error"
    );

    // REA-5847
    check_parse_fails!(
        Dialect::MySQL,
        r#"CREATE TABLE foo (x TEXT) COMMENT="foo""bar""#,
        "Expected: Token::SingleQuotedString"
    );
    check_parse_fails!(
        Dialect::MySQL,
        r#"CREATE TABLE foo (x TEXT) COMMENT="foo''bar""#,
        "Expected: Token::SingleQuotedString"
    );
}

#[test]
fn deallocate_mysql() {
    check_parse_mysql!("DEALLOCATE PREPARE a42");
    check_parse_mysql!("DEALLOCATE PREPARE a42");
    // REA-5807
    check_parse_fails!(
        Dialect::MySQL,
        "DROP PREPARE a42",
        "after DROP, found: PREPARE"
    );
}

#[test]
fn deallocate_postgres() {
    check_parse_postgres!("DEALLOCATE a42");
    check_parse_postgres!("DEALLOCATE ALL");
    check_parse_postgres!("DEALLOCATE PREPARE ALL");
}

#[test]
fn delete() {
    check_parse_both!("DELETE FROM users;");
    check_parse_both!("DELETE FROM db1.users;");
    check_parse_both!("DELETE FROM users WHERE id = 1;");
    check_parse_mysql!("DELETE FROM `users` WHERE (`id` = 1)");
    check_parse_postgres!(r#"DELETE FROM "users" WHERE ("id" = 1)"#);
    check_parse_mysql!("delete from articles where `key`='aaa'");
    check_parse_mysql!("delete from `where` where user=?");
    check_parse_mysql!("DELETE FROM `articles` WHERE (`key` = 'aaa')");
    check_parse_mysql!("DELETE FROM `where` WHERE (`user` = ?)");
    check_parse_postgres!(r#"delete from articles where "key"='aaa'"#);
    check_parse_postgres!(r#"delete from "where" where user=$1"#);
    check_parse_postgres!(r#"DELETE FROM "articles" WHERE ("key" = 'aaa')"#);
    check_parse_postgres!(r#"DELETE FROM "where" WHERE ("user" = $1)"#);
}

#[test]
fn string_literal_backslash_escape_mysql() {
    use readyset_sql::ast::{Expr, Literal};

    let all_escaped = r#"\0\'\"\b\n\r\t\Z\\\%\_"#;
    let expected = "\0\'\"\x08\n\r\t\x1a\\\\%\\_";
    for quote in ['\'', '"'] {
        let expr = parse_expr(Dialect::MySQL, format!("{quote}{all_escaped}{quote}")).unwrap();
        if let Expr::Literal(Literal::String(result)) = expr {
            assert_eq!(result, expected);
        } else {
            panic!("Expected Expr::Literal(Literal::String)");
        }
    }
}

#[test]
fn string_literal_backslash_no_escape_postgres() {
    use readyset_sql::ast::{Expr, Literal};

    let all_escaped = r#"\0\"\b\n\r\t\Z\\\%\_"#;
    let expr = parse_expr(Dialect::PostgreSQL, format!("'{all_escaped}'")).unwrap();
    if let Expr::Literal(Literal::String(result)) = expr {
        assert_eq!(result, all_escaped);
    } else {
        panic!("Expected Expr::Literal(Literal::String)");
    }
}

#[test]
fn string_literal_backslash_escape_postgres() {
    use readyset_sql::ast::{Expr, Literal};

    // Note: This is missing \0 due to REA-5850 and \Z is in the original nom-sql test but it's not
    // actually supported by PostgreSQL.
    let all_escaped = r#"\'\"\b\n\r\t\\\%\_"#;
    let expected = "\'\"\x08\n\r\t\\%_";
    let expr = parse_expr(Dialect::PostgreSQL, format!("E'{all_escaped}'")).unwrap();
    if let Expr::Literal(Literal::String(result)) = expr {
        assert_eq!(result, expected);
    } else {
        panic!("Expected Expr::Literal(Literal::String)");
    }
}

#[test]
fn identifiers() {
    for identifier in [
        "foo",
        "f_o_o",
        "foo12",
        "FoO",
        "foO",
        "`foO`",
        "`primary`",
        "`state-province`",
        "````",
        "```i`",
    ] {
        check_parse_mysql!(format!("SELECT {identifier} FROM {identifier}"));
    }
    for identifier in [
        "foo",
        "f_o_o",
        "foo12",
        "FoO",
        "foO",
        r#""foO""#,
        r#""primary""#,
        r#""state-province""#,
    ] {
        check_parse_postgres!(format!("SELECT {identifier} FROM {identifier}"));
    }
}

#[test]
fn string_literals_mysql() {
    for literal in [
        "_utf8mb4'noria'",
        r#""a""b""#,
        "X'0008275c6480'",
        "x'0008275c6480'",
        "0x0008275c6480",
        "0x6D617263656C6F",
        "X''",
        "''",
    ] {
        check_parse_mysql!(format!("SELECT {literal}"));
    }

    check_parse_fails!(
        Dialect::MySQL,
        format!("SELECT 0x123"),
        "Odd number of digits"
    );
}

#[test]
fn drop() {
    check_parse_both!("DROP TABLE users;");
    check_parse_both!("DROP TABLE schema1.t1, schema2.t2");
    check_parse_both!("DROP CACHE test");
    check_parse_mysql!("DROP CACHE `test`");
    check_parse_postgres!(r#"DROP CACHE "test""#);
    check_parse_both!("drOP ALL    caCHEs");
    check_parse_both!("DroP   ViEw  v ;");
    check_parse_both!("DroP   ViEw  if EXISTS v ;");
    check_parse_both!("DroP   ViEw  v1,   v2, v3 ;");
    check_parse_both!("DroP    aLl       PrOXied      querIES");
    check_parse_mysql!("DROP TABLE IF EXISTS users,posts;");
    check_parse_mysql!("DROP TABLE IF EXISTS `users`, `posts`");
    check_parse_mysql!("DROP VIEW IF EXISTS `v1`, `v2`");
    check_parse_postgres!("DROP TABLE IF EXISTS \"users\", \"posts\"");
    check_parse_postgres!("DROP VIEW IF EXISTS \"v1\", \"v2\"");
}

#[test]
fn explain() {
    check_parse_both!("explain graphviz;");
    check_parse_both!("explain graphviz for cache q");
    check_parse_both!("explain last statement;");
    check_parse_both!("explain domains;");
    check_parse_both!("explain caches;");
    check_parse_both!("explain   mAtERIaLIZAtIOns");
    check_parse_both!("EXPLAIN CREATE CACHE FROM SELECT id FROM users WHERE name = $1");
    check_parse_mysql!("EXPLAIN CREATE CACHE FROM SELECT id FROM users WHERE name = ?");
    check_parse_both!("EXPLAIN CREATE CACHE FROM q_000000000000");
}

#[test]
fn placeholders() {
    check_parse_mysql!("SELECT * FROM t WHERE foo = ?");
    check_parse_both!("SELECT * FROM t WHERE foo = :12");
    check_parse_both!("SELECT * FROM t WHERE foo = $12");
}

#[test]
fn inserts() {
    check_parse_mysql!("INSERT INTO users (id, name) VALUES (?, ?);");
    check_parse_both!("INSERT INTO users (id, name) VALUES ($1, $1);");
    check_parse_both!("INSERT INTO users VALUES (42, \"test\");");
    check_parse_both!("INSERT INTO users VALUES (42, 'test', 'test', CURRENT_TIMESTAMP);");
    check_parse_both!("INSERT INTO users (id, name) VALUES (42, 'test');");
    check_parse_both!("INSERT INTO users(id, name) VALUES(42, 'test');");
    check_parse_both!("INSERT INTO db1.users VALUES (42, \"test\");");
    check_parse_both!("INSERT INTO users (id, name) VALUES (42, \"test\"),(21, \"test2\");");
    check_parse_mysql!(
        "INSERT INTO keystores (`key`, `value`) VALUES ($1, :2) \
            ON DUPLICATE KEY UPDATE `value` = `value` + 1"
    );
    check_parse_postgres!(
        r#"INSERT INTO keystores ("key", "value") VALUES ($1, :2)
            ON DUPLICATE KEY UPDATE "value" = "value" + 1"#
    );
    check_parse_both!("INSERT INTO users (id, name) VALUES ( 42, \"test\");");
    check_parse_mysql!("INSERT INTO users (`id`, `name`, `key`) VALUES (1, 'bob', 1);");
    check_parse_postgres!(r#"INSERT INTO users ("id", "name", "key") VALUES (1, 'bob', 1);"#);
    check_parse_both!("INSERT INTO users VALUES (42, 'test');");
    check_parse_both!("INSERT INTO users VALUES (42, 'test', 'test', CURRENT_TIMESTAMP);");
    check_parse_both!("INSERT INTO users (id, name) VALUES (42, 'test');");
    check_parse_both!("INSERT INTO users(id, name) VALUES(42, 'test');");
    check_parse_both!("INSERT INTO db1.users VALUES (42, 'test');");
    check_parse_both!("INSERT INTO users (id, name) VALUES (42, 'test'),(21, 'test2');");
    check_parse_postgres!(
        r#"INSERT INTO keystores ("key", "value") VALUES ($1, :2)
            ON DUPLICATE KEY UPDATE "value" = "value" + 1"#
    );
    check_parse_both!("INSERT INTO users (id, name) VALUES ( 42, 'test');");
    check_parse_both!("   INSERT INTO users VALUES (42, \"test\");     ");
    check_parse_both!("INSERT INTO users VALUES (42, \"test\");");
    check_parse_both!("INSERT INTO users VALUES (42, \"test\");");
    check_parse_both!("INSERT INTO users VALUES (42, \"test\");");
}

#[test]
fn joins() {
    check_parse_both!("SELECT tags.* FROM tags INNER JOIN taggings ON (tags.id = taggings.tag_id)");
    check_parse_mysql!(
        "SELECT `tags`.* FROM `tags` INNER JOIN `taggings` ON (`tags`.`id` = `taggings`.`tag_id`)"
    );
    check_parse_postgres!(
        r#"SELECT "tags".* FROM "tags" INNER JOIN "taggings" ON ("tags"."id" = "taggings"."tag_id")"#
    );
}

#[test]
fn literals() {
    for literal in [
        "1.5",
        "0.0",
        "1.500000000000000000000000000000",
        &(i64::MAX as u64 + 1).to_string(),
        &(i64::MIN).to_string(),
        "true",
        "True",
        "TruE",
        "TRUE",
        "false",
        "False",
        "FalsE",
        "FALSE",
        "X'01aF'",
    ] {
        check_parse_both!(format!("SELECT {} as literal", literal));
    }
}

#[test]
fn order() {
    check_parse_both!("select * from users order by name desc");
    check_parse_both!("select * from users order by name asc, age desc");
    check_parse_both!("select * from users order by name");
    check_parse_both!("SELECT * FROM users ORDER BY t1.x ASC NULLS FIRST");
    check_parse_mysql!("SELECT * FROM users ORDER BY `t`.`n` DESC");
    check_parse_postgres!("SELECT * FROM users ORDER BY \"t\".\"n\" DESC");
}

#[test]
fn misc_parser() {
    check_parse_mysql!("SELECT * FROM `users`");
    check_parse_mysql!("SELECT * FROM `users` AS `u`");
    check_parse_mysql!("SELECT `name`, `password` FROM `users` AS `u`");
    check_parse_mysql!("SELECT `name`, `password` FROM `users` AS `u` WHERE (`user_id` = '1')");
    check_parse_mysql!(
        "SELECT `name`, `password` FROM `users` AS `u` WHERE ((`user` = 'aaa') AND (`password` = 'xxx'))"
    );
    check_parse_mysql!("SELECT (`name` * 2) AS `double_name` FROM `users`");
    check_parse_both!("select * from users u");
    check_parse_both!("select name,password from users u;");
    check_parse_both!("select name,password from users u WHERE user_id='1'");
    check_parse_mysql!("SELECT * FROM `users` AS `u`");
    check_parse_mysql!("SELECT `name`, `password` FROM `users` AS `u`");
    check_parse_mysql!("SELECT `name`, `password` FROM `users` AS `u` WHERE (`user_id` = '1')");
    check_parse_mysql!(
        "select name, password from users as u where user='aaa' and password= 'xxx'"
    );
    check_parse_both!("select name, password from users as u where user='aaa' and password= 'xxx'");
    check_parse_mysql!("select name, password from users as u where user=? and password =?");
    check_parse_mysql!("select name, password from users as u where user=? and password =?");
    check_parse_mysql!(
        "SELECT `name`, `password` FROM `users` AS `u` WHERE ((`user` = 'aaa') AND (`password` = 'xxx'))"
    );
    check_parse_mysql!(
        "SELECT `name`, `password` FROM `users` AS `u` WHERE ((`user` = ?) AND (`password` = ?))"
    );
    check_parse_both!("select count(*) from users");
    check_parse_mysql!("SELECT count(*) FROM `users`");
    check_parse_both!("INSERT INTO users (name, password) VALUES ('aaa', 'xxx')");
    check_parse_mysql!("INSERT INTO `users` (`name`, `password`) VALUES ('aaa', 'xxx')");
    check_parse_both!("INSERT INTO users VALUES ('aaa', 'xxx')");
    check_parse_mysql!("INSERT INTO `users` () VALUES ('aaa', 'xxx')");
    check_parse_both!("insert into users (name, password) values ('aaa', 'xxx')");
    check_parse_mysql!("INSERT INTO `users` (`name`, `password`) VALUES ('aaa', 'xxx')");
    check_parse_both!("update users set name=42, password='xxx' where id=1");
    check_parse_mysql!("UPDATE `users` SET `name` = 42, `password` = 'xxx' WHERE (`id` = 1)");
    check_parse_both!("delete from users where user='aaa' and password= 'xxx'");
    check_parse_mysql!("delete from users where user=? and password =?");
    check_parse_mysql!("DELETE FROM `users` WHERE ((`user` = 'aaa') AND (`password` = 'xxx'))");
    check_parse_mysql!("DELETE FROM `users` WHERE ((`user` = ?) AND (`password` = ?))");
}

#[test]
fn select_modifiers() {
    let index_hint_type_list = ["USE", "IGNORE", "FORCE"];
    let index_or_key_list = ["INDEX", "KEY"];
    let index_for_list = ["", " FOR JOIN", " FOR ORDER BY", " FOR GROUP BY"];
    let index_name_list = ["index_name", "primary", "index_name1"];
    for hint_type in index_hint_type_list {
        for index_or_key in index_or_key_list.iter() {
            for index_for in index_for_list.iter() {
                let mut formatted_index_list_str = String::new();
                let mut index_list = vec![];
                for n in index_name_list.iter() {
                    index_list.push(n);
                    if formatted_index_list_str.is_empty() {
                        formatted_index_list_str = n.to_string();
                    } else {
                        formatted_index_list_str = format!("{formatted_index_list_str}, {n}");
                    }
                    let index_hint_str = format!(
                        "{hint_type} {index_or_key}{index_for} ({formatted_index_list_str})"
                    );
                    check_parse_mysql!(format!("SELECT * FROM `users` {index_hint_str}"));
                }
            }
        }
    }
}

#[test]
fn rename() {
    check_parse_mysql!("RENAME TABLE t1 TO t2");
    check_parse_mysql!("RENAME TABLE `t1` TO `t2`");
    check_parse_mysql!("RENAME TABLE `from` TO `to`");
    check_parse_mysql!("RENAME TABLE `from` TO `to`");
    check_parse_mysql!("RENAME TABLE t1 TO t2, `change` to t3, t4 to `select`");
    check_parse_mysql!("rename table `posts_likes` to `post_likes`");
    check_parse_mysql!("RENAME TABLE `posts_likes` TO `post_likes`");
    check_parse_postgres!(r#"RENAME TABLE t1 TO t2"#);
    check_parse_postgres!(r#"RENAME TABLE "t1" TO "t2""#);
    check_parse_postgres!(r#"RENAME TABLE "from" TO "to""#);
    check_parse_postgres!(r#"RENAME TABLE "from" TO "to""#);
    check_parse_postgres!(r#"RENAME TABLE t1 TO t2, "change" to t3, t4 to "select""#);
    check_parse_postgres!(r#"RENAME TABLE "t1" TO "t2", "change" TO "t3", "t4" TO "select""#);
    check_parse_postgres!(r#"rename table "posts_likes" to "post_likes""#);
    check_parse_postgres!(r#"RENAME TABLE "posts_likes" TO "post_likes""#);
}

#[test]
#[ignore = "REA-5855"]
fn rls() {
    check_parse_postgres!(
        r#"CREATE RLS ON "public"."rls_test" IF NOT EXISTS USING ("auth_id", "user_id") = (my.var_auth_id, my.var_user_id)"#
    );
    check_parse_postgres!(r#"DROP RLS ON "public"."rls_test""#);
    check_parse_postgres!("DROP ALL RLS");
    check_parse_postgres!("SHOW RLS ON public.test");
    check_parse_postgres!("SHOW ALL RLS");
}

#[test]
fn select() {
    check_parse_both!("SELECT id, name FROM users;");
    check_parse_fails!(
        Dialect::MySQL,
        "SELECT * FROM;",
        "Expected: identifier, found: ;"
    );
    check_parse_both!("SELECT 1");
    check_parse_both!("SELECT 1 ORDER BY 1 LIMIT 1");
    check_parse_both!("SELECT users.id, users.name FROM users;");
    check_parse_both!("SELECT * FROM users;");
    check_parse_both!("SELECT users.* FROM users, votes;");
    check_parse_both!("SELECT id,name FROM users;");
    check_parse_both!("select id, name from users;");
    check_parse_both!("SELECT id, name FROM users;");
    check_parse_both!("select id, name from users;");
    check_parse_both!("select id, name from users");
    check_parse_both!("select id, name from users\n");
    check_parse_both!("select id, name from users\n;");
    check_parse_mysql!("select * from ContactInfo where email=?;");
    check_parse_both!("select * from ContactInfo where email= $3;");
    check_parse_both!("select * from ContactInfo where email= :5;");
    check_parse_both!("select * from users limit 10\n");
    check_parse_both!("select * from users limit 10 offset 10\n");
    check_parse_mysql!("select * from users limit 5, 10\n");

    // REA-5766
    check_parse_fails!(
        Dialect::PostgreSQL,
        "select * from users limit all\n",
        "nom-sql AST differs from sqlparser-rs AST"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "select * from users limit all offset 10\n",
        "nom-sql AST differs from sqlparser-rs AST"
    );

    check_parse_postgres!("select * from users offset 10\n");
    check_parse_both!("select * from PaperTag as t;");
    check_parse_both!("select * from db1.PaperTag as t;");
    check_parse_both!("select name as TagName from PaperTag;");
    check_parse_both!("select PaperTag.name as TagName from PaperTag;");
    check_parse_both!("select name TagName from PaperTag;");
    check_parse_both!("select PaperTag.name TagName from PaperTag;");
    check_parse_mysql!("select distinct tag from PaperTag where paperId=?;");
    check_parse_mysql!("select data from Records where ROW(recordId, recordType) = (?, ?);");
    check_parse_mysql!("select infoJson from PaperStorage where paperId=? and paperStorageId=?;");
    check_parse_mysql!("select * from users where id = ? limit 10\n");
    check_parse_mysql!("select * from users limit ?");
    check_parse_both!("select distinct tag from PaperTag where paperId=$1;");
    check_parse_both!("select data from Records where ROW(recordId, recordType) = ($1, $2);");
    check_parse_both!("select infoJson from PaperStorage where paperId=$1 and paperStorageId=$2;");
    check_parse_both!("select * from users where id = $1 limit 10\n");
    check_parse_both!("select * from users limit $1");
    check_parse_both!("SELECT max(addr_id) FROM address;");
    check_parse_both!("SELECT max(addr_id) AS max_addr FROM address;");
    check_parse_both!("SELECT COUNT(*) FROM votes GROUP BY aid;");
    check_parse_both!("SELECT COUNT(DISTINCT vote_id) FROM votes GROUP BY aid;");
    check_parse_both!(
        "SELECT COUNT(CASE WHEN vote_id > 10 THEN vote_id END) FROM votes GROUP BY aid;"
    );
    check_parse_both!("SELECT SUM(CASE WHEN sign = 1 THEN vote_id END) FROM votes GROUP BY aid;");
    check_parse_both!("SELECT SUM(CASE WHEN sign = 1 THEN vote_id END) FROM votes GROUP BY aid;");
    check_parse_both!(
        "SELECT SUM(CASE WHEN sign = 1 THEN vote_id ELSE 6 END) FROM votes GROUP BY aid;"
    );
    check_parse_both!(
        "SELECT
            COUNT(CASE WHEN votes.story_id IS NULL AND votes.vote = 0 THEN votes.vote END) as votes
            FROM votes
            GROUP BY votes.comment_id;"
    );
    check_parse_both!("SELECT coalesce(a, b,c) as x,d FROM sometable;");
    check_parse_mysql!(
        "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND \
        item.i_subject = ? ORDER BY item.i_title limit 50;"
    );
    check_parse_postgres!(
        "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND \
        item.i_subject = $1 ORDER BY item.i_title limit 50;"
    );
    check_parse_both!("select paperId from PaperConflict join PCMember using (contactId);");
    // slightly simplified from
    // "select PCMember.contactId, group_concat(reviewType separator '')
    // from PCMember left join PaperReview on (PCMember.contactId=PaperReview.contactId)
    // group by PCMember.contactId"
    check_parse_both!(
        "select PCMember.contactId \
               from PCMember \
               join PaperReview on PCMember.contactId=PaperReview.contactId \
               order by contactId;"
    );
    // simplified from
    // "select max(conflictType), PaperReview.contactId as reviewer, PCMember.contactId as
    //  pcMember, ChairAssistant.contactId as assistant, Chair.contactId as chair,
    //  max(PaperReview.reviewNeedsSubmit) as reviewNeedsSubmit from ContactInfo
    //  left join PaperReview using (contactId) left join PaperConflict using (contactId)
    //  left join PCMember using (contactId) left join ChairAssistant using (contactId)
    //  left join Chair using (contactId) where ContactInfo.contactId=?
    //  group by ContactInfo.contactId;";
    check_parse_mysql!(
        "select PCMember.contactId, ChairAssistant.contactId, \
               Chair.contactId from ContactInfo left join PaperReview using (contactId) \
               left join PaperConflict using (contactId) left join PCMember using \
               (contactId) left join ChairAssistant using (contactId) left join Chair \
               using (contactId) where ContactInfo.contactId=?;"
    );
    check_parse_both!(
        "select PCMember.contactId, ChairAssistant.contactId, \
               Chair.contactId from ContactInfo left join PaperReview using (contactId) \
               left join PaperConflict using (contactId) left join PCMember using \
               (contactId) left join ChairAssistant using (contactId) left join Chair \
               using (contactId) where ContactInfo.contactId=$1;"
    );
    check_parse_both!(
        "SELECT ol_i_id FROM orders, order_line \
            WHERE orders.o_c_id IN (SELECT o_c_id FROM orders, order_line \
            WHERE orders.o_id = order_line.ol_o_id);"
    );
    check_parse_both!(
        "SELECT ol_i_id FROM orders, order_line WHERE orders.o_c_id \
            IN (SELECT o_c_id FROM orders, order_line \
            WHERE orders.o_id = order_line.ol_o_id \
            AND orders.o_id > (SELECT MAX(o_id) FROM orders));"
    );
    check_parse_both!("SELECT x FROM (SELECT x FROM t) sq WHERE x = 1");
    check_parse_both!(
        "SELECT o_id, ol_i_id FROM orders JOIN \
            (SELECT ol_i_id FROM order_line) AS ids \
            ON (orders.o_id = ids.ol_i_id);"
    );
    check_parse_both!("SELECT MAX(o_id)-3333 FROM orders;");
    check_parse_both!("SELECT max(o_id) * 2 as double_max FROM orders;");
    check_parse_mysql!("SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)");
    check_parse_both!("SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)");
    check_parse_mysql!(
        "SELECT id, CAST(created_at AS date) AS created_day FROM users WHERE id = ?;"
    );
    check_parse_both!(
        "SELECT id, CAST(created_at AS date) AS created_day FROM users WHERE id = $1;"
    );
    check_parse_both!(
        "WITH max_val AS (SELECT max(value) as value FROM t1)
            SELECT name FROM t2 JOIN max_val ON max_val.value = t2.value"
    );
    check_parse_both!(
        "WITH
            max_val AS (SELECT max(value) as value FROM t1),
            min_val AS (SELECT min(value) as value FROM t1)
        SELECT name FROM t2
            JOIN max_val ON max_val.value = t2.max_value
            JOIN min_val ON min_val.value = t2.min_value"
    );
    check_parse_both!("SELECT id, coalesce(a, \"b\",c) AS created_day FROM users;");
    check_parse_both!("SELECT NULL, 1, \"foo\", CURRENT_TIME FROM users;");
    // SQL        let qstring = "SELECT NULL, 1, \"foo\";";
    check_parse_mysql!(
        "SELECT `auth_permission`.`content_type_id`, `auth_permission`.`codename`
           FROM `auth_permission`
           JOIN `django_content_type`
             ON ( `auth_permission`.`content_type_id` = `django_content_type`.`id` )
           WHERE `auth_permission`.`content_type_id` IN (0);"
    );
    check_parse_both!("SELECT id, count(*) FROM t GROUP BY 1");
    check_parse_both!("SELECT id FROM t ORDER BY 1");
    check_parse_mysql!("WITH `foo` AS (SELECT `x` FROM `t`) SELECT `x` FROM `foo`");
    check_parse_mysql!("SELECT `x` FROM `t` LIMIT 10 OFFSET 0");
    check_parse_both!("select x, count(*) from t having count(*) > 1");
    check_parse_mysql!("SELECT `x`, count(*) FROM `t` HAVING (count(*) > 1)");
    check_parse_both!("select * from t1 left join t2 on x inner join t3 on z");
    check_parse_both!("SELECT id, coalesce(a, 'b',c) AS created_day FROM users;");
    check_parse_both!("SELECT NULL, 1, 'foo', CURRENT_TIME FROM users;");
    // SQL        let qstring = "SELECT NULL, 1, \"foo\";";
    check_parse_postgres!(
        "SELECT \"auth_permission\".\"content_type_id\", \"auth_permission\".\"codename\"
           FROM \"auth_permission\"
           JOIN \"django_content_type\"
             ON ( \"auth_permission\".\"content_type_id\" = \"django_content_type\".\"id\" )
          WHERE \"auth_permission\".\"content_type_id\" IN (0);"
    );
    check_parse_postgres!("WITH \"foo\" AS (SELECT \"x\" FROM \"t\") SELECT \"x\" FROM \"foo\"");
    check_parse_both!("select x, count(*) from t having count(*) > 1");
    check_parse_postgres!("SELECT \"x\", count(*) FROM \"t\" HAVING (count(*) > 1)");
    check_parse_postgres!(
        "select exists(select * from \"groups\" where \"id\" = $1) as \"exists\""
    );
    check_parse_postgres!(
        "SELECT EXISTS (SELECT * FROM \"groups\" WHERE (\"id\" = $1)) AS \"exists\""
    );
    check_parse_postgres!(
        r#"SELECT "public"."User"."id", "public"."User"."email", "public"."User"."name" FROM "public"."User" WHERE 1=1 OFFSET $1"#
    );
    check_parse_postgres!("SELECT id FROM a OFFSET 5");
    check_parse_postgres!("SELECT id FROM a LIMIT 0.0");
    check_parse_postgres!("SELECT id FROM a LIMIT 0.;");
    check_parse_postgres!("SELECT id FROM a LIMIT 1.1");
    check_parse_postgres!("SELECT id FROM a LIMIT 1.");
    check_parse_postgres!("SELECT id FROM a LIMIT -0.0");
    check_parse_postgres!("SELECT id FROM a LIMIT 3791947566539531989 OFFSET -0.0");
    check_parse_postgres!("select a::numeric as n from t");
    check_parse_postgres!("select a::numeric(1,2) as n from t");
    // This is technically fine syntax, i.e. there could be a custom type named numericas, but
    // nom-sql doesn't parse it.
    check_parse_fails!(
        Dialect::PostgreSQL,
        "select a::numericas n from t",
        "NomSqlError"
    );
}

#[test]
fn set() {
    check_parse_both!("SET SQL_AUTO_IS_NULL = 0;");
    check_parse_mysql!("SET @var = 123;");
    check_parse_both!("set autocommit=1");
    check_parse_mysql!("SET @@LOCAL.autocommit = 1");
    check_parse_mysql!("set gloBal var = 2");
    check_parse_mysql!("set @@gLobal.var = 2");
    check_parse_mysql!("SET @@GLOBAL.var = 2");
    check_parse_mysql!("set @@Session.var = 1");
    check_parse_mysql!("set @@var = 1");
    check_parse_both!("set SeSsion var = 1");
    check_parse_mysql!("SET @@SESSION.var = 1");
    check_parse_both!("set lOcal var = 2");
    check_parse_mysql!("set @@local.var = 2");
    check_parse_mysql!("SET @@LOCAL.var = 2");
    check_parse_both!("SET NAMES 'iso8660'");
    check_parse_both!("set names 'utf8mb4' collate 'utf8mb4_unicode_ci'");
    check_parse_both!("SET NAMES iso8660");
    check_parse_both!("set names utf8mb4 collate utf8mb4_unicode_ci");
    check_parse_mysql!("SET @myvar = 100 + 200;");
    check_parse_mysql!("SET @myvar = 100 + 200, @@notmyvar = 'value', @@Global.g = @@global.V;");
    check_parse_postgres!("SET client_min_messages TO 'warning'");
    check_parse_postgres!("SET SESSION timezone TO 'UTC'");
    check_parse_both!("SET NAMES 'UTF8'");
    check_parse_postgres!("SET SESSION timezone TO DEFAULT");
    check_parse_postgres!("SET SESSION timezone = DEFAULT");
    check_parse_postgres!("SET LOCAL whatever = 'x', 1, hi");
    check_parse_postgres!("SET standard_conforming_strings = on");
}

#[test]
fn show() {
    check_parse_both!("SHOW TABLES");
    check_parse_both!("SHOW FULL TABLES");
    check_parse_both!("SHOW TABLES FROM db1");
    check_parse_both!("SHOW TABLES LIKE 'm%'");
    check_parse_both!("SHOW TABLES FROM db1 WHERE Tables_in_db1 = 't1'");
    check_parse_both!("SHOW EVENTS");
    check_parse_both!("SHOW\tEVENTS");
    check_parse_both!("SHOW CACHES");
    check_parse_both!("SHOW\tCACHES\t");
    check_parse_both!("SHOW CACHES where query_id = 'test'");
    check_parse_both!("SHOW PROXIED QUERIES");
    check_parse_both!("SHOW\tPROXIED\tQUERIES");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES");
    check_parse_both!("SHOW\tPROXIED\tSUPPORTED\tQUERIES");
    check_parse_both!("SHOW PROXIED QUERIES LIMIT 10");
    check_parse_both!("SHOW\tPROXIED\tSUPPORTED\tQUERIES LIMIT 20");
    check_parse_both!("SHOW PROXIED QUERIES where query_id = 'test'");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES where query_id = 'test'");
    check_parse_both!("SHOW PROXIED QUERIES where query_id = 'other'");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES where query_id = 'other'");
    check_parse_both!("SHOW PROXIED QUERIES where query_id = 'test' LIMIT 10");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES where query_id = 'test' LIMIT 20");
    check_parse_both!("SHOW READYSET STATUS");
    check_parse_both!("SHOW\tREADYSET\tSTATUS");
    check_parse_both!("SHOW READYSET VERSION");
    check_parse_both!("SHOW\tREADYSET\tVERSION");
    check_parse_both!("SHOW READYSET TABLES");
    check_parse_both!("SHOW READYSET ALL TABLES");
    check_parse_both!("SHOW\t READYSET\t MIGRATION\t STATUS\t 123456");
    check_parse_both!("SHOW CONNECTIONS");
}

#[test]
fn types() {
    check_parse_type_both!("bigint(20)");
    check_parse_type_both!("bool");
    check_parse_type_both!("integer(16)");
    check_parse_type_both!("datetime(16)");
    check_parse_type_both!("boolean");
    check_parse_type_both!("json");
    check_parse_type_both!("double(16,12)");
    check_parse_type_mysql!("mediumint(8)");
    check_parse_type_mysql!("mediumint");
    check_parse_type_both!("point");
    check_parse_type_mysql!("point srid 4326");
    check_parse_type_both!("NUMERIC");
    check_parse_type_both!("NUMERIC(10)");
    check_parse_type_both!("NUMERIC(10, 20)");
    check_parse_type_postgres!("\"char\"");
    check_parse_type_both!("macaddr");
    check_parse_type_both!("inet");
    check_parse_type_both!("uuid");
    check_parse_type_both!("jsonb");
    check_parse_type_both!("geometry(point)");
    check_parse_type_both!("geometry(point, 4326)");
    check_parse_type_both!("bit");
    check_parse_type_both!("bit(10)");
    check_parse_type_both!("bit varying");
    check_parse_type_both!("bit varying(10)");
    check_parse_type_both!("timestamp");
    check_parse_type_both!("timestamp (5)");
    check_parse_type_both!("timestamp without time zone");
    check_parse_type_both!("timestamp (5)   without time zone");
    check_parse_type_both!("timestamp with time zone");
    check_parse_type_both!("timestamptz");
    check_parse_type_both!("timestamp (5)    with time zone");
    check_parse_type_both!("serial");
    check_parse_type_both!("bigserial");
    check_parse_type_both!("varchar");
    check_parse_type_both!("character varying");
    check_parse_type_both!("character varying(20)");
    check_parse_type_both!("character(16)");
    check_parse_type_both!("time without time zone");
    check_parse_type_postgres!("unsupportedtype");
    check_parse_type_postgres!("foo.custom");
    check_parse_type_postgres!("int[]");
    check_parse_type_postgres!("text[][]");
    check_parse_type_postgres!("float[4][5]");
    check_parse_type_both!("citext");
    check_parse_type_both!("int2");
    check_parse_type_both!("int4");
    check_parse_type_both!("int8");
    check_parse_type_both!("interval");
    check_parse_type_postgres!("tsvector");
}

#[test]
#[ignore = "REA-5844"]
fn binary_modifier() {
    check_parse_type_mysql!("varchar(255) binary");
}

#[test]
#[ignore = "REA-5856"]
fn signed_modifier() {
    check_parse_type_mysql!("bigint(20) unsigned");
    check_parse_type_mysql!("bigint(20) signed");
    check_parse_type_mysql!("decimal(6,2) unsigned");
    check_parse_type_mysql!("numeric(2) unsigned");
    check_parse_type_mysql!("float unsigned");
}

#[test]
#[ignore = "REA-5857"]
fn interval_type() {
    check_parse_type_both!("interval dAY");
    check_parse_type_both!("INTERVAL hour to mINuTe (4)");
}

#[test]
fn transactions() {
    check_parse_both!("    START       TRANSACTION ;  ");
    check_parse_postgres!(" BEGIN  TRANSACTION; ");
    check_parse_both!("    BEGIN       WORK;   ");
    check_parse_both!("    BEGIN;   ");
    check_parse_both!("    COMMIT");
    check_parse_both!("    COMMIT       WORK   ");
    check_parse_both!("    COMMIT");
    check_parse_both!("    COMMIT       WORK   ");
    check_parse_postgres!("    COMMIT       TRANSACTION   ");
    check_parse_postgres!("    END");
    check_parse_postgres!("    END       WORK   ");
    check_parse_postgres!("    END       TRANSACTION   ");
    check_parse_both!("    ROLLBACK ");
    check_parse_both!("    ROLLBACK       WORK   ");
}

#[test]
fn truncate() {
    check_parse_both!("truncate mytable");
    check_parse_both!("tRUNcate mydb.mytable");
    check_parse_both!("truncate table mytable");
    check_parse_postgres!("truncate t1, t2");
    check_parse_postgres!("truncate t1 restart identity");
    check_parse_postgres!("truncate t1 cascade");
    check_parse_both!("trunCATe mytable");
    check_parse_both!("truncate mydb.mytable");
    check_parse_both!("truncate table mytable");
    check_parse_postgres!("truncate table table1, table2");
    check_parse_postgres!("truncate ONLY mytable");
    check_parse_postgres!("truncate t1, only t2");
    check_parse_postgres!("truncate mytable continue identity");
    check_parse_postgres!("truncate mytable restart identity");
    check_parse_postgres!("truncate mytable restrict");
    check_parse_postgres!("truncate mytable cascade");
    check_parse_postgres!("truncate mytable restart identity cascade");
    // REA-5858
    check_parse_fails!(
        Dialect::PostgreSQL,
        "truncate t1 *, t2*, t3",
        "sqlparser error"
    );
    check_parse_postgres!(r#"TRUNCATE "t1", ONLY "t2" RESTART IDENTITY CASCADE"#);
}

#[test]
fn update() {
    check_parse_both!("UPDATE users SET id = 42, name = 'test'");
    check_parse_both!("UPDATE users SET id = 42, name = 'test' WHERE id = 1");
    check_parse_mysql!("UPDATE users SET karma = karma + 1 WHERE users.id = ?;");
    check_parse_both!("UPDATE users SET karma = karma + 1 WHERE users.id = $1;");
    check_parse_mysql!("UPDATE `stories` SET `hotness` = -19216.5479744 WHERE `stories`.`id` = ?");
    check_parse_both!("UPDATE users SET karma = karma + 1;");
    check_parse_both!("UPDATE users SET id = 42, name = 'test' WHERE id = 1");
    check_parse_mysql!("UPDATE `users` SET `id` = 42, `name` = 'test' WHERE (`id` = 1)");
    check_parse_mysql!(
        "update `group_permission` set `permission` = REPLACE(permission,  'viewDiscussions', 'viewForum') where `permission` LIKE '%viewDiscussions'"
    );
    check_parse_postgres!(
        r#"update "group_permission" set "permission" = REPLACE(permission,  'viewDiscussions', 'viewForum') where "permission" LIKE '%viewDiscussions'"#
    );
    check_parse_postgres!(
        "UPDATE \"stories\" SET \"hotness\" = -19216.5479744 WHERE \"stories\".\"id\" = $1"
    );
    check_parse_postgres!("UPDATE \"users\" SET \"id\" = 42, \"name\" = 'test' WHERE (\"id\" = 1)");
}
