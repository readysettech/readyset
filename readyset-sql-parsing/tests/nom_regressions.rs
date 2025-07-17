use readyset_sql::Dialect;
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

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
    ] {
        check_parse_both!(format!("SELECT {expr}"));
        check_parse_both!(format!("SELECT {expr} AS expr"));
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
    ] {
        check_parse_mysql!(format!("SELECT {expr}"));
    }
}

#[test]
fn expr_postgres() {
    for expr in [
        r#"cast("lp"."start_ddtm" as date)"#,
        r#"lower('AbC' COLLATE "es_ES")"#,
        r#"upper('AbC' COLLATE "es_ES")"#,
    ] {
        check_parse_postgres!(format!("SELECT {expr}"));
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
