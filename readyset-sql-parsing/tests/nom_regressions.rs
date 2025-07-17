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
