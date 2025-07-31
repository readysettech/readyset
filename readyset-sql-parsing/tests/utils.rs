#[macro_export]
macro_rules! check_parse_mysql {
    ($sql:expr) => {
        parse_query_with_config(
            ParsingPreset::BothPanicOnMismatch
                .into_config()
                .log_on_mismatch(true),
            Dialect::MySQL,
            $sql,
        )
        .expect(&format!("Failed to parse MySQL query: {}", $sql))
    };
}

#[macro_export]
macro_rules! check_parse_postgres {
    ($sql:expr) => {
        parse_query_with_config(
            ParsingPreset::BothPanicOnMismatch
                .into_config()
                .log_on_mismatch(true),
            Dialect::PostgreSQL,
            $sql,
        )
        .expect(&format!("Failed to parse PostgreSQL query: {}", $sql))
    };
}

#[macro_export]
macro_rules! check_parse_both {
    ($sql:expr) => {{
        $crate::check_parse_mysql!($sql);
        $crate::check_parse_postgres!($sql)
    }};
}

#[macro_export]
macro_rules! check_parse_fails {
    ($dialect:expr, $sql:expr, $expected_error:expr) => {
        let result = parse_query_with_config(ParsingPreset::BothErrorOnMismatch, $dialect, $sql)
            .expect_err(&format!("Expected failure for {:?}: {:?}", $dialect, $sql));
        assert!(
            result.to_string().contains($expected_error),
            "Expected error '{}' not found: got {}",
            $expected_error,
            result
        );
    };
}

#[macro_export]
macro_rules! check_rt_mysql {
    ($sql:expr) => {
        let ast = $crate::check_parse_mysql!($sql);
        let displayed = ast.display(Dialect::MySQL).to_string();
        let displayed_ast = $crate::check_parse_mysql!(&displayed);
        pretty_assertions::assert_eq!(ast, displayed_ast);
    };
}

#[macro_export]
macro_rules! check_rt_postgres {
    ($sql:expr) => {
        let ast = dbg!($crate::check_parse_postgres!($sql));
        let displayed = dbg!(ast.display(Dialect::PostgreSQL).to_string());
        let displayed_ast = dbg!($crate::check_parse_postgres!(&displayed));
        pretty_assertions::assert_eq!(ast, displayed_ast);
    };
}

#[macro_export]
macro_rules! check_rt_both {
    ($sql:expr) => {
        $crate::check_rt_mysql!($sql);
        $crate::check_rt_postgres!($sql);
    };
}

#[macro_export]
macro_rules! check_parse_type_mysql {
    ($ty:expr) => {
        check_parse_mysql!(concat!("CREATE TABLE t (x ", $ty, ")"));
        check_parse_mysql!(concat!("ALTER TABLE t ADD x ", $ty));
    };
}

#[macro_export]
macro_rules! check_parse_type_postgres {
    ($ty:expr) => {
        check_parse_postgres!(concat!("CREATE TABLE t (x ", $ty, ")"));
        check_parse_postgres!(concat!("ALTER TABLE t ADD x ", $ty));
    };
}

#[macro_export]
macro_rules! check_parse_type_both {
    ($ty:expr) => {
        check_parse_type_mysql!($ty);
        check_parse_type_postgres!($ty);
    };
}

#[macro_export]
macro_rules! check_parse_type_fails {
    ($dialect:expr, $ty:expr, $expected_error:expr) => {
        check_parse_fails!(
            $dialect,
            concat!("CREATE TABLE t (x ", $ty, ")"),
            $expected_error
        );
        check_parse_fails!(
            $dialect,
            concat!("ALTER TABLE t ADD x ", $ty),
            $expected_error
        );
    };
}
