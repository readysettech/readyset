//! Parsing of MySQL `ZEROFILL` columns.

use readyset_sql::ast::{
    AlterTableDefinition, AlterTableStatement, ColumnConstraint, ColumnSpecification,
    CreateTableStatement, Expr, Literal, SqlQuery, SqlType,
};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

fn parse(sql: &str) -> SqlQuery {
    parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
}

fn fields(query: SqlQuery) -> Vec<ColumnSpecification> {
    match query {
        SqlQuery::CreateTable(CreateTableStatement { body: Ok(body), .. }) => body.fields,
        other => panic!("expected a CREATE TABLE statement, got {other:?}"),
    }
}

fn zerofill(name: &str, sql_type: SqlType, rest: &[ColumnConstraint]) -> ColumnSpecification {
    let mut constraints = vec![ColumnConstraint::ZeroFill];
    constraints.extend_from_slice(rest);
    ColumnSpecification::with_constraints(name.into(), sql_type, constraints)
}

#[test]
fn create_table_zerofill_columns() {
    let sql = "CREATE TABLE t (
        id int(10) unsigned zerofill NOT NULL AUTO_INCREMENT,
        n int zerofill,
        mi mediumint zerofill,
        m mediumint(8) unsigned zerofill NOT NULL DEFAULT '00000005',
        d decimal(14,0) unsigned zerofill,
        dd decimal zerofill,
        f float zerofill,
        f30 float(30) zerofill,
        s smallint signed zerofill,
        b bigint zerofill,
        i4 int4 zerofill,
        i8 int8(15) zerofill,
        nu numeric(8,2) zerofill,
        r real zerofill,
        ru real unsigned zerofill,
        PRIMARY KEY (id)
    )";
    assert_eq!(
        fields(parse(sql)),
        vec![
            zerofill(
                "id",
                SqlType::IntUnsigned(Some(10)),
                &[ColumnConstraint::NotNull, ColumnConstraint::AutoIncrement]
            ),
            zerofill("n", SqlType::IntUnsigned(Some(10)), &[]),
            zerofill("mi", SqlType::MediumIntUnsigned(Some(8)), &[]),
            zerofill(
                "m",
                SqlType::MediumIntUnsigned(Some(8)),
                &[
                    ColumnConstraint::NotNull,
                    ColumnConstraint::DefaultValue(Expr::Literal(Literal::String(
                        "00000005".into()
                    ))),
                ]
            ),
            zerofill("d", SqlType::Decimal(14, 0), &[]),
            zerofill("dd", SqlType::Decimal(10, 0), &[]),
            zerofill("f", SqlType::Float, &[]),
            zerofill("f30", SqlType::Double, &[]),
            zerofill("s", SqlType::SmallIntUnsigned(Some(5)), &[]),
            zerofill("b", SqlType::BigIntUnsigned(Some(20)), &[]),
            zerofill("i4", SqlType::IntUnsigned(Some(10)), &[]),
            zerofill("i8", SqlType::BigIntUnsigned(Some(15)), &[]),
            zerofill("nu", SqlType::Decimal(8, 2), &[]),
            zerofill("r", SqlType::Double, &[]),
            zerofill("ru", SqlType::Double, &[]),
        ]
    );
    check_rt_mysql_sqlparser!(sql);
}

#[test]
fn alter_table_add_zerofill_column() {
    let sql = "ALTER TABLE t ADD COLUMN z int(6) zerofill NOT NULL";
    match parse(sql) {
        SqlQuery::AlterTable(AlterTableStatement {
            definitions: Ok(definitions),
            ..
        }) => match definitions.as_slice() {
            [AlterTableDefinition::AddColumn { spec, .. }] => assert_eq!(
                *spec,
                zerofill(
                    "z",
                    SqlType::IntUnsigned(Some(6)),
                    &[ColumnConstraint::NotNull]
                )
            ),
            other => panic!("expected one ADD COLUMN, got {other:?}"),
        },
        other => panic!("expected an ALTER TABLE statement, got {other:?}"),
    }
    check_rt_mysql_sqlparser!(sql);
}

#[test]
fn zerofill_float_with_scale_is_unsupported() {
    check_parse_type_fails!(Dialect::MySQL, "float(7,2) zerofill", "ZEROFILL");
    check_parse_type_fails!(Dialect::MySQL, "double(10,3) unsigned zerofill", "ZEROFILL");
}
