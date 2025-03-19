use crate::common::field_list;
use crate::table::relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{failed, NomSqlError, NomSqlResult};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::error::ErrorKind;
use nom::sequence::{preceded, tuple};
use nom::Parser;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

/// Parse rule for our custom `CREATE RLS ON <table> [IF NOT EXISTS] USING (col1, col2, ...) = (var1, var2, ...)` statement.
pub fn create_rls(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateRlsStatement> {
    move |i| {
        let (i, _) = tag_no_case("create")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("rls")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("on")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, rls_table) = relation(dialect)(i)?;
        let (i, if_not_exists) = map(
            opt(preceded(
                whitespace1,
                tuple((
                    tag_no_case("if"),
                    whitespace1,
                    tag_no_case("not"),
                    whitespace1,
                    tag_no_case("exists"),
                )),
            )),
            |all| all.is_some(),
        )(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("using")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, mut columns) = field_list(dialect)(i)?;
        let (i, _) = tag(")")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("=")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, variables) = field_list(dialect)(i)?;
        let (i, _) = tag(")")(i)?;

        // Make sure, the RLS table is qualified with the schema name
        if rls_table.schema.is_none() {
            failed!(i)
        }

        // Make sure `columns` and `variables` have the same arity
        if columns.len() != variables.len() {
            failed!(i)
        }

        // Make sure the columns do not reference table, and make it reference the RLS table.
        for col in &mut columns {
            match &col.table {
                None => {
                    col.table = Some(rls_table.clone());
                }
                Some(table) => {
                    if !table.eq(&rls_table) {
                        failed!(i)
                    }
                }
            }
        }

        // Since we use for the variables and the columns the same parser,
        // make sure all names are 2 parts: [prefix.name].
        // We will interpret table name as the `prefix` part,
        // and column name as the `name` part, making sure no schema was specified.
        for var in &variables {
            if !matches!(&var.table, Some(rel) if rel.schema.is_none()) {
                failed!(i)
            }
        }

        let stmt = CreateRlsStatement {
            table: rls_table,
            policy: columns
                .into_iter()
                .zip(variables)
                .map(|(col, var)| {
                    (
                        col,
                        Variable {
                            scope: VariableScope::Session,
                            name: var.display_unquoted().to_string().into(),
                        },
                    )
                })
                .collect(),
            if_not_exists,
        };

        Ok((i, stmt))
    }
}

pub(crate) fn parse_rest_of_drop_or_show_rls(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Option<Relation>> {
    move |i| {
        let (i, maybe_table) = alt((
            tuple((tag_no_case("all"), whitespace1, tag_no_case("rls"))).map(|_| None),
            tuple((
                tag_no_case("rls"),
                whitespace1,
                tag_no_case("on"),
                whitespace1,
                relation(dialect),
            ))
            .map(|(_, _, _, _, table)| Some(table)),
        ))(i)?;
        if let Some(table) = &maybe_table {
            if table.schema.is_none() {
                failed!(i)
            }
        }
        Ok((i, maybe_table))
    }
}

/// Parse rule for our custom `DROP {ALL RLS | RLS ON <table>}` statement.
pub fn drop_rls(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropRlsStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, table) = parse_rest_of_drop_or_show_rls(dialect)(i)?;
        Ok((i, DropRlsStatement { table }))
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_query;
    use readyset_sql::analysis::visit::Visitor;
    use readyset_sql::ast::{Column, CreateRlsStatement, Relation, SqlQuery, Variable};
    use readyset_sql::{Dialect, DialectDisplay};

    macro_rules! verify_parsed_statement {
        ($statement_text:expr, $sql_query:ident) => {
            match parse_query(Dialect::PostgreSQL, $statement_text) {
                Ok(SqlQuery::$sql_query(parsed_stmt)) => {
                    assert_eq!(
                        $statement_text,
                        parsed_stmt.display(Dialect::PostgreSQL).to_string()
                    );
                    parsed_stmt
                }
                Err(e) => {
                    panic!("{e}");
                }
                _ => unreachable!(),
            }
        };
    }

    #[test]
    fn create_rls_test() {
        let stmt_text = r#"CREATE RLS ON "public"."rls_test" IF NOT EXISTS USING ("auth_id", "user_id") = (my.var_auth_id, my.var_user_id)"#;
        let stmt = verify_parsed_statement!(stmt_text, CreateRls);

        struct RlsPolicyVisitor {
            stmt: CreateRlsStatement,
        }

        impl<'ast> Visitor<'ast> for RlsPolicyVisitor {
            type Error = std::convert::Infallible;

            fn visit_table(&mut self, table: &'ast Relation) -> Result<(), Self::Error> {
                self.stmt.table = table.clone();
                Ok(())
            }

            fn visit_rls_rule(
                &mut self,
                col: &'ast Column,
                var: &'ast Variable,
            ) -> Result<(), Self::Error> {
                self.stmt.policy.push((col.clone(), var.clone()));
                Ok(())
            }

            fn visit_rls_if_not_exists(&mut self, if_not_exists: bool) -> Result<(), Self::Error> {
                self.stmt.if_not_exists = if_not_exists;
                Ok(())
            }
        }

        let mut vis = RlsPolicyVisitor {
            stmt: CreateRlsStatement::new("".into(), false),
        };
        vis.visit_create_rls_statement(&stmt).unwrap();

        assert_eq!(vis.stmt, stmt);
    }

    #[test]
    fn drop_rls_test() {
        let stmt_text = r#"DROP RLS ON "public"."rls_test""#;
        verify_parsed_statement!(stmt_text, DropRls);
    }

    #[test]
    fn drop_all_rls_test() {
        let stmt_text = "DROP ALL RLS";
        verify_parsed_statement!(stmt_text, DropRls);
    }
}
