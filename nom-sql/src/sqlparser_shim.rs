use core::{panic, str};

use nom::branch::alt;
use nom::combinator::map;
use nom::error::ErrorKind;
use nom::{AsBytes, Slice};
use nom_locate::LocatedSpan;
use rust_decimal::Decimal;

use crate::create::create_cached_query;
use crate::drop::{drop_all_caches, drop_all_proxied_queries, drop_cached_query};
use crate::explain::explain_statement;
use crate::expression::scoped_var;
use crate::literal::dollarsign_placeholder;
use crate::{Dialect, NomSqlError, NomSqlResult};

/// Convert sqlparser-rs's AST to nom-sql's.
///
/// This only attempts to convert statements that the mysql client sends on startup so we need to at least to not die
/// (but see the TODO below), plus `SELECT` statements.
///
/// TODO: This should be `TryFrom`, but I wanted to hard error with clickable line numbers while hacking
impl From<sqlparser::ast::Statement> for crate::SqlQuery {
    fn from(value: sqlparser::ast::Statement) -> Self {
        use sqlparser::ast::Statement::*;
        match value {
            Query(query) => (*query).into(),
            // This is kind of crazy; neither sqlparser-rs nor nom-sql actually support MySQL's `SHOW DATABASES` syntax
            ShowVariable { ref variable } => {
                let variable = variable
                    .iter()
                    .map(|ident| &ident.value)
                    .collect::<Vec<_>>();
                if variable == vec!["databases"] {
                    Self::Show(crate::ShowStatement::Databases)
                } else if variable == vec!["proxied", "queries"] {
                    // TODO: when converting this to `TryFrom`, this should error and cause us to fall back to custom parsing
                    Self::Show(crate::ShowStatement::ProxiedQueries(
                        crate::show::ProxiedQueriesOptions {
                            limit: None,
                            only_supported: false,
                            query_id: None,
                        },
                    ))
                } else if variable == vec!["readyset", "status"] {
                    Self::Show(crate::ShowStatement::ReadySetStatus)
                } else if variable == vec!["cached", "queries"] {
                    Self::Show(crate::ShowStatement::CachedQueries(None))
                } else {
                    unimplemented!("unsupported ShowVariables {value:?}")
                }
            }
            ShowTables {
                full,
                show_options:
                    sqlparser::ast::ShowStatementOptions {
                        show_in,
                        filter_position,
                        ..
                    },
                ..
            } => Self::Show(crate::ShowStatement::Tables(crate::show::Tables {
                full,
                from_db: match show_in {
                    Some(sqlparser::ast::ShowStatementIn {
                        parent_name: Some(parent_name),
                        ..
                    }) => Some(parent_name.to_string()), // TODO: object name can be multipart
                    _ => None,
                },
                filter: match filter_position {
                    Some(sqlparser::ast::ShowStatementFilterPosition::Infix(filter))
                    | Some(sqlparser::ast::ShowStatementFilterPosition::Suffix(filter)) => {
                        Some(filter.into())
                    }
                    None => None,
                },
            })),
            ShowDatabases {
                terse: _,
                history: _,
                show_options: _,
            } => Self::Show(crate::ShowStatement::Databases),
            CreateTable(create) => Self::CreateTable(create.into()),
            Insert(insert) => Self::Insert(insert.into()),
            Delete(delete) => Self::Delete(delete.into()),
            create @ CreateView { .. } => Self::CreateView(create.into()),
            _ => unimplemented!("unsupported query {value:?}"),
        }
    }
}
impl From<sqlparser::ast::Query> for crate::SqlQuery {
    fn from(value: sqlparser::ast::Query) -> Self {
        if matches!(*value.body, sqlparser::ast::SetExpr::Select(_)) {
            crate::SqlQuery::Select(value.into())
        } else {
            todo!("unsupported query {value:?}")
        }
    }
}

impl From<sqlparser::ast::Query> for crate::SelectStatement {
    fn from(value: sqlparser::ast::Query) -> Self {
        let sqlparser::ast::Query {
            body,
            order_by,
            limit,
            offset,
            with,
            ..
        } = value;
        match *body {
            sqlparser::ast::SetExpr::Select(select) => {
                let (tables, join_clauses): (Vec<crate::TableExpr>, Vec<Vec<crate::JoinClause>>) =
                    select
                        .from
                        .into_iter()
                        .map(|table_with_joins| {
                            (
                                table_with_joins.relation.into(),
                                table_with_joins.joins.into_iter().map(Into::into).collect(),
                            )
                        })
                        .unzip();
                let join = join_clauses.into_iter().flatten().collect();
                crate::SelectStatement {
                    ctes: if let Some(sqlparser::ast::With { cte_tables, .. }) = with {
                        cte_tables.into_iter().map(Into::into).collect()
                    } else {
                        Vec::new()
                    },
                    distinct: matches!(select.distinct, Some(sqlparser::ast::Distinct::Distinct)),
                    fields: select.projection.into_iter().map(Into::into).collect(),
                    tables,
                    join,
                    where_clause: select.selection.map(Into::into),
                    group_by: {
                        let group_by: crate::GroupByClause = select.group_by.into();
                        if group_by.fields.is_empty() {
                            None
                        } else {
                            Some(group_by)
                        }
                    },
                    having: select.having.map(Into::into),
                    order: order_by.map(Into::into),
                    limit_clause: crate::LimitClause::LimitOffset {
                        limit: limit.map(|expr| {
                            if let crate::Expr::Literal(literal) = expr.into() {
                                crate::LimitValue::Literal(literal)
                            } else {
                                unimplemented!("non-literal limit expression");
                            }
                        }),
                        offset: offset.map(|sqlparser::ast::Offset { value: expr, .. }| {
                            if let crate::Expr::Literal(literal) = expr.into() {
                                literal
                            } else {
                                unimplemented!("non-literal offset expression");
                            }
                        }),
                    },
                }
            }
            _ => unreachable!("{body:?}"),
        }
    }
}

impl From<sqlparser::ast::Cte> for crate::CommonTableExpr {
    fn from(value: sqlparser::ast::Cte) -> Self {
        Self {
            name: value.alias.name.into(),
            statement: (*value.query).into(),
        }
    }
}

impl From<sqlparser::ast::SelectItem> for crate::FieldDefinitionExpr {
    fn from(value: sqlparser::ast::SelectItem) -> Self {
        use sqlparser::ast::SelectItem::*;
        match value {
            ExprWithAlias { expr, alias } => crate::FieldDefinitionExpr::Expr {
                expr: expr.into(),
                alias: Some(alias.into()),
            },
            UnnamedExpr(expr) => crate::FieldDefinitionExpr::Expr {
                expr: expr.into(),
                alias: None,
            },
            QualifiedWildcard(relation, _options) => {
                crate::FieldDefinitionExpr::AllInTable(relation.into())
            }
            Wildcard(_options) => crate::FieldDefinitionExpr::All,
        }
    }
}

impl From<sqlparser::ast::Ident> for crate::SqlIdentifier {
    fn from(value: sqlparser::ast::Ident) -> Self {
        value.value.into()
    }
}

impl From<sqlparser::ast::ObjectName> for crate::Relation {
    fn from(value: sqlparser::ast::ObjectName) -> Self {
        let mut identifiers = value.0.into_iter().map(Into::into);
        let first = identifiers.next().unwrap_or_default();
        if let Some(second) = identifiers.next() {
            Self {
                name: second,
                schema: Some(first),
            }
        } else {
            Self {
                name: first,
                schema: None,
            }
        }
    }
}

impl From<sqlparser::ast::Expr> for crate::Expr {
    fn from(value: sqlparser::ast::Expr) -> Self {
        use sqlparser::ast::Expr::*;
        match value {
            AllOp {
                left,
                compare_op,
                right,
            } => Self::OpAll {
                lhs: left.into(),
                op: compare_op.into(),
                rhs: right.into(),
            },
            AnyOp {
                left,
                compare_op,
                right,
                is_some: _,
            } => Self::OpAny {
                lhs: left.into(),
                op: compare_op.into(),
                rhs: right.into(),
            },
            Array(array) => Self::Array(array.elem.into_iter().map(Into::into).collect()),
            AtTimeZone {
                timestamp,
                time_zone,
            } => unimplemented!("{timestamp:?} AT TIMEZONE {time_zone:?}"),
            Between {
                expr,
                negated,
                low,
                high,
            } => Self::Between {
                operand: expr.into(),
                min: low.into(),
                max: high.into(),
                negated,
            },
            BinaryOp { left, op, right } => Self::BinaryOp {
                lhs: left.into(),
                op: op.into(),
                rhs: right.into(),
            },
            Case {
                operand: None, // XXX do we really not support the CASE operand?
                conditions,
                results,
                else_result,
            } => Self::CaseWhen {
                branches: conditions
                    .into_iter()
                    .zip(results)
                    .map(|(condition, result)| crate::CaseWhenBranch {
                        condition: condition.into(),
                        body: result.into(),
                    })
                    .collect(),
                else_expr: else_result.map(Into::into),
            },
            Case {
                operand: Some(_expr), // XXX do we really not support the CASE operand?
                conditions: _,
                results: _,
                else_result: _,
            } => todo!(),
            Cast {
                kind,
                expr,
                data_type,
                format: _, // TODO: I think this is where we would support `AT TIMEZONE` syntax
            } => Self::Cast {
                expr: expr.into(),
                ty: data_type.into(),
                postgres_style: kind == sqlparser::ast::CastKind::DoubleColon,
            },
            Ceil { expr: _, field: _ } => todo!(),
            Collate {
                expr: _,
                collation: _,
            } => todo!(),
            CompositeAccess { expr: _, key: _ } => todo!(),
            // TODO: this could be a variable like `@@GLOBAL.foo`, which should go through `ident_into_expr` or similar
            CompoundIdentifier(idents) => Self::Column(idents.into()),
            Convert {
                expr: _,
                data_type: _,
                charset: _,
                target_before_value: _,
                styles: _,
                is_try: _,
            } => todo!(),
            Cube(_vec) => todo!(),
            Dictionary(_vec) => todo!(),
            Exists { subquery, negated } => {
                if negated {
                    Self::Exists(Box::new((*subquery).into()))
                } else {
                    Self::UnaryOp {
                        op: crate::UnaryOperator::Not,
                        rhs: Box::new(Self::Exists(Box::new((*subquery).into()))),
                    }
                }
            }
            Extract {
                field,
                syntax: _, // We only support FROM
                expr,
            } => Self::Call(crate::FunctionExpr::Extract {
                field: field.into(),
                expr: expr.into(),
            }),
            Floor { expr: _, field: _ } => todo!(),
            Function(function) => function.into(),
            GroupingSets(_vec) => todo!(),
            Identifier(ident) => ident_into_expr(ident),
            ILike {
                negated: _,
                expr: _,
                pattern: _,
                escape_char: _,
                any: _,
            } => todo!(),
            InList {
                expr,
                list,
                negated,
            } => Self::In {
                lhs: expr.into(),
                rhs: crate::InValue::List(list.into_iter().map(Into::into).collect()),
                negated,
            },
            InSubquery {
                expr,
                subquery,
                negated,
            } => Self::In {
                lhs: expr.into(),
                rhs: crate::InValue::Subquery(Box::new((*subquery).into())),
                negated,
            },
            Interval(_interval) => todo!(),
            IntroducedString {
                introducer: _,
                value: _,
            } => todo!(),
            InUnnest {
                expr: _,
                array_expr: _,
                negated: _,
            } => todo!(),
            IsDistinctFrom(_expr, _expr1) => todo!(),
            IsFalse(_expr) => todo!(),
            IsNotDistinctFrom(_expr, _expr1) => todo!(),
            IsNotFalse(_expr) => todo!(),
            IsNotNull(expr) => Self::BinaryOp {
                lhs: expr.into(),
                op: crate::BinaryOperator::IsNot,
                rhs: Box::new(crate::Expr::Literal(crate::Literal::Null)),
            },
            IsNotTrue(_expr) => todo!(),
            IsNotUnknown(_expr) => todo!(),
            IsNull(expr) => Self::BinaryOp {
                lhs: expr.into(),
                op: crate::BinaryOperator::Is,
                rhs: Box::new(crate::Expr::Literal(crate::Literal::Null)),
            },
            IsTrue(_expr) => todo!(),
            IsUnknown(_expr) => todo!(),
            JsonAccess { value: _, path: _ } => todo!(),
            Lambda(_lambda_function) => todo!(),
            Like {
                negated,
                expr,
                pattern,
                escape_char: _,
                any: _,
            } => {
                let like = Self::BinaryOp {
                    lhs: expr.into(),
                    op: crate::BinaryOperator::Like,
                    rhs: pattern.into(),
                };
                if negated {
                    Self::UnaryOp {
                        op: crate::UnaryOperator::Not,
                        rhs: Box::new(like),
                    }
                } else {
                    like
                }
            }
            Map(_map) => todo!(),
            MatchAgainst {
                columns: _,
                match_value: _,
                opt_search_modifier: _,
            } => todo!(),
            Method(_method) => todo!(),
            Named { expr: _, name: _ } => todo!(),
            Nested(expr) => (*expr).into(),
            OuterJoin(_expr) => todo!(),
            Overlay {
                expr: _,
                overlay_what: _,
                overlay_from: _,
                overlay_for: _,
            } => todo!(),
            Position { expr: _, r#in: _ } => todo!(),
            Prior(_expr) => todo!(),
            QualifiedWildcard(_object_name, _token) => {
                todo!("Not actually sure how we represent `foo`.* in nom-sql")
            }
            RLike {
                negated: _,
                expr: _,
                pattern: _,
                regexp: _,
            } => todo!(),
            Rollup(_vec) => todo!(),
            SimilarTo {
                negated: _,
                expr: _,
                pattern: _,
                escape_char: _,
            } => todo!(),
            Struct {
                values: _,
                fields: _,
            } => todo!(),
            Subquery(_query) => todo!(),
            Substring {
                expr: _,
                substring_from: _,
                substring_for: _,
                special: _,
            } => todo!(),
            Trim {
                expr: _,
                trim_where: _,
                trim_what: _,
                trim_characters: _,
            } => todo!(),
            Tuple(_vec) => todo!(),
            TypedString {
                data_type: _,
                value: _,
            } => todo!(),
            UnaryOp { op, expr } => Self::UnaryOp {
                op: op.into(),
                rhs: expr.into(),
            },
            Value(value) => Self::Literal(value.into()),
            CompoundFieldAccess {
                root: _,
                access_chain: _,
            } => {
                todo!("foo['bar'].baz[1] - probably unsupported")
            }
            Wildcard(_token) => todo!(),
        }
    }
}

impl From<Box<sqlparser::ast::Expr>> for Box<crate::Expr> {
    fn from(value: Box<sqlparser::ast::Expr>) -> Self {
        Box::new((*value).into())
    }
}

fn ident_into_expr(ident: sqlparser::ast::Ident) -> crate::Expr {
    // TODO: handle different dialects; may require custom alternative to `From` that allows plumbing dialect through
    if let Ok((_, var)) = scoped_var(Dialect::MySQL)(ident.value.as_bytes().into()) {
        crate::Expr::Variable(var)
    } else {
        crate::Expr::Column(ident.into())
    }
}

impl From<sqlparser::ast::BinaryOperator> for crate::BinaryOperator {
    fn from(value: sqlparser::ast::BinaryOperator) -> Self {
        use sqlparser::ast::BinaryOperator::*;
        match value {
            And => Self::And,
            Arrow => Self::Arrow1,
            ArrowAt => Self::AtArrowLeft,
            AtArrow => Self::AtArrowRight,
            AtAt => unimplemented!("@@ {value:?}"),
            AtQuestion => unimplemented!("@? {value:?}"),
            BitwiseAnd => unimplemented!("& {value:?}"),
            BitwiseOr => unimplemented!("| {value:?}"),
            BitwiseXor => unimplemented!("^ {value:?}"),
            Custom(_) => unimplemented!("CUSTOM {value:?}"),
            Divide => Self::Divide,
            DuckIntegerDivide => unimplemented!("Duck // {value:?}"),
            Eq => Self::Equal,
            Gt => Self::Greater,
            GtEq => Self::GreaterOrEqual,
            HashArrow => Self::HashArrow1,
            HashLongArrow => Self::HashArrow2,
            HashMinus => Self::HashSubtract,
            LongArrow => Self::Arrow2,
            Lt => Self::Less,
            LtEq => Self::LessOrEqual,
            Minus => Self::Subtract,
            Modulo => unimplemented!("% {value:?}"),
            Multiply => Self::Multiply,
            MyIntegerDivide => unimplemented!("MySQL DIV {value:?}"),
            NotEq => Self::NotEqual,
            Or => Self::Or,
            Overlaps => todo!(),
            PGBitwiseShiftLeft => todo!(),
            PGBitwiseShiftRight => todo!(),
            PGBitwiseXor => todo!(),
            PGCustomBinaryOperator(_vec) => todo!(),
            PGExp => todo!(),
            PGILikeMatch => todo!(),
            PGLikeMatch => todo!(),
            PGNotILikeMatch => todo!(),
            PGNotLikeMatch => todo!(),
            PGOverlap => todo!(),
            PGRegexIMatch => todo!(),
            PGRegexMatch => todo!(),
            PGRegexNotIMatch => todo!(),
            PGRegexNotMatch => todo!(),
            PGStartsWith => todo!(),
            Plus => Self::Add,
            Question => todo!(),
            QuestionAnd => Self::QuestionMarkAnd,
            QuestionPipe => Self::QuestionMarkPipe,
            Spaceship => unimplemented!("<=> {value:?}"),
            StringConcat => todo!(),
            Xor => todo!(),
        }
    }
}

impl From<sqlparser::ast::UnaryOperator> for crate::UnaryOperator {
    fn from(value: sqlparser::ast::UnaryOperator) -> Self {
        use sqlparser::ast::UnaryOperator::*;
        match value {
            Plus => todo!(),
            Minus => Self::Neg,
            Not => Self::Not,
            PGBitwiseNot | PGSquareRoot | PGCubeRoot | PGPostfixFactorial | PGPrefixFactorial
            | PGAbs => unimplemented!("unsupported postgres unary operator"),
            BangNot => unimplemented!("unsupported Hive bang not (!)"),
        }
    }
}

/// Convert a function call into an expression.
///
/// It's not always a function call because we turn `DATE(x)` into `CAST(x AS DATE)`
impl From<sqlparser::ast::Function> for crate::Expr {
    fn from(value: sqlparser::ast::Function) -> Self {
        // TODO: handle null treatment and other stuff
        let sqlparser::ast::Function { args, name, .. } = value;
        let (exprs, distinct, separator): (Vec<crate::Expr>, bool, Option<String>) = match args {
            sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                args,
                duplicate_treatment,
                clauses, // TODO: handle other stuff like order/limit, etc.
            }) => (
                args.into_iter().map(Into::into).collect(),
                duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct),
                clauses.into_iter().find_map(|clause| match clause {
                    sqlparser::ast::FunctionArgumentClause::Separator(separator) => {
                        Some(value_into_string(separator))
                    }
                    _ => None,
                }),
            ),
            _ => unimplemented!(),
        };
        // TODO: if there's more than 1 component, it's presumably a UDF or something and we should bail
        if let Some(name) = name.0.last().map(|ident| ident.value.to_lowercase()) {
            match name.as_str() {
                // TODO: fix this unnecessary cloning
                "avg" => Self::Call(crate::FunctionExpr::Avg {
                    expr: Box::new(exprs[0].clone()),
                    distinct,
                }),
                // TODO: check for `count(*)` which we have a separate enum variant for
                "count" => Self::Call(crate::FunctionExpr::Count {
                    expr: Box::new(exprs[0].clone()),
                    distinct,
                }),
                "group_concat" => Self::Call(crate::FunctionExpr::GroupConcat {
                    expr: Box::new(exprs[0].clone()),
                    separator,
                }),
                "max" => Self::Call(crate::FunctionExpr::Max(Box::new(exprs[0].clone()))),
                "min" => Self::Call(crate::FunctionExpr::Min(Box::new(exprs[0].clone()))),
                "sum" => Self::Call(crate::FunctionExpr::Sum {
                    expr: Box::new(exprs[0].clone()),
                    distinct,
                }),
                "date" => Self::Cast {
                    expr: Box::new(exprs[0].clone()),
                    ty: crate::SqlType::Date,
                    postgres_style: false,
                },
                "extract" | "substring" => todo!(),
                _ => Self::Call(crate::FunctionExpr::Call {
                    name: name.into(),
                    arguments: exprs,
                }),
            }
        } else {
            todo!("Turn this into TryFrom")
        }
    }
}

fn value_into_string(value: sqlparser::ast::Value) -> String {
    use sqlparser::ast::Value::*;
    match value {
        Number(s, _)
        | SingleQuotedString(s)
        | DollarQuotedString(sqlparser::ast::DollarQuotedString { value: s, .. })
        | TripleSingleQuotedString(s)
        | TripleDoubleQuotedString(s)
        | EscapedStringLiteral(s)
        | UnicodeStringLiteral(s)
        | SingleQuotedByteStringLiteral(s)
        | DoubleQuotedByteStringLiteral(s)
        | TripleSingleQuotedByteStringLiteral(s)
        | TripleDoubleQuotedByteStringLiteral(s)
        | SingleQuotedRawStringLiteral(s)
        | DoubleQuotedRawStringLiteral(s)
        | TripleSingleQuotedRawStringLiteral(s)
        | TripleDoubleQuotedRawStringLiteral(s)
        | NationalStringLiteral(s)
        | DoubleQuotedString(s) => s,
        HexStringLiteral(_) => todo!(),
        Boolean(_) => todo!(),
        Null => todo!(),
        Placeholder(_) => todo!(),
    }
}

impl From<sqlparser::ast::FunctionArg> for crate::Expr {
    fn from(value: sqlparser::ast::FunctionArg) -> Self {
        use sqlparser::ast::FunctionArg::*;
        match value {
            Named { arg, .. } | ExprNamed { arg, .. } | Unnamed(arg) => arg.into(),
        }
    }
}

impl From<sqlparser::ast::FunctionArgExpr> for crate::Expr {
    fn from(value: sqlparser::ast::FunctionArgExpr) -> Self {
        use sqlparser::ast::FunctionArgExpr::*;
        match value {
            Expr(expr) => expr.into(),
            QualifiedWildcard(object_name) => Self::Column(object_name.into()),
            Wildcard => unimplemented!(),
        }
    }
}

impl From<sqlparser::ast::DateTimeField> for crate::TimestampField {
    fn from(value: sqlparser::ast::DateTimeField) -> Self {
        use sqlparser::ast::DateTimeField;
        match value {
            DateTimeField::Century => Self::Century,
            DateTimeField::Day | DateTimeField::Days => Self::Day,
            DateTimeField::DayOfWeek => Self::Dow,
            DateTimeField::DayOfYear => Self::Doy,
            DateTimeField::Decade => Self::Decade,
            DateTimeField::Dow => Self::Dow,
            DateTimeField::Doy => Self::Doy,
            DateTimeField::Epoch => Self::Epoch,
            DateTimeField::Hour | DateTimeField::Hours => Self::Hour,
            DateTimeField::Isodow => Self::Isodow,
            DateTimeField::Isoyear => Self::Isoyear,
            DateTimeField::Julian => Self::Julian,
            DateTimeField::Microsecond | DateTimeField::Microseconds => Self::Microseconds,
            DateTimeField::Millenium | DateTimeField::Millennium => Self::Millennium,
            DateTimeField::Millisecond | DateTimeField::Milliseconds => Self::Milliseconds,
            DateTimeField::Minute | DateTimeField::Minutes => Self::Minute,
            DateTimeField::Month | DateTimeField::Months => Self::Month,
            DateTimeField::Quarter => Self::Quarter,
            DateTimeField::Second | DateTimeField::Seconds => Self::Second,
            DateTimeField::Timezone => Self::Timezone,
            DateTimeField::TimezoneHour => Self::TimezoneHour,
            DateTimeField::TimezoneMinute => Self::TimezoneMinute,
            DateTimeField::Week(_) | DateTimeField::Weeks => Self::Week, // Optional weekday is only BigQuery
            DateTimeField::Year | DateTimeField::Years => Self::Year,
            DateTimeField::Custom(_)
            | DateTimeField::Date
            | DateTimeField::Datetime
            | DateTimeField::IsoWeek
            | DateTimeField::Nanosecond
            | DateTimeField::Nanoseconds
            | DateTimeField::NoDateTime
            | DateTimeField::Time
            | DateTimeField::TimezoneAbbr
            | DateTimeField::TimezoneRegion => {
                unimplemented!("not supported by MySQL or Postgres")
            }
        }
    }
}

impl From<sqlparser::ast::GroupByExpr> for crate::GroupByClause {
    fn from(value: sqlparser::ast::GroupByExpr) -> Self {
        match value {
            sqlparser::ast::GroupByExpr::Expressions(exprs, _modifiers) => crate::GroupByClause {
                fields: exprs.into_iter().map(Into::into).collect(),
            },
            sqlparser::ast::GroupByExpr::All(_) => {
                unimplemented!("Snowflake/DuckDB/ClickHouse group by syntax {value:?}")
            }
        }
    }
}

impl From<sqlparser::ast::Expr> for crate::FieldReference {
    fn from(value: sqlparser::ast::Expr) -> Self {
        if let sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(ref n, _)) = value {
            if let Ok(i) = n.parse() {
                return crate::FieldReference::Numeric(i);
            }
        }
        crate::FieldReference::Expr(value.into())
    }
}

impl From<sqlparser::ast::OrderBy> for crate::OrderClause {
    fn from(value: sqlparser::ast::OrderBy) -> Self {
        crate::OrderClause {
            order_by: value.exprs.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<sqlparser::ast::OrderByExpr> for crate::OrderBy {
    fn from(value: sqlparser::ast::OrderByExpr) -> Self {
        let sqlparser::ast::OrderByExpr {
            expr,
            asc,
            nulls_first,
            ..
        } = value;
        Self {
            field: expr.into(),
            order_type: asc.map(|asc| {
                if asc {
                    crate::OrderType::OrderAscending
                } else {
                    crate::OrderType::OrderDescending
                }
            }),
            null_order: nulls_first.map(|nulls_first| {
                if nulls_first {
                    crate::order::NullOrder::NullsFirst
                } else {
                    crate::order::NullOrder::NullsLast
                }
            }),
        }
    }
}

impl From<sqlparser::ast::TableFactor> for crate::TableExpr {
    fn from(value: sqlparser::ast::TableFactor) -> Self {
        match value {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => Self {
                inner: crate::TableExprInner::Table(name.into()),
                alias: alias.map(|table_alias| table_alias.name.into()), // XXX we don't support [`TableAlias::columns`]
                index_hint: None, // TODO(mvzink): Find where this is parsed in sqlparser
            },
            sqlparser::ast::TableFactor::Derived {
                subquery,
                alias,
                lateral: _lateral, // XXX We don't support this
            } => {
                if let crate::SqlQuery::Select(subselect) = (*subquery).into() {
                    Self {
                        inner: crate::TableExprInner::Subquery(Box::new(subselect)),
                        alias: alias.map(|table_alias| table_alias.name.into()), // XXX we don't support [`TableAlias::columns`]
                        index_hint: None, // TODO(mvzink): Find where this is parsed in sqlparser
                    }
                } else {
                    panic!("unexpected non-SELECT subquery in table expression")
                }
            }
            _ => unimplemented!("unsupported table expression {value:?}"),
        }
    }
}

impl From<sqlparser::ast::Join> for crate::JoinClause {
    fn from(value: sqlparser::ast::Join) -> Self {
        Self {
            operator: (&value.join_operator).into(),
            constraint: value.join_operator.into(),
            right: crate::JoinRightSide::Table(value.relation.into()),
        }
    }
}

impl From<&sqlparser::ast::JoinOperator> for crate::JoinOperator {
    fn from(value: &sqlparser::ast::JoinOperator) -> Self {
        use sqlparser::ast::JoinOperator::*;
        match value {
            // TODO: fix these
            Inner(..) => Self::InnerJoin,
            LeftOuter(..) => Self::LeftOuterJoin,
            RightOuter(..) => Self::RightJoin, // XXX is this outer?
            CrossJoin => Self::CrossJoin,
            _ => unimplemented!("unsupported join type {value:?}"),
        }
    }
}

impl From<sqlparser::ast::JoinOperator> for crate::JoinConstraint {
    fn from(value: sqlparser::ast::JoinOperator) -> Self {
        use sqlparser::ast::JoinOperator::*;
        match value {
            Inner(constraint)
            | LeftOuter(constraint)
            | RightOuter(constraint)
            | FullOuter(constraint)
            | LeftSemi(constraint)
            | RightSemi(constraint)
            | LeftAnti(constraint)
            | RightAnti(constraint)
            | Semi(constraint)
            | Anti(constraint)
            | AsOf { constraint, .. } => constraint.into(),
            CrossJoin => Self::Empty,
            CrossApply => Self::Empty,
            OuterApply => Self::Empty,
        }
    }
}

impl From<sqlparser::ast::JoinConstraint> for crate::JoinConstraint {
    fn from(value: sqlparser::ast::JoinConstraint) -> Self {
        use sqlparser::ast::JoinConstraint::*;
        match value {
            On(expr) => Self::On(expr.into()),
            Using(idents) => Self::Using(idents.into_iter().map(Into::into).collect()),
            None => Self::Empty,
            Natural => unimplemented!("unsupported NATURAL join"),
        }
    }
}

impl From<sqlparser::ast::Ident> for crate::Column {
    fn from(value: sqlparser::ast::Ident) -> Self {
        Self {
            name: value.into(),
            table: None,
        }
    }
}

impl From<Vec<sqlparser::ast::Ident>> for crate::Column {
    fn from(mut value: Vec<sqlparser::ast::Ident>) -> Self {
        let name: crate::SqlIdentifier = value.pop().unwrap().into();
        let table = if let Some(table) = value.pop() {
            if let Some(schema) = value.pop() {
                Some(crate::Relation {
                    schema: Some(schema.into()),
                    name: table.into(),
                })
            } else {
                Some(crate::Relation {
                    schema: None,
                    name: table.into(),
                })
            }
        } else {
            None
        };
        Self { name, table }
    }
}

impl From<String> for crate::ItemPlaceholder {
    fn from(value: String) -> Self {
        if value == "?" {
            Self::QuestionMark
        } else {
            if let Ok((_, placeholder)) = dollarsign_placeholder(LocatedSpan::new(value.as_bytes()))
            {
                placeholder
            } else {
                unimplemented!("nyi placeholder '{value}'")
            }
        }
    }
}

impl From<sqlparser::ast::ShowStatementFilter> for crate::show::FilterPredicate {
    fn from(value: sqlparser::ast::ShowStatementFilter) -> Self {
        use sqlparser::ast::ShowStatementFilter::*;
        match value {
            Like(like) => Self::Like(like),
            Where(expr) => Self::Where(expr.into()),
            _ => unimplemented!("not supported for MySQL or Postgres"),
        }
    }
}

impl From<sqlparser::ast::Value> for crate::Literal {
    fn from(value: sqlparser::ast::Value) -> Self {
        use sqlparser::ast::Value::*;
        match value {
            Placeholder(name) => crate::Literal::Placeholder(name.into()),
            Boolean(b) => crate::Literal::Boolean(b),
            Null => crate::Literal::Null,
            DoubleQuotedString(s)
            | SingleQuotedString(s)
            | DoubleQuotedByteStringLiteral(s)
            | SingleQuotedByteStringLiteral(s) => crate::Literal::String(s),
            DollarQuotedString(sqlparser::ast::DollarQuotedString { value, .. }) => {
                crate::Literal::String(value)
            }
            Number(s, _unknown) => {
                if let Ok(i) = s.parse::<i64>() {
                    crate::Literal::Integer(i)
                } else if let Ok(i) = s.parse::<u64>() {
                    crate::Literal::UnsignedInteger(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    crate::Literal::Double(crate::Double {
                        value: f,
                        precision: 0,
                    })
                } else if let Ok(d) = Decimal::from_str_exact(&s) {
                    // Seems like this will later get re-parsed the same way
                    crate::Literal::Numeric(d.mantissa(), d.scale())
                } else {
                    panic!("failed to parse number: {s}") // TODO: remember that 99% of this should be converted to TryFrom
                }
            }
            _ => unimplemented!("unsupported literal {value:?}"),
        }
    }
}

impl From<sqlparser::ast::CreateTable> for crate::CreateTableStatement {
    fn from(value: sqlparser::ast::CreateTable) -> Self {
        Self {
            if_not_exists: value.if_not_exists,
            table: value.name.into(),
            body: Ok(crate::CreateTableBody {
                fields: value.columns.into_iter().map(Into::into).collect(),
                keys: if value.constraints.is_empty() {
                    None
                } else {
                    Some(value.constraints.into_iter().map(Into::into).collect())
                },
            }),
            options: Ok(vec![]), // TODO(mvzink): options are individual fields on the sqlparser struct
        }
    }
}

impl From<sqlparser::ast::ColumnDef> for crate::ColumnSpecification {
    fn from(value: sqlparser::ast::ColumnDef) -> Self {
        Self {
            column: value.name.into(),
            sql_type: value.data_type.into(),
            // TODO(mvzink): these are all types of column options
            constraints: vec![],
            comment: None,
            generated: None,
        }
    }
}

impl From<sqlparser::ast::DataType> for crate::SqlType {
    fn from(value: sqlparser::ast::DataType) -> Self {
        use sqlparser::ast::DataType::*;
        match value {
            Int(len) => Self::Int(len.map(|n| n as u16)),
            TinyInt(len) => Self::TinyInt(len.map(|n| n as u16)),
            UnsignedBigInt(len) | UnsignedInt8(len) => Self::UnsignedBigInt(len.map(|n| n as u16)),
            Text => Self::Text,
            TinyText => Self::TinyText,
            MediumText => Self::MediumText,
            LongText => Self::LongText,
            Character(len) | Char(len) => Self::Char(len.and_then(character_length_into_u16)),
            Varchar(len) | Nvarchar(len) | CharVarying(len) | CharacterVarying(len) => {
                Self::VarChar(len.and_then(character_length_into_u16))
            }
            Uuid => Self::Uuid,
            CharacterLargeObject(_) => unimplemented!("check mysql/pg support"),
            CharLargeObject(_) => unimplemented!("check mysql/pg support"),
            Clob(_) => unimplemented!("check mysql/pg support"),
            Binary(n) => Self::Binary(n.map(|n| n as u16)),
            Varbinary(n) => Self::VarBinary(n.map(|n| n as u16).unwrap_or(0)),
            // TOOD: technically mysql inspects the size and converts to tiny/medium/long
            Blob(_) => Self::Blob,
            TinyBlob => Self::TinyBlob,
            MediumBlob => Self::MediumBlob,
            LongBlob => Self::LongBlob,
            Bytes(_) => unimplemented!(),
            Numeric(info) => exact_number_info_into_numeric(info).expect("TryFrom"),
            Decimal(info) => exact_number_info_into_decimal(info).expect("TryFrom"),
            BigNumeric(info) => exact_number_info_into_numeric(info).expect("TryFrom"),
            BigDecimal(info) => exact_number_info_into_decimal(info).expect("TryFrom"),
            Dec(info) => exact_number_info_into_decimal(info).expect("TryFrom"),
            Float(_) => Self::Float,
            UnsignedTinyInt(n) => Self::UnsignedTinyInt(n.map(|n| n as u16)),
            Int2(_) => Self::Int2,
            UnsignedInt2(n) => Self::UnsignedSmallInt(n.map(|n| n as u16)),
            SmallInt(n) => Self::SmallInt(n.map(|n| n as u16)),
            UnsignedSmallInt(n) => Self::UnsignedSmallInt(n.map(|n| n as u16)),
            MediumInt(n) => Self::MediumInt(n.map(|n| n as u16)),
            UnsignedMediumInt(n) => Self::UnsignedMediumInt(n.map(|n| n as u16)),
            Int4(_) => Self::Int4,
            Int8(_) => Self::Int8,
            Int16 | Int32 | Int64 | Int128 | Int256 => unimplemented!(),
            Integer(n) => Self::Int(n.map(|n| n as u16)),
            UnsignedInt(n) => Self::UnsignedInt(n.map(|n| n as u16)),
            UnsignedInt4(n) => Self::UnsignedInt(n.map(|n| n as u16)),
            UnsignedInteger(n) => Self::UnsignedInt(n.map(|n| n as u16)),
            UInt8 | UInt16 | UInt32 | UInt64 | UInt128 | UInt256 => unimplemented!(),
            BigInt(n) => Self::BigInt(n.map(|n| n as u16)),
            Float4 | Float8 | Float32 | Float64 => unimplemented!(),
            Real => Self::Real,
            // XXX we don't support precision on doubles; [MySQL] says it's deprecated, but still present as of 9.1
            // [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/floating-point-types.html
            Double(_info) => Self::Double,
            DoublePrecision => Self::Double,
            Bool => Self::Bool,
            Boolean => Self::Bool,
            Date => Self::Date,
            Date32 => unimplemented!(),
            // TODO: Should we support these options?
            Time(_, _timezone_info) => Self::Time,
            Datetime(n) => Self::DateTime(n.map(|n| n as u16)),
            Datetime64(_, _) => unimplemented!(),
            Timestamp(_, _timezone_info) => Self::Timestamp,
            Interval => Self::Time,
            JSON => Self::Json,
            JSONB => Self::Jsonb,
            Regclass => unimplemented!(),
            String(_) => unimplemented!(),
            FixedString(_) => unimplemented!(),
            Bytea => Self::ByteArray,
            Bit(n) => Self::Bit(n.map(|n| n as u16)),
            BitVarying(n) => Self::VarBit(n.map(|n| n as u16)),
            c @ Custom(..) => unimplemented!("custom type {c:?}"),
            Array(def) => Self::Array(Box::new(def.into())),
            Map(_data_type, _data_type1) => unimplemented!(),
            Tuple(_vec) => unimplemented!(),
            Nested(_vec) => unimplemented!(),
            // XXX bits is a Clickhouse extension for ENUM8/ENUM16
            Enum(variants, _bits) => Self::Enum(
                variants
                    .into_iter()
                    .map(|variant| match variant {
                        sqlparser::ast::EnumMember::Name(s) => s,
                        // XXX expression is a Clickhouse extension
                        sqlparser::ast::EnumMember::NamedValue(s, _expr) => s,
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            Set(_vec) => unimplemented!(),
            Struct(_vec, _struct_bracket_kindd) => unimplemented!(),
            Union(_vec) => unimplemented!(),
            Nullable(_data_type) => unimplemented!(),
            LowCardinality(_data_type) => unimplemented!(),
            Unspecified => unimplemented!(),
            Trigger => unimplemented!(),
            AnyType => unimplemented!(),
        }
    }
}

fn exact_number_info_into_numeric(
    info: sqlparser::ast::ExactNumberInfo,
) -> Result<crate::SqlType, std::num::TryFromIntError> {
    match info {
        sqlparser::ast::ExactNumberInfo::None => Ok(crate::SqlType::Numeric(None)),
        sqlparser::ast::ExactNumberInfo::Precision(precision) => {
            Ok(crate::SqlType::Numeric(Some((precision.try_into()?, None))))
        }
        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => Ok(
            crate::SqlType::Numeric(Some((precision.try_into()?, Some(scale.try_into()?)))),
        ),
    }
}

fn exact_number_info_into_decimal(
    info: sqlparser::ast::ExactNumberInfo,
) -> Result<crate::SqlType, std::num::TryFromIntError> {
    match info {
        sqlparser::ast::ExactNumberInfo::None => Ok(crate::SqlType::Decimal(28, 0)),
        sqlparser::ast::ExactNumberInfo::Precision(precision) => {
            Ok(crate::SqlType::Decimal(precision.try_into()?, 0))
        }
        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => Ok(
            crate::SqlType::Decimal(precision.try_into()?, scale.try_into()?),
        ),
    }
}

fn character_length_into_u16(value: sqlparser::ast::CharacterLength) -> Option<u16> {
    match value {
        sqlparser::ast::CharacterLength::IntegerLength { length, unit: _ } => {
            length.try_into().ok()
        }
        sqlparser::ast::CharacterLength::Max => None,
    }
}

impl From<sqlparser::ast::ArrayElemTypeDef> for crate::SqlType {
    fn from(value: sqlparser::ast::ArrayElemTypeDef) -> Self {
        match value {
            sqlparser::ast::ArrayElemTypeDef::None => unimplemented!(),
            sqlparser::ast::ArrayElemTypeDef::AngleBracket(data_type) => (*data_type).into(),
            // TODO: Should we explicitly reject numbers in the square brackets?
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(data_type, _) => (*data_type).into(),
            sqlparser::ast::ArrayElemTypeDef::Parenthesis(data_type) => (*data_type).into(),
        }
    }
}

impl From<sqlparser::ast::TableConstraint> for crate::TableKey {
    fn from(value: sqlparser::ast::TableConstraint) -> Self {
        use sqlparser::ast::TableConstraint::*;
        match value {
            Check { name, expr } => Self::CheckConstraint {
                constraint_name: name.map(Into::into),
                expr: (*expr).into(),
                enforced: None, // TODO(mvzink): Find out where this is supposed to come from
            },
            ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                // XXX Not sure why, but we don't support characteristics
                characteristics: _characteristics,
            } => Self::ForeignKey {
                // TODO(mvzink): Where do these two different names come from for sqlparser?
                constraint_name: name.clone().map(Into::into),
                index_name: name.map(Into::into),
                columns: columns.into_iter().map(Into::into).collect(),
                target_table: foreign_table.into(),
                target_columns: referred_columns.into_iter().map(Into::into).collect(),
                on_delete: on_delete.map(Into::into),
                on_update: on_update.map(Into::into),
            },
            FulltextOrSpatial {
                fulltext: true,
                opt_index_name,
                columns,
                index_type_display: _index_type_display,
            } => Self::FulltextKey {
                index_name: opt_index_name.map(Into::into),
                columns: columns.into_iter().map(Into::into).collect(),
            },
            FulltextOrSpatial {
                fulltext: false, ..
            } => unimplemented!("No support for spatial columns, should raise a plain error"),
            Index {
                name,
                index_type,
                columns,
                display_as_key: _display_as_key,
            } => Self::Key {
                // TODO(mvzink): Where do these two different names come from for sqlparser?
                constraint_name: name.clone().map(Into::into),
                index_name: name.map(Into::into),
                columns: columns.into_iter().map(Into::into).collect(),
                index_type: index_type.map(Into::into),
            },
            PrimaryKey {
                name,
                index_name,
                columns,
                characteristics,
                // XXX Not sure why, but we don't support any of these
                index_type: _index_type,
                index_options: _index_options,
            } => Self::PrimaryKey {
                constraint_name: name.map(Into::into),
                index_name: index_name.map(Into::into),
                columns: columns.into_iter().map(Into::into).collect(),
                constraint_timing: characteristics.map(Into::into),
            },
            Unique {
                name,
                index_name,
                index_type,
                columns,
                nulls_distinct,
                characteristics,
                // XXX Not sure why, but we don't support any of these
                index_type_display: _index_type_display,
                index_options: _index_options,
            } => Self::UniqueKey {
                constraint_name: name.map(Into::into),
                index_name: index_name.map(Into::into),
                columns: columns.into_iter().map(Into::into).collect(),
                index_type: index_type.map(Into::into),
                constraint_timing: characteristics.map(Into::into),
                nulls_distinct: match nulls_distinct {
                    sqlparser::ast::NullsDistinctOption::Distinct => {
                        Some(crate::common::NullsDistinct::Distinct)
                    }
                    sqlparser::ast::NullsDistinctOption::NotDistinct => {
                        Some(crate::common::NullsDistinct::NotDistinct)
                    }
                    sqlparser::ast::NullsDistinctOption::None => None,
                },
            },
        }
    }
}

impl From<sqlparser::ast::ReferentialAction> for crate::common::ReferentialAction {
    fn from(value: sqlparser::ast::ReferentialAction) -> Self {
        use sqlparser::ast::ReferentialAction::*;
        match value {
            Cascade => Self::Cascade,
            NoAction => Self::NoAction,
            Restrict => Self::Restrict,
            SetDefault => Self::SetDefault,
            SetNull => Self::SetNull,
        }
    }
}

// TODO: Check that we are correctly representing this matrix, i.e. that deferrable = false +
// initially = deferred is correctly represented by `Self::NotDeferrable` and we don't need a
// separate variant for `Self::NotDeferrableInitiallyDeferred` (which doesn't sound like a valid
// combination)
impl From<sqlparser::ast::ConstraintCharacteristics> for crate::common::ConstraintTiming {
    fn from(value: sqlparser::ast::ConstraintCharacteristics) -> Self {
        use sqlparser::ast::DeferrableInitial::*;
        match (value.deferrable, value.initially) {
            (None, None) => Self::NotDeferrable,
            (None, Some(Immediate)) => Self::NotDeferrableInitiallyImmediate,
            (None, Some(Deferred)) => Self::NotDeferrable,
            (Some(true), None) => Self::Deferrable,
            (Some(false), None) => Self::NotDeferrable,
            (Some(true), Some(Immediate)) => Self::DeferrableInitiallyImmediate,
            (Some(true), Some(Deferred)) => Self::DeferrableInitiallyDeferred,
            (Some(false), Some(Immediate)) => Self::NotDeferrableInitiallyImmediate,
            (Some(false), Some(Deferred)) => Self::NotDeferrable,
        }
    }
}

impl From<sqlparser::ast::IndexType> for crate::IndexType {
    fn from(value: sqlparser::ast::IndexType) -> Self {
        match value {
            sqlparser::ast::IndexType::BTree => Self::BTree,
            sqlparser::ast::IndexType::Hash => Self::Hash,
        }
    }
}

impl From<sqlparser::ast::Insert> for crate::InsertStatement {
    fn from(value: sqlparser::ast::Insert) -> Self {
        Self {
            table: value.table_name.into(),
            fields: if value.columns.is_empty() {
                None
            } else {
                Some(value.columns.into_iter().map(Into::into).collect())
            },
            data: if let Some(query) = value.source {
                match *query.body {
                    sqlparser::ast::SetExpr::Values(values) => values
                        .rows
                        .into_iter()
                        .map(|row| row.into_iter().map(Into::into).collect())
                        .collect(),
                    _ => unimplemented!(), // Our AST currently doesn't support anything else
                }
            } else {
                Vec::new()
            },
            ignore: value.ignore,
            on_duplicate: match value.on {
                Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) => {
                    Some(assignments.into_iter().map(assignment_into_expr).collect())
                }
                _ => None, // TODO: We could support Postgres' ON CONFLICT too
            },
        }
    }
}

fn assignment_into_expr(assignment: sqlparser::ast::Assignment) -> (crate::Column, crate::Expr) {
    (
        match assignment.target {
            sqlparser::ast::AssignmentTarget::ColumnName(object_name) => object_name.into(),
            sqlparser::ast::AssignmentTarget::Tuple(_vec) => {
                todo!("Currently don't support tuple assignment: (a,b) = (1,2)")
            }
        },
        assignment.value.into(),
    )
}

impl From<sqlparser::ast::ObjectName> for crate::Column {
    fn from(value: sqlparser::ast::ObjectName) -> Self {
        value.0.into()
    }
}

impl From<sqlparser::ast::Delete> for crate::DeleteStatement {
    fn from(value: sqlparser::ast::Delete) -> Self {
        Self {
            // TODO: Support multiple tables (in `value.tables`, or possibly in `FromTable`)
            table: value.from.into(),
            where_clause: value.selection.map(Into::into),
            // TODO: Support order_by and limit
        }
    }
}

impl From<sqlparser::ast::FromTable> for crate::Relation {
    fn from(value: sqlparser::ast::FromTable) -> Self {
        use sqlparser::ast::FromTable::*;
        match value {
            WithFromKeyword(tables) | WithoutKeyword(tables) => tables
                .into_iter()
                .map(Into::into)
                .next()
                .expect("empty list of tables"),
        }
    }
}

impl From<sqlparser::ast::TableWithJoins> for crate::Relation {
    fn from(value: sqlparser::ast::TableWithJoins) -> Self {
        match value.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name.into(),
            _ => todo!("We don't support joins yet"),
        }
    }
}

impl From<sqlparser::ast::Statement> for crate::CreateViewStatement {
    fn from(value: sqlparser::ast::Statement) -> Self {
        if let sqlparser::ast::Statement::CreateView {
            or_replace,
            name,
            columns,
            query,
            ..
        } = value
        {
            Self {
                name: name.into(),
                or_replace,
                // TODO: do we really not need to handle datatypes or anything, just plain Columns?
                fields: columns.into_iter().map(Into::into).collect(),
                // TODO: handle compound selects, not sure how sqlparser would represent that
                definition: Ok(Box::new(crate::SelectSpecification::Simple(
                    (*query).into(),
                ))),
            }
        } else {
            unreachable!()
        }
    }
}

impl From<sqlparser::ast::ViewColumnDef> for crate::Column {
    fn from(value: sqlparser::ast::ViewColumnDef) -> Self {
        Self {
            name: value.name.into(),
            table: None,
        }
    }
}

/// First, attempt to parse certain readyset extensions; then, parse using sqlparser-rs. The common case is presumably
/// "everything except readyset", so this should be reversed.
pub fn sql_query_sqlparser(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], crate::SqlQuery> {
    move |i| {
        if let Ok((i, q)) = alt((
            map(drop_cached_query(dialect), crate::SqlQuery::DropCache),
            map(drop_all_caches, crate::SqlQuery::DropAllCaches),
            map(
                drop_all_proxied_queries(),
                crate::SqlQuery::DropAllProxiedQueries,
            ),
            map(explain_statement(dialect), crate::SqlQuery::Explain),
            map(create_cached_query(dialect), crate::SqlQuery::CreateCache),
        ))(i)
        {
            Ok((i, q))
        } else {
            let sqlparser_dialect: &dyn sqlparser::dialect::Dialect = match dialect {
                Dialect::MySQL => &sqlparser::dialect::MySqlDialect {},
                Dialect::PostgreSQL => &sqlparser::dialect::PostgreSqlDialect {},
            };
            // TODO: add any modicum of actual error handling
            let mut parser = sqlparser::parser::Parser::new(sqlparser_dialect)
                .try_with_sql(str::from_utf8(i.as_bytes()).map_err(|_| {
                    nom::Err::Error(NomSqlError {
                        input: i,
                        kind: ErrorKind::Fail,
                    })
                })?)
                .map_err(|e| {
                    nom::Err::Error(NomSqlError {
                        input: i,
                        kind: ErrorKind::Fail,
                    })
                })?;
            let statement = parser.parse_statement().map_err(|e| {
                nom::Err::Error(NomSqlError {
                    input: i,
                    kind: ErrorKind::Fail,
                })
            })?;
            // TODO(mvzink): Check with parser for remainder; for now, ignore it
            let i = i.slice(i.len()..);
            str::from_utf8(i.as_bytes()).expect("invalid remainder");
            Ok((i, statement.into()))
        }
    }
}
