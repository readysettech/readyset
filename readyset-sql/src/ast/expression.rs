use std::{fmt, iter, mem};

use concrete_iter::concrete_iter;
use derive_more::derive::From;
use itertools::Itertools;
use proptest::{
    prelude::{Arbitrary, BoxedStrategy, Just, Strategy as _},
    prop_oneof,
};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;
use test_strategy::Arbitrary;

use crate::ast::*;
use crate::{
    AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect, TryIntoDialect,
};

/// Function call expressions
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Arbitrary,
    IntoStaticStr,
)]
pub enum FunctionExpr {
    /// `ARRAY_AGG` aggregation - PostgreSQL array aggregation function
    ///
    /// Syntax: `ARRAY_AGG(expr [ORDER BY sort_expr [ASC|DESC], ...])`
    ArrayAgg {
        expr: Box<Expr>,
        distinct: DistinctOption,
        order_by: Option<OrderClause>,
    },

    /// `AVG` aggregation. The boolean argument is `true` if `DISTINCT`
    Avg {
        expr: Box<Expr>,
        distinct: bool,
    },

    /// `COUNT` aggregation
    Count {
        expr: Box<Expr>,
        distinct: bool,
    },

    /// `COUNT(*)` aggregation
    CountStar,

    Extract {
        field: TimestampField,
        expr: Box<Expr>,
    },

    /// The SQL `LOWER`/`UPPER` functions.
    ///
    /// The supported syntax for MySQL dialect is:
    ///
    /// `LOWER(string)`
    /// `UPPER(string)`
    /// Note, `collation` will always be None for MySQL dialect
    ///
    /// The supported syntax for Postgres dialect is:
    ///
    /// `LOWER(string [COLLATE collation_name])`
    /// `UPPER(string [COLLATE collation_name])`
    Lower {
        expr: Box<Expr>,
        collation: Option<CollationName>,
    },
    Upper {
        expr: Box<Expr>,
        collation: Option<CollationName>,
    },

    /// `SUM` aggregation
    Sum {
        expr: Box<Expr>,
        distinct: bool,
    },

    /// `MAX` aggregation
    Max(Box<Expr>),

    /// `MIN` aggregation
    Min(Box<Expr>),

    /// `GROUP_CONCAT` aggregation
    GroupConcat {
        expr: Box<Expr>,
        separator: Option<String>,
        distinct: DistinctOption,
        order_by: Option<OrderClause>,
    },

    /// The standard SQL Window Functions
    /// https://www.postgresql.org/docs/17/tutorial-window.html
    /// https://dev.mysql.com/doc/refman/8.4/en/window-function-descriptions.html
    /// Note that some of the standard aggregate functions can also
    /// be used as window functions such as `AVG`, `COUNT`, `MAX`, `MIN`, and `SUM`
    ///
    /// We currently don't support the other window functions
    RowNumber,
    Rank,
    DenseRank,

    /// `STRING_AGG` aggregation
    StringAgg {
        expr: Box<Expr>,
        separator: Option<String>,
        distinct: DistinctOption,
        order_by: Option<OrderClause>,
    },

    /// A Readyset-specific function that places datetime/timestamps into buckets
    /// based on the interval.
    /// Not supported by upstream DBs.
    /// Mainly used in GROUP BY clauses to provide timeseries-like functionality
    Bucket {
        expr: Box<Expr>,
        interval: Box<Expr>,
    },

    /// The SQL `SUBSTRING`/`SUBSTR` function.
    ///
    /// The supported syntax is one of:
    ///
    /// `SUBSTR[ING](string FROM pos FOR len)`
    /// `SUBSTR[ING](string FROM pos)`
    /// `SUBSTR[ING](string FOR len)`
    /// `SUBSTR[ING](string, pos)`
    /// `SUBSTR[ING](string, pos, len)`
    Substring {
        string: Box<Expr>,
        pos: Option<Box<Expr>>,
        len: Option<Box<Expr>>,
    },

    JsonObjectAgg {
        key: Box<Expr>,
        value: Box<Expr>,
        allow_duplicate_keys: bool,
    },

    /// `convert_tz(expr, from_tz, to_tz)`
    ConvertTz(Box<Expr>, Box<Expr>, Box<Expr>),
    /// `dayofweek(expr)`
    DayOfWeek(Box<Expr>),
    /// `month(expr)`
    Month(Box<Expr>),
    /// `timediff(left, right)`
    Timediff(Box<Expr>, Box<Expr>),
    /// `addtime(base, delta)`
    Addtime(Box<Expr>, Box<Expr>),
    /// `date_format(date, format)`
    DateFormat(Box<Expr>, Box<Expr>),
    /// `date_trunc(field, source)`
    DateTrunc(Box<Expr>, Box<Expr>),

    /// `ifnull(expr, val)`
    IfNull(Box<Expr>, Box<Expr>),
    /// `coalesce(expr, ...)`
    Coalesce(Vec<Expr>),

    /// `round(expr [, prec])`
    Round(Box<Expr>, Option<Box<Expr>>),
    /// `greatest(expr, ...)`
    Greatest(Vec<Expr>),
    /// `least(expr, ...)`
    Least(Vec<Expr>),

    /// `concat(...)`, variadic
    Concat(Vec<Expr>),
    /// `concat_ws(separator, ...)`, first arg is separator
    ConcatWs(Vec<Expr>),
    /// `split_part(string, delim, field)`
    SplitPart(Box<Expr>, Box<Expr>, Box<Expr>),
    /// `length(expr)`
    Length(Box<Expr>),
    /// `octet_length(expr)`
    OctetLength(Box<Expr>),
    /// `char_length(expr)` / `character_length(expr)`
    CharLength(Box<Expr>),
    /// `ascii(expr)`
    Ascii(Box<Expr>),
    /// `hex(expr)`
    Hex(Box<Expr>),

    /// `json_depth(expr)`
    JsonDepth(Box<Expr>),
    /// `json_valid(expr)`
    JsonValid(Box<Expr>),
    /// `json_overlaps(a, b)`
    JsonOverlaps(Box<Expr>, Box<Expr>),
    /// `json_quote(expr)`
    JsonQuote(Box<Expr>),
    /// `json_typeof(expr)` / `jsonb_typeof(expr)` (consolidated)
    JsonTypeof(Box<Expr>),
    /// `json_array_length(expr)` / `jsonb_array_length(expr)` (consolidated)
    JsonArrayLength(Box<Expr>),
    /// `json_extract_path_text(json, keys...)` / `jsonb_extract_path_text(json, keys...)` (consolidated)
    JsonExtractPathText(Box<Expr>, Vec<Expr>),

    /// `json_object(...)` (PG behavior)
    JsonObject(Vec<Expr>),
    /// `jsonb_object(...)` (returns Jsonb)
    JsonbObject(Vec<Expr>),
    /// `json_build_object(...)` (allow_duplicate_keys=true)
    JsonBuildObject(Vec<Expr>),
    /// `jsonb_build_object(...)` (allow_duplicate_keys=false)
    JsonbBuildObject(Vec<Expr>),
    /// `json_strip_nulls(expr)` (returns Json)
    JsonStripNulls(Box<Expr>),
    /// `jsonb_strip_nulls(expr)` (returns Jsonb)
    JsonbStripNulls(Box<Expr>),
    /// `json_extract_path(json, keys...)` (returns Json)
    JsonExtractPath(Box<Expr>, Vec<Expr>),
    /// `jsonb_extract_path(json, keys...)` (returns Jsonb)
    JsonbExtractPath(Box<Expr>, Vec<Expr>),
    /// `jsonb_insert(target, path, new_value [, insert_after])`
    JsonbInsert(Box<Expr>, Box<Expr>, Box<Expr>, Option<Box<Expr>>),
    /// `jsonb_set(target, path, new_value [, create_if_missing])`
    JsonbSet(Box<Expr>, Box<Expr>, Box<Expr>, Option<Box<Expr>>),
    /// `jsonb_set_lax(target, path, new_value [, create_if_missing [, null_value_treatment]])`
    JsonbSetLax(
        Box<Expr>,
        Box<Expr>,
        Box<Expr>,
        Option<Box<Expr>>,
        Option<Box<Expr>>,
    ),
    /// `jsonb_pretty(expr)`
    JsonbPretty(Box<Expr>),

    /// `array_to_string(array, delim [, null_string])`
    ArrayToString(Box<Expr>, Box<Expr>, Option<Box<Expr>>),

    /// `st_astext(expr)`
    StAsText(Box<Expr>),
    /// `st_aswkt(expr)`
    StAsWkt(Box<Expr>),
    /// `st_asewkt(expr)`
    StAsEwkt(Box<Expr>),

    /// `current_date`
    CurrentDate,
    /// `current_timestamp[(precision)]`
    CurrentTimestamp(Option<Box<Expr>>),
    /// `current_time`
    CurrentTime,
    /// `localtimestamp`
    LocalTimestamp,
    /// `localtime`
    LocalTime,
    /// `current_user`
    CurrentUser,
    /// `session_user`
    SessionUser,
    /// `current_catalog`
    CurrentCatalog,
    /// SQL `USER` (name avoids Rust keyword conflicts)
    SqlUser,

    /// User-defined function call, optionally schema-qualified
    ///
    /// This variant is used when we parse a function call that could be a UDF
    /// (i.e., schema-qualified calls like `myschema.func()`).
    Udf {
        /// Optional schema qualification (e.g., `myschema.func()`)
        schema: Option<SqlIdentifier>,
        /// Function name
        name: SqlIdentifier,
        /// Arguments to the function (always requires parentheses, unlike `Call`)
        arguments: Vec<Expr>,
    },
}

macro_rules! order_by_clause_str {
    ($o:expr, $dialect:expr) => {
        $o.as_ref()
            .map(|clause| format!(" {}", clause.display($dialect).to_string()))
            .unwrap_or_else(String::new)
    };
}

impl FunctionExpr {
    pub fn alias(&self, dialect: Dialect) -> Option<String> {
        Some(match self {
            FunctionExpr::ArrayAgg {
                expr,
                distinct,
                order_by,
            } => {
                format!(
                    "array_agg({}{}{})",
                    distinct,
                    expr.alias(dialect)?,
                    order_by_clause_str!(order_by, dialect),
                )
            }
            FunctionExpr::Avg { expr, distinct } => format!(
                "avg({}{})",
                if *distinct { "DISTINCT " } else { "" },
                expr.alias(dialect)?
            ),
            FunctionExpr::Count { expr, distinct } => format!(
                "count({}{})",
                if *distinct { "DISTINCT " } else { "" },
                expr.alias(dialect)?
            ),
            FunctionExpr::Sum { expr, distinct } => format!(
                "sum({}{})",
                if *distinct { "DISTINCT " } else { "" },
                expr.alias(dialect)?
            ),
            FunctionExpr::Max(col) => format!("max({})", col.alias(dialect)?),
            FunctionExpr::Min(col) => format!("min({})", col.alias(dialect)?),
            FunctionExpr::Extract { field, expr } => {
                format!("extract({field} from {})", expr.alias(dialect)?)
            }
            FunctionExpr::Lower { expr, collation } => format!(
                "lower({}{})",
                expr.alias(dialect)?,
                if let Some(c) = collation {
                    format!(" COLLATE {c}")
                } else {
                    "".to_string()
                }
            ),
            FunctionExpr::Upper { expr, collation } => format!(
                "upper({}{})",
                expr.alias(dialect)?,
                if let Some(c) = collation {
                    format!(" COLLATE {c}")
                } else {
                    "".to_string()
                }
            ),
            FunctionExpr::GroupConcat {
                expr,
                separator,
                distinct,
                order_by,
            } => format!(
                "group_concat({}{}{} {})",
                distinct,
                expr.alias(dialect)?,
                order_by_clause_str!(order_by, dialect),
                separator
                    .as_ref()
                    .map(|s| format!("'{}'", s.replace('\'', "''").replace('\\', "\\\\")))
                    .unwrap_or_default(),
            ),
            FunctionExpr::StringAgg {
                expr,
                separator,
                distinct,
                order_by,
            } => format!(
                "string_agg({}{}, {}{})",
                distinct,
                expr.alias(dialect)?,
                separator
                    .as_ref()
                    .map(|s| format!("'{}'", s.replace('\'', "''").replace('\\', "\\\\")))
                    .unwrap_or("NULL".to_string()),
                order_by_clause_str!(order_by, dialect),
            ),
            FunctionExpr::Substring { string, pos, len } => format!(
                "substring({}, {}, {})",
                string.alias(dialect)?,
                pos.as_ref()
                    .map(|pos| pos.alias(dialect).unwrap_or_default())
                    .unwrap_or_default(),
                len.as_ref()
                    .map(|len| len.alias(dialect).unwrap_or_default())
                    .unwrap_or_default(),
            ),
            FunctionExpr::JsonObjectAgg {
                key,
                value,
                allow_duplicate_keys,
            } => {
                let fname = match dialect {
                    Dialect::MySQL => "json_objectagg",
                    Dialect::PostgreSQL => {
                        if *allow_duplicate_keys {
                            "json_object_agg"
                        } else {
                            "jsonb_object_agg"
                        }
                    }
                };

                format!(
                    "{}({}, {})",
                    fname,
                    key.alias(dialect)?,
                    value.alias(dialect)?
                )
            }
            FunctionExpr::CountStar => "count(*)".to_string(),
            // No-paren functions
            FunctionExpr::CurrentDate => "current_date".to_string(),
            FunctionExpr::CurrentTimestamp(_) => "current_timestamp".to_string(),
            FunctionExpr::CurrentTime => "current_time".to_string(),
            FunctionExpr::LocalTimestamp => "localtimestamp".to_string(),
            FunctionExpr::LocalTime => "localtime".to_string(),
            FunctionExpr::CurrentUser => "current_user".to_string(),
            FunctionExpr::SessionUser => "session_user".to_string(),
            FunctionExpr::CurrentCatalog => "current_catalog".to_string(),
            FunctionExpr::SqlUser => "user".to_string(),
            // Typed built-in function variants: each produces its own format string.
            FunctionExpr::ConvertTz(a, b, c) => format!(
                "convert_tz({}, {}, {})",
                a.alias(dialect)?,
                b.alias(dialect)?,
                c.alias(dialect)?
            ),
            FunctionExpr::DayOfWeek(e) => format!("dayofweek({})", e.alias(dialect)?),
            FunctionExpr::Month(e) => format!("month({})", e.alias(dialect)?),
            FunctionExpr::Timediff(a, b) => {
                format!("timediff({}, {})", a.alias(dialect)?, b.alias(dialect)?)
            }
            FunctionExpr::Addtime(a, b) => {
                format!("addtime({}, {})", a.alias(dialect)?, b.alias(dialect)?)
            }
            FunctionExpr::DateFormat(a, b) => {
                format!("date_format({}, {})", a.alias(dialect)?, b.alias(dialect)?)
            }
            FunctionExpr::DateTrunc(a, b) => {
                format!("date_trunc({}, {})", a.alias(dialect)?, b.alias(dialect)?)
            }
            FunctionExpr::IfNull(a, b) => {
                format!("ifnull({}, {})", a.alias(dialect)?, b.alias(dialect)?)
            }
            FunctionExpr::Coalesce(args) => format!(
                "coalesce({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::Round(e, prec) => {
                if let Some(p) = prec {
                    format!("round({}, {})", e.alias(dialect)?, p.alias(dialect)?)
                } else {
                    format!("round({})", e.alias(dialect)?)
                }
            }
            FunctionExpr::Greatest(args) => format!(
                "greatest({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::Least(args) => format!(
                "least({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::Concat(args) => format!(
                "concat({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::ConcatWs(args) => format!(
                "concat_ws({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::SplitPart(a, b, c) => format!(
                "split_part({}, {}, {})",
                a.alias(dialect)?,
                b.alias(dialect)?,
                c.alias(dialect)?
            ),
            FunctionExpr::Length(e) => format!("length({})", e.alias(dialect)?),
            FunctionExpr::OctetLength(e) => format!("octet_length({})", e.alias(dialect)?),
            FunctionExpr::CharLength(e) => format!("char_length({})", e.alias(dialect)?),
            FunctionExpr::Ascii(e) => format!("ascii({})", e.alias(dialect)?),
            FunctionExpr::Hex(e) => format!("hex({})", e.alias(dialect)?),
            FunctionExpr::JsonDepth(e) => format!("json_depth({})", e.alias(dialect)?),
            FunctionExpr::JsonValid(e) => format!("json_valid({})", e.alias(dialect)?),
            FunctionExpr::JsonOverlaps(a, b) => {
                format!(
                    "json_overlaps({}, {})",
                    a.alias(dialect)?,
                    b.alias(dialect)?
                )
            }
            FunctionExpr::JsonQuote(e) => format!("json_quote({})", e.alias(dialect)?),
            FunctionExpr::JsonTypeof(e) => format!("json_typeof({})", e.alias(dialect)?),
            FunctionExpr::JsonArrayLength(e) => {
                format!("json_array_length({})", e.alias(dialect)?)
            }
            FunctionExpr::JsonExtractPathText(json, keys) => format!(
                "json_extract_path_text({}, {})",
                json.alias(dialect)?,
                keys.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::JsonObject(args) | FunctionExpr::JsonbObject(args) => {
                let name = if matches!(self, FunctionExpr::JsonObject(_)) {
                    "json_object"
                } else {
                    "jsonb_object"
                };
                format!(
                    "{}({})",
                    name,
                    args.iter()
                        .map(|a| a.alias(dialect))
                        .collect::<Option<Vec<_>>>()?
                        .join(", ")
                )
            }
            FunctionExpr::JsonBuildObject(args) => format!(
                "json_build_object({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::JsonbBuildObject(args) => format!(
                "jsonb_build_object({})",
                args.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::JsonStripNulls(e) => format!("json_strip_nulls({})", e.alias(dialect)?),
            FunctionExpr::JsonbStripNulls(e) => {
                format!("jsonb_strip_nulls({})", e.alias(dialect)?)
            }
            FunctionExpr::JsonExtractPath(json, keys) => format!(
                "json_extract_path({}, {})",
                json.alias(dialect)?,
                keys.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::JsonbExtractPath(json, keys) => format!(
                "jsonb_extract_path({}, {})",
                json.alias(dialect)?,
                keys.iter()
                    .map(|a| a.alias(dialect))
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            FunctionExpr::JsonbInsert(a, b, c, d) => {
                let mut s = format!(
                    "jsonb_insert({}, {}, {}",
                    a.alias(dialect)?,
                    b.alias(dialect)?,
                    c.alias(dialect)?
                );
                if let Some(d) = d {
                    s.push_str(&format!(", {}", d.alias(dialect)?));
                }
                s.push(')');
                s
            }
            FunctionExpr::JsonbSet(a, b, c, d) => {
                let mut s = format!(
                    "jsonb_set({}, {}, {}",
                    a.alias(dialect)?,
                    b.alias(dialect)?,
                    c.alias(dialect)?
                );
                if let Some(d) = d {
                    s.push_str(&format!(", {}", d.alias(dialect)?));
                }
                s.push(')');
                s
            }
            FunctionExpr::JsonbSetLax(a, b, c, d, e) => {
                let mut s = format!(
                    "jsonb_set_lax({}, {}, {}",
                    a.alias(dialect)?,
                    b.alias(dialect)?,
                    c.alias(dialect)?
                );
                if let Some(d) = d {
                    s.push_str(&format!(", {}", d.alias(dialect)?));
                }
                if let Some(e) = e {
                    s.push_str(&format!(", {}", e.alias(dialect)?));
                }
                s.push(')');
                s
            }
            FunctionExpr::JsonbPretty(e) => format!("jsonb_pretty({})", e.alias(dialect)?),
            FunctionExpr::ArrayToString(a, b, c) => {
                let mut s = format!(
                    "array_to_string({}, {}",
                    a.alias(dialect)?,
                    b.alias(dialect)?
                );
                if let Some(c) = c {
                    s.push_str(&format!(", {}", c.alias(dialect)?));
                }
                s.push(')');
                s
            }
            FunctionExpr::StAsText(e) => format!("st_astext({})", e.alias(dialect)?),
            FunctionExpr::StAsWkt(e) => format!("st_aswkt({})", e.alias(dialect)?),
            FunctionExpr::StAsEwkt(e) => format!("st_asewkt({})", e.alias(dialect)?),
            FunctionExpr::Udf {
                schema,
                name,
                arguments,
            } => {
                let qualified_name = if let Some(s) = schema {
                    format!(
                        "{}.{}",
                        dialect.quote_identifier(s),
                        dialect.quote_identifier(name)
                    )
                } else {
                    dialect.quote_identifier(name).to_string()
                };
                format!(
                    "{}({})",
                    qualified_name,
                    arguments
                        .iter()
                        .map(|arg| arg.alias(dialect))
                        .collect::<Option<Vec<_>>>()?
                        .join(", ")
                )
            }
            FunctionExpr::RowNumber => "row_number()".to_string(),
            FunctionExpr::Rank => "rank()".to_string(),
            FunctionExpr::DenseRank => "dense_rank()".to_string(),
            FunctionExpr::Bucket { expr, interval } => format!(
                "bucket({}, {})",
                expr.alias(dialect)?,
                interval.alias(dialect)?,
            ),
        })
    }
}

impl FunctionExpr {
    /// Returns an iterator over all the direct arguments passed to the given function call
    /// expression
    #[concrete_iter]
    pub fn arguments<'a>(&'a self) -> impl Iterator<Item = &'a Expr> {
        match self {
            FunctionExpr::ArrayAgg { expr: arg, .. }
            | FunctionExpr::Avg { expr: arg, .. }
            | FunctionExpr::Count { expr: arg, .. }
            | FunctionExpr::Sum { expr: arg, .. }
            | FunctionExpr::Max(arg)
            | FunctionExpr::Min(arg)
            | FunctionExpr::GroupConcat { expr: arg, .. }
            | FunctionExpr::StringAgg { expr: arg, .. }
            | FunctionExpr::Extract { expr: arg, .. }
            | FunctionExpr::Bucket { expr: arg, .. }
            | FunctionExpr::Lower { expr: arg, .. }
            | FunctionExpr::Upper { expr: arg, .. }
            | FunctionExpr::DayOfWeek(arg)
            | FunctionExpr::Month(arg)
            | FunctionExpr::Length(arg)
            | FunctionExpr::OctetLength(arg)
            | FunctionExpr::CharLength(arg)
            | FunctionExpr::Ascii(arg)
            | FunctionExpr::Hex(arg)
            | FunctionExpr::JsonDepth(arg)
            | FunctionExpr::JsonValid(arg)
            | FunctionExpr::JsonQuote(arg)
            | FunctionExpr::JsonTypeof(arg)
            | FunctionExpr::JsonArrayLength(arg)
            | FunctionExpr::JsonStripNulls(arg)
            | FunctionExpr::JsonbStripNulls(arg)
            | FunctionExpr::JsonbPretty(arg)
            | FunctionExpr::StAsText(arg)
            | FunctionExpr::StAsWkt(arg)
            | FunctionExpr::StAsEwkt(arg) => {
                concrete_iter!(iter::once(arg.as_ref()))
            }
            FunctionExpr::JsonObjectAgg { key, value, .. } => {
                concrete_iter!(iter::once(key.as_ref()).chain(iter::once(value.as_ref())))
            }
            FunctionExpr::Timediff(a, b)
            | FunctionExpr::Addtime(a, b)
            | FunctionExpr::DateFormat(a, b)
            | FunctionExpr::DateTrunc(a, b)
            | FunctionExpr::IfNull(a, b)
            | FunctionExpr::JsonOverlaps(a, b) => {
                concrete_iter!(iter::once(a.as_ref()).chain(iter::once(b.as_ref())))
            }
            FunctionExpr::ConvertTz(a, b, c) | FunctionExpr::SplitPart(a, b, c) => {
                concrete_iter!(
                    iter::once(a.as_ref())
                        .chain(iter::once(b.as_ref()))
                        .chain(iter::once(c.as_ref()))
                )
            }
            FunctionExpr::Round(e, prec) => {
                concrete_iter!(iter::once(e.as_ref()).chain(prec.iter().map(|p| p.as_ref())))
            }
            FunctionExpr::ArrayToString(a, b, c) => {
                concrete_iter!(
                    iter::once(a.as_ref())
                        .chain(iter::once(b.as_ref()))
                        .chain(c.iter().map(|p| p.as_ref()))
                )
            }
            FunctionExpr::JsonbInsert(a, b, c, d) | FunctionExpr::JsonbSet(a, b, c, d) => {
                concrete_iter!(
                    iter::once(a.as_ref())
                        .chain(iter::once(b.as_ref()))
                        .chain(iter::once(c.as_ref()))
                        .chain(d.iter().map(|p| p.as_ref()))
                )
            }
            FunctionExpr::JsonbSetLax(a, b, c, d, e) => {
                concrete_iter!(
                    iter::once(a.as_ref())
                        .chain(iter::once(b.as_ref()))
                        .chain(iter::once(c.as_ref()))
                        .chain(d.iter().map(|p| p.as_ref()))
                        .chain(e.iter().map(|p| p.as_ref()))
                )
            }
            FunctionExpr::JsonExtractPathText(json, keys)
            | FunctionExpr::JsonExtractPath(json, keys)
            | FunctionExpr::JsonbExtractPath(json, keys) => {
                concrete_iter!(iter::once(json.as_ref()).chain(keys.iter()))
            }
            FunctionExpr::Coalesce(args)
            | FunctionExpr::Greatest(args)
            | FunctionExpr::Least(args)
            | FunctionExpr::Concat(args)
            | FunctionExpr::ConcatWs(args)
            | FunctionExpr::JsonObject(args)
            | FunctionExpr::JsonbObject(args)
            | FunctionExpr::JsonBuildObject(args)
            | FunctionExpr::JsonbBuildObject(args) => {
                concrete_iter!(args.iter())
            }
            FunctionExpr::CountStar
            | FunctionExpr::RowNumber
            | FunctionExpr::Rank
            | FunctionExpr::DenseRank
            | FunctionExpr::CurrentDate
            | FunctionExpr::CurrentTimestamp(_)
            | FunctionExpr::CurrentTime
            | FunctionExpr::LocalTimestamp
            | FunctionExpr::LocalTime
            | FunctionExpr::CurrentUser
            | FunctionExpr::SessionUser
            | FunctionExpr::CurrentCatalog
            | FunctionExpr::SqlUser => concrete_iter!(iter::empty()),
            FunctionExpr::Udf { arguments, .. } => concrete_iter!(arguments),
            FunctionExpr::Substring { string, pos, len } => {
                concrete_iter!(
                    iter::once(string.as_ref())
                        .chain(pos.iter().map(|p| p.as_ref()))
                        .chain(len.iter().map(|p| p.as_ref()))
                )
            }
        }
    }

    /// Consumes self and returns all direct arguments as owned `Expr` values
    pub fn into_arguments(self) -> Vec<Expr> {
        match self {
            FunctionExpr::ArrayAgg { expr, .. }
            | FunctionExpr::Avg { expr, .. }
            | FunctionExpr::Count { expr, .. }
            | FunctionExpr::Sum { expr, .. }
            | FunctionExpr::Max(expr)
            | FunctionExpr::Min(expr)
            | FunctionExpr::GroupConcat { expr, .. }
            | FunctionExpr::StringAgg { expr, .. }
            | FunctionExpr::Extract { expr, .. }
            | FunctionExpr::Bucket { expr, .. }
            | FunctionExpr::Lower { expr, .. }
            | FunctionExpr::Upper { expr, .. }
            | FunctionExpr::DayOfWeek(expr)
            | FunctionExpr::Month(expr)
            | FunctionExpr::Length(expr)
            | FunctionExpr::OctetLength(expr)
            | FunctionExpr::CharLength(expr)
            | FunctionExpr::Ascii(expr)
            | FunctionExpr::Hex(expr)
            | FunctionExpr::JsonDepth(expr)
            | FunctionExpr::JsonValid(expr)
            | FunctionExpr::JsonQuote(expr)
            | FunctionExpr::JsonTypeof(expr)
            | FunctionExpr::JsonArrayLength(expr)
            | FunctionExpr::JsonStripNulls(expr)
            | FunctionExpr::JsonbStripNulls(expr)
            | FunctionExpr::JsonbPretty(expr)
            | FunctionExpr::StAsText(expr)
            | FunctionExpr::StAsWkt(expr)
            | FunctionExpr::StAsEwkt(expr) => vec![*expr],
            FunctionExpr::JsonObjectAgg { key, value, .. } => vec![*key, *value],
            FunctionExpr::Timediff(a, b)
            | FunctionExpr::Addtime(a, b)
            | FunctionExpr::DateFormat(a, b)
            | FunctionExpr::DateTrunc(a, b)
            | FunctionExpr::IfNull(a, b)
            | FunctionExpr::JsonOverlaps(a, b) => vec![*a, *b],
            FunctionExpr::ConvertTz(a, b, c) | FunctionExpr::SplitPart(a, b, c) => vec![*a, *b, *c],
            FunctionExpr::Round(e, prec) => {
                let mut v = vec![*e];
                if let Some(p) = prec {
                    v.push(*p);
                }
                v
            }
            FunctionExpr::ArrayToString(a, b, c) => {
                let mut v = vec![*a, *b];
                if let Some(c) = c {
                    v.push(*c);
                }
                v
            }
            FunctionExpr::JsonbInsert(a, b, c, d) | FunctionExpr::JsonbSet(a, b, c, d) => {
                let mut v = vec![*a, *b, *c];
                if let Some(d) = d {
                    v.push(*d);
                }
                v
            }
            FunctionExpr::JsonbSetLax(a, b, c, d, e) => {
                let mut v = vec![*a, *b, *c];
                if let Some(d) = d {
                    v.push(*d);
                }
                if let Some(e) = e {
                    v.push(*e);
                }
                v
            }
            FunctionExpr::JsonExtractPathText(json, mut keys)
            | FunctionExpr::JsonExtractPath(json, mut keys)
            | FunctionExpr::JsonbExtractPath(json, mut keys) => {
                let mut v = vec![*json];
                v.append(&mut keys);
                v
            }
            FunctionExpr::Coalesce(args)
            | FunctionExpr::Greatest(args)
            | FunctionExpr::Least(args)
            | FunctionExpr::Concat(args)
            | FunctionExpr::ConcatWs(args)
            | FunctionExpr::JsonObject(args)
            | FunctionExpr::JsonbObject(args)
            | FunctionExpr::JsonBuildObject(args)
            | FunctionExpr::JsonbBuildObject(args) => args,
            FunctionExpr::Substring { string, pos, len } => {
                let mut v = vec![*string];
                if let Some(p) = pos {
                    v.push(*p);
                }
                if let Some(l) = len {
                    v.push(*l);
                }
                v
            }
            FunctionExpr::CountStar
            | FunctionExpr::RowNumber
            | FunctionExpr::Rank
            | FunctionExpr::DenseRank
            | FunctionExpr::CurrentDate
            | FunctionExpr::CurrentTimestamp(_)
            | FunctionExpr::CurrentTime
            | FunctionExpr::LocalTimestamp
            | FunctionExpr::LocalTime
            | FunctionExpr::CurrentUser
            | FunctionExpr::SessionUser
            | FunctionExpr::CurrentCatalog
            | FunctionExpr::SqlUser => vec![],
            FunctionExpr::Udf { arguments, .. } => arguments,
        }
    }

    /// Maps a function name and argument list to the corresponding typed `FunctionExpr` variant.
    ///
    /// Recognized built-in names are mapped to their specific variants. Unrecognized names,
    /// or calls with fewer arguments than the minimum required for a variant, produce
    /// `FunctionExpr::Udf` rather than panicking.
    ///
    /// This is the canonical source of the name→variant mapping used by both the SQL parser
    /// (`nom-sql`) and the query generator.
    pub fn from_name_and_args(name: &str, args: Vec<Expr>) -> Self {
        let n = args.len();
        let mut it = args.into_iter();
        // All `next_box!()` calls below are guarded by `if n >= K` match guards, so
        // `it.next()` is guaranteed to return `Some`.
        macro_rules! next_box {
            () => {
                Box::new(it.next().expect("arity guard ensures arg is available"))
            };
        }
        // Capture the result first; the debug_assert below checks that fixed-arity
        // functions do not silently drop excess arguments.
        let result = match name {
            // Date/Time
            "convert_tz" if n >= 3 => Self::ConvertTz(next_box!(), next_box!(), next_box!()),
            "dayofweek" if n >= 1 => Self::DayOfWeek(next_box!()),
            "month" if n >= 1 => Self::Month(next_box!()),
            "timediff" if n >= 2 => Self::Timediff(next_box!(), next_box!()),
            "addtime" if n >= 2 => Self::Addtime(next_box!(), next_box!()),
            "date_format" if n >= 2 => Self::DateFormat(next_box!(), next_box!()),
            "date_trunc" if n >= 2 => Self::DateTrunc(next_box!(), next_box!()),
            // Both `now()` and `current_timestamp()` map to `CurrentTimestamp`, preserving the
            // optional precision argument for DDL round-trip display (Readyset doesn't use it).
            "now" | "current_timestamp" => Self::CurrentTimestamp(it.next().map(Box::new)),
            // Null handling
            "ifnull" if n >= 2 => Self::IfNull(next_box!(), next_box!()),
            "coalesce" => Self::Coalesce(it.by_ref().collect()),
            // Math
            "round" if n >= 1 => {
                let expr = next_box!();
                let prec = it.next().map(Box::new);
                Self::Round(expr, prec)
            }
            "greatest" => Self::Greatest(it.by_ref().collect()),
            "least" => Self::Least(it.by_ref().collect()),
            // String
            "concat" => Self::Concat(it.by_ref().collect()),
            "concat_ws" => Self::ConcatWs(it.by_ref().collect()),
            "split_part" if n >= 3 => Self::SplitPart(next_box!(), next_box!(), next_box!()),
            "length" if n >= 1 => Self::Length(next_box!()),
            "octet_length" if n >= 1 => Self::OctetLength(next_box!()),
            "char_length" | "character_length" if n >= 1 => Self::CharLength(next_box!()),
            "ascii" if n >= 1 => Self::Ascii(next_box!()),
            "hex" if n >= 1 => Self::Hex(next_box!()),
            "lower" if n >= 1 => Self::Lower {
                expr: next_box!(),
                collation: None,
            },
            "upper" if n >= 1 => Self::Upper {
                expr: next_box!(),
                collation: None,
            },
            "substring" | "substr" if n >= 1 => {
                let string = next_box!();
                let pos = it.next().map(Box::new);
                let len = it.next().map(Box::new);
                Self::Substring { string, pos, len }
            }
            // JSON (consolidated)
            "json_depth" if n >= 1 => Self::JsonDepth(next_box!()),
            "json_valid" if n >= 1 => Self::JsonValid(next_box!()),
            "json_overlaps" if n >= 2 => Self::JsonOverlaps(next_box!(), next_box!()),
            "json_quote" if n >= 1 => Self::JsonQuote(next_box!()),
            "json_typeof" | "jsonb_typeof" if n >= 1 => Self::JsonTypeof(next_box!()),
            "json_array_length" | "jsonb_array_length" if n >= 1 => {
                Self::JsonArrayLength(next_box!())
            }
            "json_extract_path_text" | "jsonb_extract_path_text" if n >= 1 => {
                let json = next_box!();
                Self::JsonExtractPathText(json, it.by_ref().collect())
            }
            // JSON (separate)
            "json_object" => Self::JsonObject(it.by_ref().collect()),
            "jsonb_object" => Self::JsonbObject(it.by_ref().collect()),
            "json_build_object" => Self::JsonBuildObject(it.by_ref().collect()),
            "jsonb_build_object" => Self::JsonbBuildObject(it.by_ref().collect()),
            "json_strip_nulls" if n >= 1 => Self::JsonStripNulls(next_box!()),
            "jsonb_strip_nulls" if n >= 1 => Self::JsonbStripNulls(next_box!()),
            "json_extract_path" if n >= 1 => {
                let json = next_box!();
                Self::JsonExtractPath(json, it.by_ref().collect())
            }
            "jsonb_extract_path" if n >= 1 => {
                let json = next_box!();
                Self::JsonbExtractPath(json, it.by_ref().collect())
            }
            "jsonb_insert" if n >= 3 => {
                let (a, b, c) = (next_box!(), next_box!(), next_box!());
                Self::JsonbInsert(a, b, c, it.next().map(Box::new))
            }
            "jsonb_set" if n >= 3 => {
                let (a, b, c) = (next_box!(), next_box!(), next_box!());
                Self::JsonbSet(a, b, c, it.next().map(Box::new))
            }
            "jsonb_set_lax" if n >= 3 => {
                let (a, b, c) = (next_box!(), next_box!(), next_box!());
                let d = it.next().map(Box::new);
                let e = it.next().map(Box::new);
                Self::JsonbSetLax(a, b, c, d, e)
            }
            "jsonb_pretty" if n >= 1 => Self::JsonbPretty(next_box!()),
            // Array
            "array_to_string" if n >= 2 => {
                let (a, b) = (next_box!(), next_box!());
                Self::ArrayToString(a, b, it.next().map(Box::new))
            }
            // Spatial
            "st_astext" if n >= 1 => Self::StAsText(next_box!()),
            "st_aswkt" if n >= 1 => Self::StAsWkt(next_box!()),
            "st_asewkt" if n >= 1 => Self::StAsEwkt(next_box!()),
            // Unrecognized name, or a known name called with fewer arguments than required
            // (arity guards above did not match). Both cases fall back to Udf so that the
            // call site can produce a "user-defined function not supported" error downstream
            // rather than failing silently at parse time.
            _ => Self::Udf {
                schema: None,
                name: name.into(),
                arguments: it.by_ref().collect(),
            },
        };
        debug_assert!(
            it.next().is_none(),
            "from_name_and_args: extra arguments silently dropped for {name:?}"
        );
        result
    }

    /// Returns the canonical lowercase SQL function name used by `BuiltinFunction::from_name_and_args`.
    ///
    /// This covers only the typed built-in variants that are lowered via `from_name_and_args`.
    /// Returns `None` for aggregate, window, and UDF variants that are not lowered that way.
    pub fn builtin_name(&self) -> Option<&'static str> {
        match self {
            FunctionExpr::ConvertTz(..) => Some("convert_tz"),
            FunctionExpr::DayOfWeek(..) => Some("dayofweek"),
            FunctionExpr::Month(..) => Some("month"),
            FunctionExpr::Timediff(..) => Some("timediff"),
            FunctionExpr::Addtime(..) => Some("addtime"),
            FunctionExpr::DateFormat(..) => Some("date_format"),
            FunctionExpr::DateTrunc(..) => Some("date_trunc"),
            FunctionExpr::IfNull(..) => Some("ifnull"),
            FunctionExpr::Coalesce(..) => Some("coalesce"),
            FunctionExpr::Round(..) => Some("round"),
            FunctionExpr::Greatest(..) => Some("greatest"),
            FunctionExpr::Least(..) => Some("least"),
            FunctionExpr::Concat(..) => Some("concat"),
            FunctionExpr::ConcatWs(..) => Some("concat_ws"),
            FunctionExpr::SplitPart(..) => Some("split_part"),
            FunctionExpr::Length(..) => Some("length"),
            FunctionExpr::OctetLength(..) => Some("octet_length"),
            FunctionExpr::CharLength(..) => Some("char_length"),
            FunctionExpr::Ascii(..) => Some("ascii"),
            FunctionExpr::Hex(..) => Some("hex"),
            FunctionExpr::JsonDepth(..) => Some("json_depth"),
            FunctionExpr::JsonValid(..) => Some("json_valid"),
            FunctionExpr::JsonOverlaps(..) => Some("json_overlaps"),
            FunctionExpr::JsonQuote(..) => Some("json_quote"),
            FunctionExpr::JsonTypeof(..) => Some("json_typeof"),
            FunctionExpr::JsonArrayLength(..) => Some("json_array_length"),
            FunctionExpr::JsonExtractPathText(..) => Some("json_extract_path_text"),
            FunctionExpr::JsonObject(..) => Some("json_object"),
            FunctionExpr::JsonbObject(..) => Some("jsonb_object"),
            FunctionExpr::JsonBuildObject(..) => Some("json_build_object"),
            FunctionExpr::JsonbBuildObject(..) => Some("jsonb_build_object"),
            FunctionExpr::JsonStripNulls(..) => Some("json_strip_nulls"),
            FunctionExpr::JsonbStripNulls(..) => Some("jsonb_strip_nulls"),
            FunctionExpr::JsonExtractPath(..) => Some("json_extract_path"),
            FunctionExpr::JsonbExtractPath(..) => Some("jsonb_extract_path"),
            FunctionExpr::JsonbInsert(..) => Some("jsonb_insert"),
            FunctionExpr::JsonbSet(..) => Some("jsonb_set"),
            FunctionExpr::JsonbSetLax(..) => Some("jsonb_set_lax"),
            FunctionExpr::JsonbPretty(..) => Some("jsonb_pretty"),
            FunctionExpr::ArrayToString(..) => Some("array_to_string"),
            FunctionExpr::StAsText(..) => Some("st_astext"),
            FunctionExpr::StAsWkt(..) => Some("st_aswkt"),
            FunctionExpr::StAsEwkt(..) => Some("st_asewkt"),
            // Aggregates — not lowered via from_name_and_args
            FunctionExpr::ArrayAgg { .. }
            | FunctionExpr::Avg { .. }
            | FunctionExpr::Count { .. }
            | FunctionExpr::CountStar
            | FunctionExpr::GroupConcat { .. }
            | FunctionExpr::StringAgg { .. }
            | FunctionExpr::Sum { .. }
            | FunctionExpr::Max(_)
            | FunctionExpr::Min(_)
            | FunctionExpr::JsonObjectAgg { .. }
            // Window functions — not lowered via from_name_and_args
            | FunctionExpr::RowNumber
            | FunctionExpr::Rank
            | FunctionExpr::DenseRank
            // These built-ins have dedicated lowering arms that do not go through
            // the builtin_name() / into_arguments() round-trip
            | FunctionExpr::Bucket { .. }
            | FunctionExpr::Substring { .. }
            | FunctionExpr::Extract { .. }
            | FunctionExpr::Lower { .. }
            | FunctionExpr::Upper { .. }
            // No-paren functions — lowered directly, not via from_name_and_args
            | FunctionExpr::CurrentDate
            | FunctionExpr::CurrentTimestamp(_)
            | FunctionExpr::CurrentTime
            | FunctionExpr::LocalTimestamp
            | FunctionExpr::LocalTime
            | FunctionExpr::CurrentUser
            | FunctionExpr::SessionUser
            | FunctionExpr::CurrentCatalog
            | FunctionExpr::SqlUser
            // UDF — dynamic name, never a static builtin_name
            | FunctionExpr::Udf { .. } => None,
        }
    }
}

impl DialectDisplay for FunctionExpr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            FunctionExpr::ArrayAgg {
                expr,
                distinct,
                order_by,
            } => {
                write!(
                    f,
                    "array_agg({}{}{})",
                    distinct,
                    expr.display(dialect),
                    order_by_clause_str!(order_by, dialect),
                )
            }
            FunctionExpr::Avg {
                expr,
                distinct: true,
            } => write!(f, "avg(distinct {})", expr.display(dialect)),
            FunctionExpr::Count {
                expr,
                distinct: true,
            } => write!(f, "count(distinct {})", expr.display(dialect)),
            FunctionExpr::Sum {
                expr,
                distinct: true,
            } => write!(f, "sum(distinct {})", expr.display(dialect)),
            FunctionExpr::Avg { expr, .. } => write!(f, "avg({})", expr.display(dialect)),
            FunctionExpr::Count { expr, .. } => write!(f, "count({})", expr.display(dialect)),
            FunctionExpr::CountStar => write!(f, "count(*)"),
            FunctionExpr::Bucket { expr, interval } => write!(
                f,
                "bucket({}, {})",
                expr.display(dialect),
                interval.display(dialect),
            ),
            FunctionExpr::Sum { expr, .. } => write!(f, "sum({})", expr.display(dialect)),
            FunctionExpr::Max(col) => write!(f, "max({})", col.display(dialect)),
            FunctionExpr::Min(col) => write!(f, "min({})", col.display(dialect)),
            FunctionExpr::GroupConcat {
                expr,
                separator,
                distinct,
                order_by,
            } => {
                write!(
                    f,
                    "group_concat({}{}{}",
                    distinct,
                    expr.display(dialect),
                    order_by_clause_str!(order_by, dialect),
                )?;
                if let Some(separator) = separator {
                    write!(
                        f,
                        " separator '{}'",
                        separator.replace('\'', "''").replace('\\', "\\\\")
                    )?;
                }
                write!(f, ")")
            }
            FunctionExpr::StringAgg {
                expr,
                separator,
                distinct,
                order_by,
            } => {
                write!(
                    f,
                    "string_agg({}{}{}",
                    distinct,
                    expr.display(dialect),
                    order_by_clause_str!(order_by, dialect),
                )?;
                if let Some(separator) = separator {
                    write!(
                        f,
                        ", '{}'",
                        separator.replace('\'', "''").replace('\\', "\\\\")
                    )?;
                } else {
                    write!(f, ", NULL")?;
                }
                write!(f, ")")
            }
            FunctionExpr::Substring { string, pos, len } => {
                write!(f, "substring({}", string.display(dialect))?;

                if let Some(pos) = pos {
                    write!(f, " from {}", pos.display(dialect))?;
                }

                if let Some(len) = len {
                    write!(f, " for {}", len.display(dialect))?;
                }

                write!(f, ")")
            }
            FunctionExpr::JsonObjectAgg {
                key,
                value,
                allow_duplicate_keys,
            } => {
                let fname = match dialect {
                    Dialect::MySQL => "json_objectagg",
                    Dialect::PostgreSQL => {
                        if *allow_duplicate_keys {
                            "json_object_agg"
                        } else {
                            "jsonb_object_agg"
                        }
                    }
                };

                write!(
                    f,
                    "{}({}, {})",
                    fname,
                    key.display(dialect),
                    value.display(dialect)
                )
            }
            FunctionExpr::Extract { field, expr } => {
                write!(f, "extract({field} FROM {})", expr.display(dialect))
            }
            FunctionExpr::Lower { expr, collation } => {
                write!(f, "lower({}", expr.display(dialect))?;
                if let Some(c) = collation {
                    write!(f, " COLLATE {c}")?;
                }
                write!(f, ")")
            }
            FunctionExpr::Upper { expr, collation } => {
                write!(f, "upper({}", expr.display(dialect))?;
                if let Some(c) = collation {
                    write!(f, " COLLATE {c}")?;
                }
                write!(f, ")")
            }
            FunctionExpr::RowNumber => write!(f, "ROW_NUMBER()"),
            FunctionExpr::Rank => write!(f, "RANK()"),
            FunctionExpr::DenseRank => write!(f, "DENSE_RANK()"),
            // No-paren functions
            FunctionExpr::CurrentDate => write!(f, "current_date"),
            FunctionExpr::CurrentTimestamp(prec) => match prec {
                Some(p) => write!(f, "current_timestamp({})", p.display(dialect)),
                None => write!(f, "current_timestamp"),
            },
            FunctionExpr::CurrentTime => write!(f, "current_time"),
            FunctionExpr::LocalTimestamp => write!(f, "localtimestamp"),
            FunctionExpr::LocalTime => write!(f, "localtime"),
            FunctionExpr::CurrentUser => write!(f, "current_user"),
            FunctionExpr::SessionUser => write!(f, "session_user"),
            FunctionExpr::CurrentCatalog => write!(f, "current_catalog"),
            FunctionExpr::SqlUser => write!(f, "user"),
            // New function variants - helper macro for simple display
            FunctionExpr::ConvertTz(a, b, c) => write!(
                f,
                "convert_tz({}, {}, {})",
                a.display(dialect),
                b.display(dialect),
                c.display(dialect)
            ),
            FunctionExpr::DayOfWeek(e) => write!(f, "dayofweek({})", e.display(dialect)),
            FunctionExpr::Month(e) => write!(f, "month({})", e.display(dialect)),
            FunctionExpr::Timediff(a, b) => write!(
                f,
                "timediff({}, {})",
                a.display(dialect),
                b.display(dialect)
            ),
            FunctionExpr::Addtime(a, b) => {
                write!(f, "addtime({}, {})", a.display(dialect), b.display(dialect))
            }
            FunctionExpr::DateFormat(a, b) => write!(
                f,
                "date_format({}, {})",
                a.display(dialect),
                b.display(dialect)
            ),
            FunctionExpr::DateTrunc(a, b) => write!(
                f,
                "date_trunc({}, {})",
                a.display(dialect),
                b.display(dialect)
            ),
            FunctionExpr::IfNull(a, b) => {
                write!(f, "ifnull({}, {})", a.display(dialect), b.display(dialect))
            }
            FunctionExpr::Coalesce(args) => write!(
                f,
                "coalesce({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::Round(e, prec) => {
                write!(f, "round({}", e.display(dialect))?;
                if let Some(p) = prec {
                    write!(f, ", {}", p.display(dialect))?;
                }
                write!(f, ")")
            }
            FunctionExpr::Greatest(args) => write!(
                f,
                "greatest({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::Least(args) => write!(
                f,
                "least({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::Concat(args) => write!(
                f,
                "concat({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::ConcatWs(args) => write!(
                f,
                "concat_ws({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::SplitPart(a, b, c) => write!(
                f,
                "split_part({}, {}, {})",
                a.display(dialect),
                b.display(dialect),
                c.display(dialect)
            ),
            FunctionExpr::Length(e) => write!(f, "length({})", e.display(dialect)),
            FunctionExpr::OctetLength(e) => write!(f, "octet_length({})", e.display(dialect)),
            FunctionExpr::CharLength(e) => write!(f, "char_length({})", e.display(dialect)),
            FunctionExpr::Ascii(e) => write!(f, "ascii({})", e.display(dialect)),
            FunctionExpr::Hex(e) => write!(f, "hex({})", e.display(dialect)),
            FunctionExpr::JsonDepth(e) => write!(f, "json_depth({})", e.display(dialect)),
            FunctionExpr::JsonValid(e) => write!(f, "json_valid({})", e.display(dialect)),
            FunctionExpr::JsonOverlaps(a, b) => write!(
                f,
                "json_overlaps({}, {})",
                a.display(dialect),
                b.display(dialect)
            ),
            FunctionExpr::JsonQuote(e) => write!(f, "json_quote({})", e.display(dialect)),
            FunctionExpr::JsonTypeof(e) => write!(f, "json_typeof({})", e.display(dialect)),
            FunctionExpr::JsonArrayLength(e) => {
                write!(f, "json_array_length({})", e.display(dialect))
            }
            FunctionExpr::JsonExtractPathText(json, keys) => write!(
                f,
                "json_extract_path_text({}, {})",
                json.display(dialect),
                keys.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonObject(args) => write!(
                f,
                "json_object({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonbObject(args) => write!(
                f,
                "jsonb_object({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonBuildObject(args) => write!(
                f,
                "json_build_object({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonbBuildObject(args) => write!(
                f,
                "jsonb_build_object({})",
                args.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonStripNulls(e) => {
                write!(f, "json_strip_nulls({})", e.display(dialect))
            }
            FunctionExpr::JsonbStripNulls(e) => {
                write!(f, "jsonb_strip_nulls({})", e.display(dialect))
            }
            FunctionExpr::JsonExtractPath(json, keys) => write!(
                f,
                "json_extract_path({}, {})",
                json.display(dialect),
                keys.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonbExtractPath(json, keys) => write!(
                f,
                "jsonb_extract_path({}, {})",
                json.display(dialect),
                keys.iter().map(|a| a.display(dialect)).join(", ")
            ),
            FunctionExpr::JsonbInsert(a, b, c, d) => {
                write!(
                    f,
                    "jsonb_insert({}, {}, {}",
                    a.display(dialect),
                    b.display(dialect),
                    c.display(dialect)
                )?;
                if let Some(d) = d {
                    write!(f, ", {}", d.display(dialect))?;
                }
                write!(f, ")")
            }
            FunctionExpr::JsonbSet(a, b, c, d) => {
                write!(
                    f,
                    "jsonb_set({}, {}, {}",
                    a.display(dialect),
                    b.display(dialect),
                    c.display(dialect)
                )?;
                if let Some(d) = d {
                    write!(f, ", {}", d.display(dialect))?;
                }
                write!(f, ")")
            }
            FunctionExpr::JsonbSetLax(a, b, c, d, e) => {
                write!(
                    f,
                    "jsonb_set_lax({}, {}, {}",
                    a.display(dialect),
                    b.display(dialect),
                    c.display(dialect)
                )?;
                if let Some(d) = d {
                    write!(f, ", {}", d.display(dialect))?;
                }
                if let Some(e) = e {
                    write!(f, ", {}", e.display(dialect))?;
                }
                write!(f, ")")
            }
            FunctionExpr::JsonbPretty(e) => write!(f, "jsonb_pretty({})", e.display(dialect)),
            FunctionExpr::ArrayToString(a, b, c) => {
                write!(
                    f,
                    "array_to_string({}, {}",
                    a.display(dialect),
                    b.display(dialect)
                )?;
                if let Some(c) = c {
                    write!(f, ", {}", c.display(dialect))?;
                }
                write!(f, ")")
            }
            FunctionExpr::StAsText(e) => write!(f, "st_astext({})", e.display(dialect)),
            FunctionExpr::StAsWkt(e) => write!(f, "st_aswkt({})", e.display(dialect)),
            FunctionExpr::StAsEwkt(e) => write!(f, "st_asewkt({})", e.display(dialect)),
            FunctionExpr::Udf {
                schema,
                name,
                arguments,
            } => {
                if let Some(s) = schema {
                    write!(f, "{}.", dialect.quote_identifier(s))?;
                }
                write!(
                    f,
                    "{}({})",
                    dialect.maybe_quote_identifier(name),
                    arguments.iter().map(|arg| arg.display(dialect)).join(", ")
                )
            }
        })
    }
}

/// Binary infix operators with [`Expr`] on both the left- and right-hand sides
///
/// This type is used as the operator in [`Expr::BinaryOp`].
///
/// Note that because all binary operators have expressions on both sides, SQL `IN` is not a binary
/// operator - since it must have either a subquery or a list of expressions on its right-hand side
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BinaryOperator {
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `LIKE`
    Like,
    /// `NOT LIKE`
    NotLike,
    /// `ILIKE`
    ILike,
    /// `NOT ILIKE`
    NotILike,
    /// `=`
    Equal,
    /// `!=` or `<>`
    NotEqual,
    /// `>`
    Greater,
    /// `>=`
    GreaterOrEqual,
    /// `<`
    Less,
    /// `<=`
    LessOrEqual,
    /// `IS`
    Is,
    /// `IS NOT`
    IsNot,
    /// `+`
    Add,
    /// `-`
    Subtract,

    /// Postgres-specific `AT TIME ZONE` operator.
    AtTimeZone,

    /// `#-`
    ///
    /// Postgres-specific JSONB operator that takes JSONB and returns JSONB with the value at a
    /// key/index path removed.
    HashSubtract,

    /// `*`
    Multiply,
    /// `/`
    Divide,
    /// `%`
    Modulo,

    /// `?`
    ///
    /// Postgres-specific JSONB operator. Looks for the given string as an object key or an array
    /// element and returns a boolean indicating the presence or absence of that string.
    QuestionMark,

    /// `?|`
    ///
    /// Postgres-specific JSONB operator. Takes an array of strings and checks whether *any* of
    /// those strings appear as object keys or array elements in the provided JSON value.
    QuestionMarkPipe,

    /// `?&`
    ///
    /// Postgres-specific JSONB operator. Takes an array of strings and checks whether *all* of
    /// those strings appear as object keys or array elements in the provided JSON value.
    QuestionMarkAnd,

    /// `||`
    ///
    /// This can represent a few different operators in different contexts. In MySQL it can
    /// represent a boolean OR operation or a string concat operation depending on whether
    /// `PIPES_AS_CONCAT` is enabled in the SQL mode. In Postgres it can either represent string
    /// concatenation or JSON concatenation, depending on the context.
    DoublePipe,

    /// `->`
    ///
    /// This extracts JSON values as JSON:
    /// - MySQL: `json -> jsonpath` to `json` (unimplemented)
    /// - PostgreSQL: `json[b] -> {text,integer}` to `json[b]`
    Arrow1,

    /// `->>`
    ///
    /// This extracts JSON values and applies a transformation:
    /// - MySQL: `json ->> jsonpath` to unquoted `text` (unimplemented)
    /// - PostgreSQL: `json[b] ->> {text,integer}` to `text`
    Arrow2,

    /// PostgreSQL `#>`
    ///
    /// This extracts JSON values as JSON: `json[b] #> text[]` to `json[b]`.
    HashArrow1,

    /// PostgreSQL `#>>`
    ///
    /// This extracts JSON values as JSON: `json[b] #>> text[]` to `text`.
    HashArrow2,

    /// `@>`
    ///
    /// Postgres-specific JSONB operator. Takes two JSONB values and determines whether the
    /// left-side values immediately contain all of the right-side values.
    AtArrowRight,

    /// `<@`
    ///
    /// Postgres-specific JSONB operator. Behaves like [`BinaryOperator::AtArrowRight`] with
    /// switched sides for the operands.
    AtArrowLeft,

    /// `&&`
    ///
    /// Postgres-specific array overlap operator. Returns true if the two arrays have any elements
    /// in common.
    DoubleAmpersand,
}

#[derive(Default)]
pub struct BinaryOperatorParameters {
    /// Only generate operaters that are valid for Postgres `ANY`/`SOME`/`ALL` according to
    /// sqlparser, which only allows these: [=, >, <, =>, =<, !=]. Others are allowed by Postgres,
    /// so this may be changed. See: https://github.com/apache/datafusion-sqlparser-rs/issues/1841
    pub for_op_all_any: bool,
}

impl Arbitrary for BinaryOperator {
    type Parameters = BinaryOperatorParameters;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        if args.for_op_all_any {
            prop_oneof![
                Just(Self::Equal),
                Just(Self::NotEqual),
                Just(Self::Greater),
                Just(Self::GreaterOrEqual),
                Just(Self::Less),
                Just(Self::LessOrEqual),
            ]
            .boxed()
        } else {
            prop_oneof![
                Just(Self::Add),
                Just(Self::And),
                Just(Self::Or),
                Just(Self::Like),
                Just(Self::NotLike),
                Just(Self::ILike),
                Just(Self::NotILike),
                Just(Self::Equal),
                Just(Self::NotEqual),
                Just(Self::Greater),
                Just(Self::GreaterOrEqual),
                Just(Self::Less),
                Just(Self::LessOrEqual),
                Just(Self::Is),
                Just(Self::IsNot),
                Just(Self::Add),
                Just(Self::Subtract),
                Just(Self::AtTimeZone),
                Just(Self::HashSubtract),
                Just(Self::Multiply),
                Just(Self::Divide),
                Just(Self::Modulo),
                Just(Self::QuestionMark),
                Just(Self::QuestionMarkPipe),
                Just(Self::QuestionMarkAnd),
                Just(Self::DoublePipe),
                Just(Self::Arrow1),
                Just(Self::Arrow2),
                Just(Self::HashArrow1),
                Just(Self::HashArrow2),
                Just(Self::AtArrowRight),
                Just(Self::AtArrowLeft),
                Just(Self::DoubleAmpersand),
            ]
            .boxed()
        }
    }
}

impl BinaryOperator {
    /// Returns true if this operator represents an ordered comparison
    pub fn is_ordering_comparison(&self) -> bool {
        use BinaryOperator::*;
        matches!(self, Greater | GreaterOrEqual | Less | LessOrEqual)
    }

    /// If this operator is an ordered comparison, invert its meaning. (i.e. Greater becomes
    /// Less)
    pub fn flip_ordering_comparison(self) -> Result<Self, Self> {
        use BinaryOperator::*;
        match self {
            Greater => Ok(Less),
            GreaterOrEqual => Ok(LessOrEqual),
            Less => Ok(Greater),
            LessOrEqual => Ok(GreaterOrEqual),
            _ => Err(self),
        }
    }
}

impl TryFrom<sqlparser::ast::BinaryOperator> for BinaryOperator {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::BinaryOperator) -> Result<Self, Self::Error> {
        use sqlparser::ast::BinaryOperator as BinOp;
        Ok(match value {
            BinOp::And => Self::And,
            BinOp::Arrow => Self::Arrow1,
            BinOp::ArrowAt => Self::AtArrowLeft,
            BinOp::AtArrow => Self::AtArrowRight,
            BinOp::AtAt => unsupported!("@@ {value:?}")?,
            BinOp::AtQuestion => unsupported!("@? {value:?}")?,
            BinOp::BitwiseAnd => unsupported!("& {value:?}")?,
            BinOp::BitwiseOr => unsupported!("| {value:?}")?,
            BinOp::BitwiseXor => unsupported!("^ {value:?}")?,
            BinOp::Custom(_) => unsupported!("Custom operator {value:?}")?,
            BinOp::Divide => Self::Divide,
            BinOp::DuckIntegerDivide => unsupported!("DuckDB // {value:?}")?,
            BinOp::Eq => Self::Equal,
            BinOp::Gt => Self::Greater,
            BinOp::GtEq => Self::GreaterOrEqual,
            BinOp::HashArrow => Self::HashArrow1,
            BinOp::HashLongArrow => Self::HashArrow2,
            BinOp::HashMinus => Self::HashSubtract,
            BinOp::LongArrow => Self::Arrow2,
            BinOp::Lt => Self::Less,
            BinOp::LtEq => Self::LessOrEqual,
            BinOp::Minus => Self::Subtract,
            BinOp::Modulo => Self::Modulo,
            BinOp::Multiply => Self::Multiply,
            BinOp::MyIntegerDivide => unsupported!("MySQL DIV {value:?}")?,
            BinOp::NotEq => Self::NotEqual,
            BinOp::Or => Self::Or,
            BinOp::Overlaps => unsupported!("OVERLAPS {value:?}")?,
            BinOp::PGBitwiseShiftLeft => unsupported!("PGBitwiseShiftLeft '<<'")?,
            BinOp::PGBitwiseShiftRight => unsupported!("PGBitwiseShiftRight '>>'")?,
            BinOp::PGBitwiseXor => unsupported!("PGBitwiseXor '#'")?,
            BinOp::PGCustomBinaryOperator(_vec) => unsupported!("PGCustomBinaryOperator")?,
            BinOp::PGExp => unsupported!("PGExp '^'")?,
            BinOp::PGILikeMatch => Self::ILike,
            BinOp::PGLikeMatch => Self::Like,
            BinOp::PGNotILikeMatch => Self::NotILike,
            BinOp::PGNotLikeMatch => Self::NotLike,
            BinOp::PGOverlap => Self::DoubleAmpersand,
            BinOp::PGRegexIMatch => unsupported!("PGRegexIMatch '~*'")?,
            BinOp::PGRegexMatch => unsupported!("PGRegexMatch '~'")?,
            BinOp::PGRegexNotIMatch => unsupported!("PGRegexNotIMatch '!~*'")?,
            BinOp::PGRegexNotMatch => unsupported!("PGRegexNotMatch '!~'")?,
            BinOp::PGStartsWith => unsupported!("PGStartsWith '^@'")?,
            BinOp::Plus => Self::Add,
            BinOp::Question => Self::QuestionMark,
            BinOp::QuestionAnd => Self::QuestionMarkAnd,
            BinOp::QuestionPipe => Self::QuestionMarkPipe,
            BinOp::Spaceship => unsupported!("Spaceship '<=>'")?,
            BinOp::StringConcat => Self::DoublePipe,
            BinOp::Xor => unsupported!("XOR operator")?,
            BinOp::DoubleHash => unsupported!("DoubleHash '##'")?,
            BinOp::LtDashGt => unsupported!("LtDashGt '<->'")?,
            BinOp::AndLt => unsupported!("AndLt '&<'")?,
            BinOp::AndGt => unsupported!("AndGt '&>'")?,
            BinOp::LtLtPipe => unsupported!("LtLtPipe '<<|'")?,
            BinOp::PipeGtGt => unsupported!("PipeGtGt '|>>'")?,
            BinOp::AndLtPipe => unsupported!("AndLtPipe '&<|'")?,
            BinOp::PipeAndGt => unsupported!("PipeAndGt '|&>'")?,
            BinOp::LtCaret => unsupported!("LtCaret '<^'")?,
            BinOp::GtCaret => unsupported!("GtCaret '>^'")?,
            BinOp::QuestionHash => unsupported!("QuestionHash '?#'")?,
            BinOp::QuestionDash => unsupported!("QuestionDash '?-'")?,
            BinOp::QuestionDashPipe => unsupported!("QuestionDashPipe '?-|'")?,
            BinOp::QuestionDoublePipe => unsupported!("QuestionDoublePipe '?||'")?,
            BinOp::At => unsupported!("At '@'")?,
            BinOp::TildeEq => unsupported!("TildeEq '~='")?,
            BinOp::Assignment => unsupported!("Assignment ':='")?,
            BinOp::Match => unsupported!("MATCH operator")?,
            BinOp::Regexp => unsupported!("REGEXP operator")?,
        })
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match *self {
            Self::And => "AND",
            Self::Or => "OR",
            Self::Like => "LIKE",
            Self::NotLike => "NOT LIKE",
            Self::ILike => "ILIKE",
            Self::NotILike => "NOT ILIKE",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::Is => "IS",
            Self::IsNot => "IS NOT",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::HashSubtract => "#-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::QuestionMark => "?",
            Self::QuestionMarkPipe => "?|",
            Self::QuestionMarkAnd => "?&",
            Self::DoublePipe => "||",
            Self::Arrow1 => "->",
            Self::Arrow2 => "->>",
            Self::HashArrow1 => "#>",
            Self::HashArrow2 => "#>>",
            Self::AtArrowRight => "@>",
            Self::AtArrowLeft => "<@",
            Self::DoubleAmpersand => "&&",
            Self::AtTimeZone => "AT TIME ZONE",
        };
        f.write_str(op)
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum UnaryOperator {
    Neg,
    Not,
}

impl TryFrom<sqlparser::ast::UnaryOperator> for UnaryOperator {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::UnaryOperator) -> Result<Self, Self::Error> {
        use sqlparser::ast::UnaryOperator as UnOp;
        match value {
            UnOp::Plus => not_yet_implemented!("Unary + operator"),
            UnOp::Minus => Ok(Self::Neg),
            UnOp::Not => Ok(Self::Not),
            UnOp::BitwiseNot
            | UnOp::PGSquareRoot
            | UnOp::PGCubeRoot
            | UnOp::PGPostfixFactorial
            | UnOp::PGPrefixFactorial
            | UnOp::PGAbs => unsupported!("unsupported postgres unary operator"),
            UnOp::BangNot
            | UnOp::Hash
            | UnOp::AtDashAt
            | UnOp::DoubleAt
            | UnOp::QuestionDash
            | UnOp::QuestionPipe => unsupported!("unsupported unary operator {value}"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Neg => write!(f, "-"),
            UnaryOperator::Not => write!(f, "NOT"),
        }
    }
}

/// Right-hand side of IN
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, From, Arbitrary,
)]
pub enum InValue {
    Subquery(Box<SelectStatement>),
    List(Vec<Expr>),
}

impl DialectDisplay for InValue {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            InValue::Subquery(stmt) => write!(f, "{}", stmt.display(dialect)),
            InValue::List(exprs) => write!(
                f,
                "{}",
                exprs.iter().map(|expr| expr.display(dialect)).join(", ")
            ),
        })
    }
}

/// A single branch of a `CASE WHEN` statement
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, From, Arbitrary,
)]
pub struct CaseWhenBranch {
    pub condition: Expr,
    pub body: Expr,
}

impl DialectDisplay for CaseWhenBranch {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "WHEN {} THEN {}",
                self.condition.display(dialect),
                self.body.display(dialect)
            )
        })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub enum CastStyle {
    /// `CAST(expression AS type)`
    As,
    /// `CONVERT(expression USING charset)`
    Convert,
    /// `expr::type`
    DoubleColon,
}

impl Arbitrary for CastStyle {
    type Parameters = Option<Dialect>;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        match args {
            Some(Dialect::PostgreSQL) => {
                prop_oneof![Just(CastStyle::As), Just(CastStyle::DoubleColon)].boxed()
            }
            Some(Dialect::MySQL) => {
                prop_oneof![Just(CastStyle::As), Just(CastStyle::Convert),].boxed()
            }
            None => prop_oneof![
                Just(CastStyle::As),
                Just(CastStyle::Convert),
                Just(CastStyle::DoubleColon)
            ]
            .boxed(),
        }
    }
}

/// SQL Expression AST
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub enum ArrayArguments {
    List(Vec<Expr>),
    Subquery(Box<SelectStatement>),
}

/// SQL Expression AST
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub enum Expr {
    /// Function call expressions
    ///
    /// TODO(aspen): Eventually, the members of FunctionExpr should be inlined here
    Call(FunctionExpr),

    /// Literal values
    Literal(Literal),

    /// Binary operator
    BinaryOp {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// `<expr> <op> ANY (<expr>)` ([PostgreSQL docs][pg-docs])
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/functions-comparisons.html#id-1.5.8.30.16
    OpAny {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// `<expr> <op> SOME (<expr>)` ([PostgreSQL docs][pg-docs])
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/functions-comparisons.html#id-1.5.8.30.16
    OpSome {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// `<expr> <op> ALL (<expr>)` ([PostgreSQL docs][pg-docs])
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/functions-comparisons.html#id-1.5.8.30.16
    OpAll {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// Unary operator
    UnaryOp { op: UnaryOperator, rhs: Box<Expr> },

    /// CASE (WHEN condition THEN then_expr)... ELSE else_expr
    CaseWhen {
        branches: Vec<CaseWhenBranch>,
        else_expr: Option<Box<Expr>>,
    },

    /// A reference to a column
    Column(Column),

    /// EXISTS (select)
    Exists(Box<SelectStatement>),

    /// operand BETWEEN min AND max
    Between {
        operand: Box<Expr>,
        min: Box<Expr>,
        max: Box<Expr>,
        negated: bool,
    },

    /// A nested SELECT query
    NestedSelect(Box<SelectStatement>),

    /// An IN (or NOT IN) predicate
    ///
    /// Per the ANSI SQL standard, IN is its own AST node, not a binary operator
    In {
        lhs: Box<Expr>,
        rhs: InValue,
        negated: bool,
    },

    /// All three basic kinds of type casting:
    /// - `CAST(expression AS type)`
    /// - `expr::type`
    /// - `CONVERT(expression, type)`
    ///
    /// Note that non-type casting is handled elsewhere:
    /// - Charset conversions with `CONVERT(expression USING charset)` are in [`Expr::Convert`]
    /// - `expr COLLATE collation` is in [`Expr::Collate`]
    Cast {
        expr: Box<Expr>,
        ty: SqlType,
        style: CastStyle,
        /// MySQL ARRAY cast syntax: `CAST(expr AS type ARRAY)`
        array: bool,
    },

    /// `CONVERT(expression USING charset)`
    ///
    /// `CONVERT(expression, type)` is equivalent to `CAST(expression AS type)`, so we represent it
    /// with [`Expr::Cast`] and [`CastStyle::Convert`].
    ConvertUsing {
        expr: Box<Expr>,
        charset: CollationName,
    },

    /// fn OVER([PARTITION BY [column, ...]] [ORDER BY [column [ASC|DESC], ...]])
    /// fn: COUNT, SUM, MIN, MAX, AVG, ROW_NUMBER, RANK, DENSE_RANK
    /// more to be supported later
    WindowFunction {
        function: FunctionExpr,
        partition_by: Vec<Expr>,
        order_by: Vec<(Expr, OrderType, NullOrder)>,
    },

    /// `ARRAY[expr1, expr2, ...]`
    Array(ArrayArguments),

    /// `ROW` constructor: `ROW(expr1, expr2, ...)` or `(expr1, expr2, ...)`
    Row {
        /// Is the `ROW` keyword explicit?
        explicit: bool,
        exprs: Vec<Expr>,
    },

    /// A variable reference
    Variable(Variable),

    /// Expr [COLLATE collation]
    /// This is here because that's how sqlparser represents it
    /// and it should be desugared before lowering
    Collate {
        expr: Box<Expr>,
        collation: CollationName,
    },
}

impl Expr {
    /// Get the alias for this expression
    /// Because this is meant to be used in outputs and displays, it strips
    /// the table name from Expr::Column variants.
    /// It also truncates the alias to 64 characters.
    pub fn alias(&self, dialect: Dialect) -> Option<SqlIdentifier> {
        // TODO: Match upstream naming (unquoted identifiers, function name without args, etc ..)
        let mut alias = match self {
            Expr::Column(col) => col.name.to_string(), // strip the table's name
            Expr::BinaryOp { lhs, op, rhs } => {
                let left = lhs.alias(dialect)?;
                let right = rhs.alias(dialect)?;
                format!("{left} {op} {right}")
            }
            Expr::Call(function) => function.alias(dialect)?,

            // Placeholders in select are not GA'd, but just in case
            Expr::Literal(Literal::Placeholder(_)) => return None,
            Expr::Variable(_) => return None,

            e => e.display(dialect).to_string(),
        };

        alias = alias.chars().take(64).collect();

        Some(alias.into())
    }

    /// If this is a `Self::BinaryOp` where the right hand side is an ANY or ALL function call,
    /// extract it to turn this into a `Self::AllOp` or `Self::AnyOp`.
    ///
    /// This is necessary because for some binary operators (namely, `LIKE` and its variants),
    /// sqlparser-rs does not parse them as binary operators, but as special expression variants. As
    /// a result, it does not recognize the `ALL` or `ANY` comparison on the right, and parses it as
    /// a regular function call (in the case of `ALL`) or with a special flag on the `Expr::Like`
    /// variant (in the case of `ANY`). So for `LIKE`/`ILIKE`, which have the special `any` flag,
    /// this pass does nothing: but it should catch any other operators which behave this way and
    /// *don't* have a special flag for `any`.
    ///
    /// It may be worth the effort to make these representations more uniform on the sqlparser-rs
    /// side; see issue [#1770](https://github.com/apache/datafusion-sqlparser-rs/issues/1770).
    fn extract_all_any_op(self) -> Result<Self, AstConversionError> {
        if let Expr::BinaryOp { lhs, op, rhs } = self {
            match *rhs {
                Expr::Call(FunctionExpr::Udf {
                    schema: None,
                    name,
                    arguments,
                }) if name.eq_ignore_ascii_case("ALL") => {
                    Ok(Self::OpAll {
                        lhs,
                        op,
                        rhs: Box::new(arguments.into_iter().exactly_one().map_err(|_| {
                            failed_err!("Wrong number of arguments for ALL operator")
                        })?),
                    })
                }
                Expr::Call(FunctionExpr::Udf {
                    schema: None,
                    name,
                    arguments,
                }) if name.eq_ignore_ascii_case("ANY") => {
                    Ok(Self::OpAny {
                        lhs,
                        op,
                        rhs: Box::new(arguments.into_iter().exactly_one().map_err(|_| {
                            failed_err!("Wrong number of arguments for ANY operator")
                        })?),
                    })
                }
                _ => Ok(Expr::BinaryOp { lhs, op, rhs }),
            }
        } else {
            Ok(self)
        }
    }
}

impl TryFromDialect<sqlparser::ast::Expr> for Expr {
    fn try_from_dialect(
        value: sqlparser::ast::Expr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::Expr::*;
        match value {
            AllOp {
                left,
                compare_op,
                right,
            } => Ok(Self::OpAll {
                lhs: left.try_into_dialect(dialect)?,
                op: compare_op.try_into()?,
                rhs: right.try_into_dialect(dialect)?,
            }),
            AnyOp {
                left,
                compare_op,
                right,
                is_some: false,
            } => Ok(Self::OpAny {
                lhs: left.try_into_dialect(dialect)?,
                op: compare_op.try_into()?,
                rhs: right.try_into_dialect(dialect)?,
            }),
            AnyOp {
                left,
                compare_op,
                right,
                is_some: true,
            } => Ok(Self::OpSome {
                lhs: left.try_into_dialect(dialect)?,
                op: compare_op.try_into()?,
                rhs: right.try_into_dialect(dialect)?,
            }),
            Array(array) => Ok(Self::Array(ArrayArguments::List(
                array.elem.try_into_dialect(dialect)?,
            ))),
            AtTimeZone {
                timestamp,
                time_zone,
            } => Ok(Self::BinaryOp {
                lhs: timestamp.try_into_dialect(dialect)?,
                op: BinaryOperator::AtTimeZone,
                rhs: time_zone.try_into_dialect(dialect)?,
            }),
            Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Self::Between {
                operand: expr.try_into_dialect(dialect)?,
                min: low.try_into_dialect(dialect)?,
                max: high.try_into_dialect(dialect)?,
                negated,
            }),
            BinaryOp { left, op, right } => Ok(Self::BinaryOp {
                lhs: left.try_into_dialect(dialect)?,
                op: op.try_into()?,
                rhs: right.try_into_dialect(dialect)?,
            }),
            Case {
                operand: None,
                conditions,
                else_result,
                ..
            } => Ok(Self::CaseWhen {
                branches: conditions
                    .into_iter()
                    .map(|condition| {
                        Ok(CaseWhenBranch {
                            condition: condition.condition.try_into_dialect(dialect)?,
                            body: condition.result.try_into_dialect(dialect)?,
                        })
                    })
                    .try_collect()?,
                else_expr: else_result.try_into_dialect(dialect)?,
            }),
            Case {
                operand: Some(expr),
                conditions,
                else_result,
                ..
            } => Ok(Self::CaseWhen {
                branches: conditions
                    .into_iter()
                    .map(|condition| {
                        Ok(CaseWhenBranch {
                            condition: Expr::BinaryOp {
                                lhs: expr.clone().try_into_dialect(dialect)?,
                                op: BinaryOperator::Equal,
                                rhs: Box::new(condition.condition.try_into_dialect(dialect)?),
                            },
                            body: condition.result.try_into_dialect(dialect)?,
                        })
                    })
                    .try_collect()?,
                else_expr: else_result.try_into_dialect(dialect)?,
            }),
            Cast {
                format: Some(_), // BigQuery-specific FORMAT clause (not AT TIME ZONE)
                ..
            } => {
                unsupported!("CAST with FORMAT clause")
            }
            Cast {
                kind,
                expr,
                data_type,
                format: None,
                array,
            } => Ok(Self::Cast {
                expr: expr.try_into_dialect(dialect)?,
                ty: data_type.try_into_dialect(dialect)?,
                style: if kind == sqlparser::ast::CastKind::DoubleColon {
                    CastStyle::DoubleColon
                } else {
                    CastStyle::As
                },
                array,
            }),
            Ceil { expr: _, field: _ } => not_yet_implemented!("CEIL"),
            Collate { expr, collation } => Ok(Self::Collate {
                expr: expr.try_into_dialect(dialect)?,
                collation: collation.try_into()?,
            }),
            CompoundIdentifier(idents) => {
                let is_variable = if let Some(first) = idents.first() {
                    first.quote_style.is_none() && first.value.starts_with('@')
                } else {
                    false
                };
                if is_variable {
                    Ok(Self::Variable(idents.try_into_dialect(dialect)?))
                } else {
                    let column: Column = idents.into_dialect(dialect);
                    Ok(Self::Column(column))
                }
            }
            Convert {
                target_before_value,
                styles,
                is_try,
                expr,
                data_type,
                charset,
            } => {
                if target_before_value {
                    unsupported!("CONVERT with type before value")?;
                }
                if !styles.is_empty() {
                    unsupported!("CONVERT with style codes")?;
                }
                if is_try {
                    unsupported!("TRY_CONVERT")?;
                }

                let expr = expr.try_into_dialect(dialect)?;
                if let Some(data_type) = data_type {
                    debug_assert!(charset.is_none());
                    Ok(Self::Cast {
                        expr,
                        ty: data_type.try_into_dialect(dialect)?,
                        style: CastStyle::Convert,
                        array: false,
                    })
                } else if let Some(charset) = charset {
                    Ok(Self::ConvertUsing {
                        expr,
                        charset: charset.try_into()?,
                    })
                } else {
                    failed!("Neither type nor charset present in CONVERT")
                }
            }
            Cube(_vec) => unsupported!("GROUP BY CUBE"),
            Dictionary(_vec) => not_yet_implemented!("DICTIONARY"),
            Exists { subquery, negated } => {
                if negated {
                    Ok(Self::UnaryOp {
                        op: crate::ast::UnaryOperator::Not,
                        rhs: Box::new(Self::Exists(subquery.try_into_dialect(dialect)?)),
                    })
                } else {
                    Ok(Self::Exists(subquery.try_into_dialect(dialect)?))
                }
            }
            Extract {
                field,
                syntax: _, // We only support FROM
                expr,
            } => Ok(Self::Call(FunctionExpr::Extract {
                field: field.try_into()?,
                expr: expr.try_into_dialect(dialect)?,
            })),
            Floor { expr: _, field: _ } => not_yet_implemented!("FLOOR"),
            Function(function) => function.try_into_dialect(dialect),
            GroupingSets(_vec) => unsupported!("GROUP BY GROUPING SETS"),
            Identifier(ident) => Ok(ident.try_into_dialect(dialect)?),
            InList {
                expr,
                list,
                negated,
            } => Ok(Self::In {
                lhs: expr.try_into_dialect(dialect)?,
                rhs: crate::ast::InValue::List(list.try_into_dialect(dialect)?),
                negated,
            }),
            InSubquery {
                expr,
                subquery,
                negated,
            } => Ok(Self::In {
                lhs: expr.try_into_dialect(dialect)?,
                rhs: crate::ast::InValue::Subquery(subquery.try_into_dialect(dialect)?),
                negated,
            }),
            Interval(_interval) => not_yet_implemented!("INTERVAL"),
            InUnnest {
                expr: _,
                array_expr: _,
                negated: _,
            } => not_yet_implemented!("IN UNNEST"),
            IsFalse(expr) => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: BinaryOperator::Is,
                rhs: Box::new(Expr::Literal(crate::ast::Literal::Boolean(false))),
            }),
            IsNotFalse(expr) => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: BinaryOperator::IsNot,
                rhs: Box::new(Expr::Literal(crate::ast::Literal::Boolean(false))),
            }),
            IsTrue(expr) => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: BinaryOperator::Is,
                rhs: Box::new(Expr::Literal(crate::ast::Literal::Boolean(true))),
            }),
            IsNotTrue(expr) => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: BinaryOperator::IsNot,
                rhs: Box::new(Expr::Literal(crate::ast::Literal::Boolean(true))),
            }),
            IsNotNull(expr) => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: BinaryOperator::IsNot,
                rhs: Box::new(Expr::Literal(crate::ast::Literal::Null)),
            }),
            IsNull(expr) => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: BinaryOperator::Is,
                rhs: Box::new(Expr::Literal(crate::ast::Literal::Null)),
            }),
            IsDistinctFrom(_expr, _expr1) => not_yet_implemented!("IS DISTINCT FROM"),
            IsNotDistinctFrom(_expr, _expr1) => not_yet_implemented!("IS NOT DISTINCT FROM"),
            IsUnknown(_expr) => not_yet_implemented!("IS UNKNOWN"),
            IsNotUnknown(_expr) => not_yet_implemented!("IS NOT UNKNOWN"),
            JsonAccess { value: _, path: _ } => not_yet_implemented!("JSON access"),
            Lambda(_lambda_function) => unsupported!("LAMBDA"),
            Like {
                escape_char: Some(_),
                ..
            }
            | ILike {
                escape_char: Some(_),
                ..
            } => {
                unsupported!("LIKE/ILIKE with custom ESCAPE character")
            }
            Like {
                negated,
                expr,
                pattern,
                escape_char: None,
                any: false,
            } => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: if negated {
                    BinaryOperator::NotLike
                } else {
                    BinaryOperator::Like
                },
                rhs: pattern.try_into_dialect(dialect)?,
            }
            .extract_all_any_op()?),
            Like {
                negated,
                expr,
                pattern,
                escape_char: None,
                any: true,
            } => Ok(Self::OpAny {
                lhs: expr.try_into_dialect(dialect)?,
                op: if negated {
                    BinaryOperator::NotLike
                } else {
                    BinaryOperator::Like
                },
                rhs: pattern.try_into_dialect(dialect)?,
            }),
            ILike {
                negated,
                expr,
                pattern,
                escape_char: None,
                any: false,
            } => Ok(Self::BinaryOp {
                lhs: expr.try_into_dialect(dialect)?,
                op: if negated {
                    BinaryOperator::NotILike
                } else {
                    BinaryOperator::ILike
                },
                rhs: pattern.try_into_dialect(dialect)?,
            }
            .extract_all_any_op()?),
            ILike {
                negated,
                expr,
                pattern,
                escape_char: None,
                any: true,
            } => Ok(Self::OpAny {
                lhs: expr.try_into_dialect(dialect)?,
                op: if negated {
                    BinaryOperator::NotILike
                } else {
                    BinaryOperator::ILike
                },
                rhs: pattern.try_into_dialect(dialect)?,
            }),
            Map(_map) => not_yet_implemented!("MAP"),
            MatchAgainst {
                columns: _,
                match_value: _,
                opt_search_modifier: _,
            } => not_yet_implemented!("MATCH AGAINST"),
            Named { expr: _, name: _ } => unsupported!("BigQuery named expression"),
            Nested(expr) => expr.try_into_dialect(dialect),
            OuterJoin(_expr) => not_yet_implemented!("OUTER JOIN"),
            Overlay {
                expr: _,
                overlay_what: _,
                overlay_from: _,
                overlay_for: _,
            } => unsupported!("OVERLAY"),
            Position { expr: _, r#in: _ } => not_yet_implemented!("POSITION"),
            Prior(_expr) => not_yet_implemented!("PRIOR"),
            RLike {
                negated: _,
                expr: _,
                pattern: _,
                regexp: _,
            } => unsupported!("RLIKE"),
            Rollup(_vec) => unsupported!("GROUP BY ROLLUP"),
            SimilarTo {
                negated: _,
                expr: _,
                pattern: _,
                escape_char: _,
            } => unsupported!("SIMILAR TO"),
            Struct {
                values: _,
                fields: _,
            } => unsupported!("STRUCT"),
            Subquery(query) => Ok(Self::NestedSelect(query.try_into_dialect(dialect)?)),
            Substring {
                expr,
                substring_from,
                substring_for,
                special: false,
                shorthand: _,
            } => Ok(Self::Call(FunctionExpr::Substring {
                string: expr.try_into_dialect(dialect)?,
                pos: substring_from
                    .map(|expr| expr.try_into_dialect(dialect))
                    .transpose()?,
                len: substring_for
                    .map(|expr| expr.try_into_dialect(dialect))
                    .transpose()?,
            })),
            Substring {
                expr,
                substring_from,
                substring_for,
                special: true,
                shorthand: _,
            } => {
                let string: Box<Expr> = expr.try_into_dialect(dialect)?;
                let pos: Option<Box<Expr>> = substring_from
                    .map(|e| e.try_into_dialect(dialect))
                    .transpose()?;
                let len: Option<Box<Expr>> = substring_for
                    .map(|e| e.try_into_dialect(dialect))
                    .transpose()?;
                Ok(Self::Call(FunctionExpr::Substring { string, pos, len }))
            }
            Trim {
                expr: _,
                trim_where: _,
                trim_what: _,
                trim_characters: _,
            } => not_yet_implemented!("TRIM"),
            Tuple(vec) => Ok(Self::Row {
                exprs: vec.try_into_dialect(dialect)?,
                explicit: false, // TODO: Fix upstrem in sqlparser
            }),
            TypedString(_) => unsupported!("TYPED STRING"),
            UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => match expr.try_into_dialect(dialect)? {
                Expr::Literal(Literal::UnsignedInteger(i)) => {
                    let literal = i64::try_from(i)
                        .ok()
                        .and_then(|i| i.checked_neg())
                        .map(Literal::Integer)
                        .unwrap_or_else(|| Literal::Number(format!("-{i}")));
                    Ok(Self::Literal(literal))
                }
                Expr::Literal(Literal::Integer(i)) => Ok(Self::Literal(Literal::Integer(-i))),
                Expr::Literal(Literal::Number(s)) if !s.starts_with('-') => {
                    Ok(Self::Literal(Literal::Number(format!("-{s}"))))
                }
                Expr::Literal(Literal::Number(s)) if s.starts_with('-') => {
                    Ok(Self::Literal(Literal::Number(s[1..].to_string())))
                }
                expr => Ok(Self::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(expr),
                }),
            },
            UnaryOp { op, expr } => Ok(Self::UnaryOp {
                op: op.try_into()?,
                rhs: expr.try_into_dialect(dialect)?,
            }),
            Value(value) => Ok(Self::Literal(value.try_into()?)),
            cfa @ CompoundFieldAccess {
                root: _,
                access_chain: _,
            } => {
                unsupported!("Compound field access a la `foo['bar'].baz[1]`: `{cfa}` = {cfa:?}")
            }
            Wildcard(_token) => unsupported!("wildcard expression in this context"),
            QualifiedWildcard(_object_name, _token) => {
                unsupported!("qualified wildcard expression in this context")
            }
            IsNormalized { .. } => unsupported!("IS NORMALIZED"),
            Prefixed { value, .. } => value.try_into_dialect(dialect),
            MemberOf(_) => unsupported!("MEMBER OF"),
        }
    }
}

impl TryFromDialect<Box<sqlparser::ast::Expr>> for Box<Expr> {
    fn try_from_dialect(
        value: Box<sqlparser::ast::Expr>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        Ok(Box::new(value.try_into_dialect(dialect)?))
    }
}

impl TryFromDialect<Box<sqlparser::ast::Expr>> for Expr {
    fn try_from_dialect(
        value: Box<sqlparser::ast::Expr>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        (*value).try_into_dialect(dialect)
    }
}

/// Convert a sqlparser-rs's `Ident` into a `Expr`; special handling because it might be a variable
/// or a column and sqlparser doesn't distinguish them.
///
/// TODO(mvzink): This may not actually be necessary for recent sqlparser versions: check for usage
/// of `CompoundIdentifier`; also check whether this needs to know the dialect for re-parsing the
/// variable name.
impl TryFromDialect<sqlparser::ast::Ident> for Expr {
    fn try_from_dialect(
        value: sqlparser::ast::Ident,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if dialect == Dialect::MySQL && value.quote_style.is_none() && value.value.starts_with('@')
        {
            Ok(Self::Variable(value.try_into_dialect(dialect)?))
        } else if value.quote_style.is_none()
            && (value.value.starts_with('$') || value.value == "?" || value.value.starts_with(':'))
        {
            Ok(Self::Literal(Literal::Placeholder(
                (&value.value).try_into()?,
            )))
        } else {
            Ok(Self::Column(value.into_dialect(dialect)))
        }
    }
}

impl TryFromDialect<sqlparser::ast::OrderByExpr> for (Expr, OrderType, NullOrder) {
    fn try_from_dialect(
        value: sqlparser::ast::OrderByExpr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let (order_type, null_order) = value.options.try_into_dialect(dialect)?;

        Ok((
            value.expr.try_into_dialect(dialect)?,
            order_type,
            null_order,
        ))
    }
}

impl TryFromDialect<sqlparser::ast::OrderByOptions> for (OrderType, NullOrder) {
    fn try_from_dialect(
        value: sqlparser::ast::OrderByOptions,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let sqlparser::ast::OrderByOptions { asc, nulls_first } = value;

        let order_type = match asc {
            Some(true) | None => OrderType::OrderAscending,
            Some(false) => OrderType::OrderDescending,
        };

        let null_order = nulls_first
            .map(|nf| {
                if nf {
                    NullOrder::NullsFirst
                } else {
                    NullOrder::NullsLast
                }
            })
            .unwrap_or(NullOrder::default_for(dialect, &order_type));

        Ok((order_type, null_order))
    }
}

/// Checks if a schema qualifier is acceptable for a built-in function.
///
/// Built-in functions can be called:
/// - Without schema qualification (e.g., `count(x)`)
/// - In PostgreSQL, with `pg_catalog` schema (e.g., `pg_catalog.count(x)`)
///
/// Returns `true` if the schema is acceptable for a built-in, `false` if the function
/// should be treated as a user-defined function call.
fn is_builtin_schema(schema: &Option<SqlIdentifier>, dialect: Dialect) -> bool {
    match schema {
        None => true,
        Some(s) => dialect == Dialect::PostgreSQL && s.as_str().eq_ignore_ascii_case("pg_catalog"),
    }
}

/// Convert a function call into an expression.
///
/// We don't turn every function into a [`FunctionExpr`], because we have some special cases that
/// turn into other kinds of expressions, such as `DATE(x)` into `CAST(x AS DATE)`.
impl TryFromDialect<sqlparser::ast::Function> for Expr {
    fn try_from_dialect(
        value: sqlparser::ast::Function,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::{
            Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
        };

        // Check for unsupported function features
        let Function {
            args,
            name,
            over,
            uses_odbc_syntax: _, // Cosmetic only, safe to ignore
            parameters,
            filter,
            null_treatment,
            within_group,
        } = value;

        // Parameters: Named args, JSON clauses, etc. (ClickHouse, SQL Server, PostgreSQL)
        if parameters != FunctionArguments::None {
            unsupported!("Function with parameters")?;
        }

        // FILTER clause: WHERE condition in aggregates (PostgreSQL, SQL Standard)
        if filter.is_some() {
            unsupported!("Function with FILTER clause")?;
        }

        // NULL treatment: IGNORE NULLS / RESPECT NULLS (PostgreSQL 19+, SQL Server, Oracle)
        if null_treatment == Some(sqlparser::ast::NullTreatment::IgnoreNulls) {
            unsupported!("Function with IGNORE NULLS")?;
        }
        // RESPECT NULLS is safe to ignore (default behavior)

        // WITHIN GROUP: Ordered-set aggregates (PostgreSQL, SQL Standard)
        if !within_group.is_empty() {
            unsupported!("Function with WITHIN GROUP clause")?;
        }

        // Function names can have at most 2 parts: schema.name or just name.
        let mut ident_iter = name.0.into_iter().map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Ok(ident),
            sqlparser::ast::ObjectNamePart::Function(_) => {
                unsupported!("identifier constructor in function name")
            }
        });

        let first = match ident_iter.next() {
            Some(result) => result?,
            None => return failed!("Function name cannot be empty"),
        };

        let (schema, mut ident) = match ident_iter.next() {
            None => (None, first),
            Some(second) => {
                let second = second?;
                if ident_iter.next().is_some() {
                    unsupported!(
                        "Function names with more than 2 parts not supported (max 2: schema.name)"
                    )?;
                }
                (Some(first.into_dialect(dialect)), second)
            }
        };

        // Special case for `COUNT(*)`
        if is_builtin_schema(&schema, dialect)
            && ident.value.eq_ignore_ascii_case("COUNT")
            && matches!(
                args,
                FunctionArguments::List(FunctionArgumentList { ref args, .. })
                    if args.len() == 1
                        && matches!(&args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
            )
        {
            if let Some(window) = over {
                return sqlparser_window_to_window_function(
                    window,
                    dialect,
                    Expr::Call(FunctionExpr::CountStar),
                );
            }

            return Ok(Expr::Call(FunctionExpr::CountStar));
        };

        let (args, distinct, clauses) = match args {
            sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                args,
                duplicate_treatment,
                clauses,
            }) => (
                args,
                duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct),
                clauses,
            ),
            sqlparser::ast::FunctionArguments::None => {
                // Functions without arguments like `CURRENT_TIMESTAMP` are always built-ins.
                // Schema-qualified functions always require arguments (e.g., `myschema.func`
                // without parens would be parsed as a column reference, not a function call).
                if schema.is_some() {
                    failed!("Schema-qualified function calls require parentheses")?;
                }
                ident.value.make_ascii_lowercase();
                let func = match ident.value.as_str() {
                    "current_date" => FunctionExpr::CurrentDate,
                    "current_timestamp" => FunctionExpr::CurrentTimestamp(None),
                    "current_time" => FunctionExpr::CurrentTime,
                    "localtimestamp" => FunctionExpr::LocalTimestamp,
                    "localtime" => FunctionExpr::LocalTime,
                    "current_user" => FunctionExpr::CurrentUser,
                    "session_user" => FunctionExpr::SessionUser,
                    "current_catalog" => FunctionExpr::CurrentCatalog,
                    "user" => FunctionExpr::SqlUser,
                    _ => {
                        return unsupported!(
                            "No-parentheses function '{}' is not supported",
                            ident.value
                        );
                    }
                };
                return Ok(Self::Call(func));
            }
            sqlparser::ast::FunctionArguments::Subquery(query) => {
                if ident.value.eq_ignore_ascii_case("ARRAY") {
                    let select = query.try_into_dialect(dialect)?;
                    return Ok(Expr::Array(ArrayArguments::Subquery(select)));
                }

                return not_yet_implemented!(
                    "subquery function call argument for {ident}: Subquery<{query}>"
                );
            }
        };

        let find_separator = || {
            clauses.iter().find_map(|clause| match clause {
                sqlparser::ast::FunctionArgumentClause::Separator(separator) => {
                    Some(sqlparser_value_into_string(separator.value.clone()))
                }
                _ => None,
            })
        };

        let order_by_clause = || -> Option<OrderClause> {
            clauses.iter().find_map(|clause| match clause {
                sqlparser::ast::FunctionArgumentClause::OrderBy(o) => {
                    let order_by: Result<Vec<OrderBy>, _> = o
                        .iter()
                        .map(|expr| expr.clone().try_into_dialect(dialect))
                        .collect();
                    order_by.ok().map(|order_by| OrderClause { order_by })
                }
                _ => None,
            })
        };

        let mut exprs = args.into_iter().map(|arg| arg.try_into_dialect(dialect));
        let mut next_expr = || {
            exprs
                .next()
                .ok_or_else(|| failed_err!("not enough arguments for {ident}"))?
                .map(Box::new)
        };

        let is_builtin = is_builtin_schema(&schema, dialect);

        let expr = if is_builtin && ident.value.eq_ignore_ascii_case("AVG") {
            Self::Call(FunctionExpr::Avg {
                expr: next_expr()?,
                distinct,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("COUNT") {
            Self::Call(FunctionExpr::Count {
                expr: next_expr()?,
                distinct,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("DATE") {
            // TODO: Arguably, this should be in a SQL rewrite pass to preserve input when rendering
            Self::Cast {
                expr: next_expr()?,
                ty: crate::ast::SqlType::Date,
                style: CastStyle::As,
                array: false,
            }
        } else if is_builtin && ident.value.eq_ignore_ascii_case("EXTRACT") {
            return failed!("{ident} should have been converted earlier");
        } else if is_builtin && ident.value.eq_ignore_ascii_case("GROUP_CONCAT") {
            // group_concat() is a mysql-specific function, and caller optionally sets
            // the output separator with special `SEPARATOR ''` syntax. we fish that value
            // out of the `clauses` list above.
            let order_by = order_by_clause();
            Self::Call(FunctionExpr::GroupConcat {
                expr: next_expr()?,
                separator: find_separator(),
                distinct: distinct.into(),
                order_by,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("STRING_AGG") {
            // `string_agg()` is a pg-specific function, and we get the mandatory separator
            // from the second parameter to the function.
            let expr = next_expr()?;
            let sep = next_expr()?;
            let separator = match *sep {
                Expr::Literal(Literal::String(s)) => Some(s),
                Expr::Literal(Literal::Null) => None,
                s => return unsupported!("Unsupported separator: {:?}", s),
            };
            let order_by = order_by_clause();

            Self::Call(FunctionExpr::StringAgg {
                expr,
                separator,
                distinct: distinct.into(),
                order_by,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("ARRAY_AGG") {
            let order_by = order_by_clause();
            Self::Call(FunctionExpr::ArrayAgg {
                expr: next_expr()?,
                distinct: distinct.into(),
                order_by,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("JSON_OBJECT_AGG") {
            Self::Call(FunctionExpr::JsonObjectAgg {
                key: next_expr()?,
                value: next_expr()?,
                allow_duplicate_keys: true,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("BUCKET") {
            Self::Call(FunctionExpr::Bucket {
                expr: next_expr()?,
                interval: next_expr()?,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("ROW_NUMBER") {
            Self::Call(FunctionExpr::RowNumber)
        } else if is_builtin && ident.value.eq_ignore_ascii_case("RANK") {
            Self::Call(FunctionExpr::Rank)
        } else if is_builtin && ident.value.eq_ignore_ascii_case("DENSE_RANK") {
            Self::Call(FunctionExpr::DenseRank)
        } else if is_builtin
            && (ident.value.eq_ignore_ascii_case("JSONB_OBJECT_AGG")
                || ident.value.eq_ignore_ascii_case("JSON_OBJECTAGG"))
        {
            Self::Call(FunctionExpr::JsonObjectAgg {
                key: next_expr()?,
                value: next_expr()?,
                allow_duplicate_keys: false,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("LOWER") {
            let expr = next_expr()?;
            match *expr {
                Self::Collate { expr, collation } => Self::Call(FunctionExpr::Lower {
                    expr,
                    collation: Some(collation),
                }),
                _ => Self::Call(FunctionExpr::Lower {
                    expr,
                    collation: None,
                }),
            }
        } else if is_builtin && ident.value.eq_ignore_ascii_case("MAX") {
            Self::Call(FunctionExpr::Max(next_expr()?))
        } else if is_builtin && ident.value.eq_ignore_ascii_case("MIN") {
            Self::Call(FunctionExpr::Min(next_expr()?))
        } else if is_builtin && ident.value.eq_ignore_ascii_case("ROW") {
            Self::Row {
                explicit: true,
                exprs: exprs.by_ref().collect::<Result<_, _>>()?,
            }
        } else if is_builtin && ident.value.eq_ignore_ascii_case("SUM") {
            Self::Call(FunctionExpr::Sum {
                expr: next_expr()?,
                distinct,
            })
        } else if is_builtin && ident.value.eq_ignore_ascii_case("MOD") {
            let lhs = next_expr()?;
            let rhs = next_expr()?;
            Self::BinaryOp {
                lhs,
                op: BinaryOperator::Modulo,
                rhs,
            }
        } else if is_builtin && ident.value.eq_ignore_ascii_case("UPPER") {
            let expr = next_expr()?;
            match *expr {
                Self::Collate { expr, collation } => Self::Call(FunctionExpr::Upper {
                    expr,
                    collation: Some(collation),
                }),
                _ => Self::Call(FunctionExpr::Upper {
                    expr,
                    collation: None,
                }),
            }
        } else if is_builtin {
            // Built-in function — map to specific variants where possible
            ident.value.make_ascii_lowercase();
            let all_args: Vec<Expr> = exprs.by_ref().collect::<Result<_, _>>()?;
            map_builtin_to_function_expr(&ident.value, all_args)
        } else {
            Self::Call(FunctionExpr::Udf {
                schema,
                name: ident.clone().into_dialect(dialect),
                arguments: exprs.by_ref().collect::<Result<_, _>>()?,
            })
        };

        if exprs.len() != 0 {
            return failed!("too many arguments for function '{ident}'");
        }

        if let Some(window) = over {
            sqlparser_window_to_window_function(window, dialect, expr)
        } else {
            Ok(expr)
        }
    }
}

/// Maps a built-in function name and arguments to a specific `FunctionExpr` variant.
/// Returns an error for unrecognized function names.
fn map_builtin_to_function_expr(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Call(FunctionExpr::from_name_and_args(name, args))
}

fn sqlparser_window_to_window_function(
    window: sqlparser::ast::WindowType,
    dialect: Dialect,
    expr: Expr,
) -> Result<Expr, AstConversionError> {
    use sqlparser::ast::WindowSpec;
    use sqlparser::ast::WindowType;

    match window {
        WindowType::NamedWindow(_) => unsupported!("named window"),
        WindowType::WindowSpec(WindowSpec {
            partition_by,
            order_by,
            window_name,
            window_frame,
        }) => {
            if window_name.is_some() {
                return unsupported!("Window name in Window Spec");
            }

            if window_frame.is_some() {
                return unsupported!("Window frame in Window Spec");
            }

            let partition_by: Vec<Expr> = partition_by
                .into_iter()
                .map(|p| p.try_into_dialect(dialect))
                .try_collect()?;

            let order_by: Vec<(Expr, OrderType, NullOrder)> = order_by
                .into_iter()
                .map(|o| o.try_into_dialect(dialect))
                .try_collect()?;

            match expr {
                Expr::Call(
                    f @ FunctionExpr::CountStar
                    | f @ FunctionExpr::RowNumber
                    | f @ FunctionExpr::Rank
                    | f @ FunctionExpr::DenseRank
                    | f @ FunctionExpr::Max(_)
                    | f @ FunctionExpr::Min(_)
                    // TODO: We probably can support distinct aggregates
                    // given that we do have access to the entire window
                    | f @ FunctionExpr::Sum {
                        distinct: false, ..
                    }
                    | f @ FunctionExpr::Avg {
                        distinct: false, ..
                    }
                    | f @ FunctionExpr::Count {
                        distinct: false, ..
                    },
                ) => Ok(Expr::WindowFunction {
                    function: f,
                    partition_by,
                    order_by,
                }),
                _ => {
                    unsupported!("{expr:?} is not supported as a window function")
                }
            }
        }
    }
}

fn sqlparser_value_into_string(value: sqlparser::ast::Value) -> String {
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
        | DoubleQuotedString(s)
        | HexStringLiteral(s)
        | Placeholder(s) => s,
        QuoteDelimitedStringLiteral(sqlparser::ast::QuoteDelimitedString { value: s, .. })
        | NationalQuoteDelimitedStringLiteral(sqlparser::ast::QuoteDelimitedString {
            value: s,
            ..
        }) => s,
        Boolean(b) => b.to_string(),
        Null => "NULL".to_string(),
    }
}

impl TryFromDialect<sqlparser::ast::FunctionArg> for Expr {
    fn try_from_dialect(
        value: sqlparser::ast::FunctionArg,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::FunctionArg::*;
        match value {
            Named { arg, .. } | ExprNamed { arg, .. } | Unnamed(arg) => {
                arg.try_into_dialect(dialect)
            }
        }
    }
}

impl TryFromDialect<sqlparser::ast::FunctionArgExpr> for Expr {
    fn try_from_dialect(
        value: sqlparser::ast::FunctionArgExpr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::FunctionArgExpr::*;
        match value {
            Expr(expr) => expr.try_into_dialect(dialect),
            QualifiedWildcard(object_name) => {
                Ok(Self::Column(object_name.try_into_dialect(dialect)?))
            }
            Wildcard => not_yet_implemented!("wildcard expression in function argument"),
            WildcardWithOptions(_) => {
                unsupported!("wildcard with options in function argument is not supported")
            }
        }
    }
}

impl DialectDisplay for Expr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Expr::Call(fe) => write!(f, "{}", fe.display(dialect)),
            Expr::Literal(l) => write!(f, "{}", l.display(dialect)),
            Expr::Column(col) => write!(f, "{}", col.display(dialect)),
            Expr::CaseWhen {
                branches,
                else_expr,
            } => {
                write!(f, "CASE ")?;
                for branch in branches {
                    write!(f, "{} ", branch.display(dialect))?;
                }
                if let Some(else_expr) = else_expr {
                    write!(f, "ELSE {} ", else_expr.display(dialect))?;
                }
                write!(f, "END")
            }
            Expr::BinaryOp { lhs, op, rhs } => write!(
                f,
                "({} {op} {})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::OpAny { lhs, op, rhs } => write!(
                f,
                "{} {op} ANY ({})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::OpSome { lhs, op, rhs } => write!(
                f,
                "{} {op} SOME ({})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::OpAll { lhs, op, rhs } => write!(
                f,
                "{} {op} ALL ({})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs,
            } => write!(f, "(-{})", rhs.display(dialect)),
            Expr::UnaryOp { op, rhs } => write!(f, "({op} {})", rhs.display(dialect)),
            Expr::Exists(statement) => write!(f, "EXISTS ({})", statement.display(dialect)),

            Expr::Between {
                operand,
                min,
                max,
                negated,
            } => {
                write!(
                    f,
                    "{} {}BETWEEN {} AND {}",
                    operand.display(dialect),
                    if *negated { "NOT " } else { "" },
                    min.display(dialect),
                    max.display(dialect)
                )
            }
            Expr::In { lhs, rhs, negated } => {
                write!(f, "{}", lhs.display(dialect))?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN ({})", rhs.display(dialect))
            }
            Expr::NestedSelect(q) => write!(f, "({})", q.display(dialect)),
            Expr::Cast {
                expr,
                ty,
                style,
                array,
            } => {
                let expr = expr.display(dialect);
                let ty = ty.display(dialect);
                match style {
                    CastStyle::As => {
                        let array_suffix = if *array { " ARRAY" } else { "" };
                        write!(f, "CAST({expr} as {ty}{array_suffix})")
                    }
                    CastStyle::Convert => {
                        debug_assert!(!array, "ARRAY syntax is only valid with CAST, not CONVERT");
                        write!(f, "CONVERT({expr}, {ty})")
                    }
                    CastStyle::DoubleColon => {
                        debug_assert!(!array, "ARRAY syntax is only valid with CAST, not ::");
                        write!(f, "({expr}::{ty})")
                    }
                }
            }
            Expr::ConvertUsing { expr, charset } => {
                let expr = expr.display(dialect);
                write!(f, "CONVERT({expr} USING {charset})")
            }
            Expr::Array(args) => {
                fn write_value(
                    expr: &Expr,
                    dialect: Dialect,
                    f: &mut fmt::Formatter,
                ) -> fmt::Result {
                    match expr {
                        Expr::Array(args) => match args {
                            ArrayArguments::List(elems) => {
                                write!(f, "[")?;
                                for (i, elem) in elems.iter().enumerate() {
                                    if i != 0 {
                                        write!(f, ",")?;
                                    }
                                    write_value(elem, dialect, f)?;
                                }
                                write!(f, "]")
                            }
                            ArrayArguments::Subquery(..) => {
                                unreachable!("can't have a subquery in lieu of a lower dimension")
                            }
                        },
                        _ => write!(f, "{}", expr.display(dialect)),
                    }
                }

                match args {
                    ArrayArguments::List(exprs) => {
                        write!(f, "ARRAY[")?;
                        for (i, expr) in exprs.iter().enumerate() {
                            if i != 0 {
                                write!(f, ",")?;
                            }
                            write_value(expr, dialect, f)?;
                        }
                        write!(f, "]")
                    }
                    ArrayArguments::Subquery(query) => {
                        write!(f, "ARRAY ({})", query.display(dialect))
                    }
                }
            }
            Expr::Row { explicit, exprs } => {
                if *explicit {
                    write!(f, "ROW")?;
                }
                write!(f, "(")?;
                for (i, expr) in exprs.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", expr.display(dialect))?;
                }
                write!(f, ")")
            }
            Expr::Variable(var) => write!(f, "{}", var.display(dialect)),
            Expr::Collate { expr, collation } => {
                write!(f, "{} COLLATE {}", expr.display(dialect), collation)
            }
            Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } => {
                write!(f, "{} OVER(", function.display(dialect))?;

                let mut ws_sep = false;

                if !partition_by.is_empty() {
                    write!(f, "PARTITION BY {}", partition_by.display(dialect))?;
                    ws_sep = true;
                }

                if !order_by.is_empty() {
                    if ws_sep {
                        write!(f, " ")?;
                    }

                    write!(f, "ORDER BY ")?;
                    for (i, (e, o, no)) in order_by.iter().enumerate() {
                        if i != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{} {o}", e.display(dialect))?;

                        // MySQL doesn't support explicit NULLS FIRST|LAST
                        if !matches!(dialect, Dialect::MySQL) {
                            write!(f, " {no}")?;
                        }
                    }
                }

                write!(f, ")")?;
                Ok(())
            }
        })
    }
}

impl Expr {
    /// If this expression is a [binary operator application](Expr::BinaryOp), returns a tuple
    /// of the left-hand side, the operator, and the right-hand side, otherwise returns None
    pub fn as_binary_op(&self) -> Option<(&Expr, BinaryOperator, &Expr)> {
        match self {
            Expr::BinaryOp { lhs, op, rhs } => Some((lhs.as_ref(), *op, rhs.as_ref())),
            _ => None,
        }
    }

    /// Returns true if any variables are present in the expression
    pub fn contains_vars(&self) -> bool {
        match self {
            Expr::Variable(_) => true,
            _ => self.recursive_subexpressions().any(Self::contains_vars),
        }
    }

    /// Functions similarly to mem::take, replacing the argument with a meaningless placeholder and
    /// returning the value from the method, thus providing a way to move ownership more easily.
    pub fn take(&mut self) -> Self {
        // If Expr implemented Default we could use mem::take directly for this purpose, but we
        // decided that it felt semantically weird and arbitrary to have a Default implementation
        // for Expr that returned a null literal, since there isn't really such a thing as a
        // "default" expression.
        mem::replace(self, Expr::Literal(Literal::Null))
    }
}

impl Arbitrary for Expr {
    type Parameters = Option<Dialect>;

    type Strategy = BoxedStrategy<Expr>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        use proptest::option;
        use proptest::prelude::*;

        prop_oneof![
            any::<Literal>().prop_map(Expr::Literal),
            any::<Column>().prop_map(Expr::Column),
            any::<Variable>().prop_map(Expr::Variable),
        ]
        .prop_recursive(4, 8, 4, move |element| {
            let box_expr = element.clone().prop_map(Box::new);
            let call = prop_oneof![
                (box_expr.clone(), any::<bool>())
                    .prop_map(|(expr, distinct)| FunctionExpr::Avg { expr, distinct }),
                (box_expr.clone(), any::<bool>())
                    .prop_map(|(expr, distinct)| FunctionExpr::Count { expr, distinct }),
                Just(FunctionExpr::CountStar),
                (box_expr.clone(), any::<bool>())
                    .prop_map(|(expr, distinct)| FunctionExpr::Sum { expr, distinct }),
                (box_expr.clone(), any::<TimestampField>())
                    .prop_map(|(expr, field)| FunctionExpr::Extract { expr, field }),
                box_expr.clone().prop_map(FunctionExpr::Max),
                box_expr.clone().prop_map(FunctionExpr::Min),
                (box_expr.clone(), any::<Option<String>>(), any::<bool>()).prop_map(
                    |(expr, separator, distinct)| {
                        FunctionExpr::GroupConcat {
                            expr,
                            separator,
                            distinct: distinct.into(),
                            order_by: None,
                        }
                    }
                ),
                (box_expr.clone(), any::<Option<String>>(), any::<bool>()).prop_map(
                    |(expr, separator, distinct)| {
                        FunctionExpr::StringAgg {
                            expr,
                            separator,
                            distinct: distinct.into(),
                            order_by: None,
                        }
                    }
                ),
                (box_expr.clone(), any::<bool>()).prop_map(|(expr, distinct)| {
                    FunctionExpr::ArrayAgg {
                        expr,
                        distinct: distinct.into(),
                        order_by: None,
                    }
                }),
                (
                    box_expr.clone(),
                    option::of(box_expr.clone()),
                    option::of(box_expr.clone())
                )
                    .prop_map(|(string, pos, len)| {
                        FunctionExpr::Substring { string, pos, len }
                    }),
                proptest::collection::vec(element.clone(), 1..6).prop_map(FunctionExpr::Coalesce),
                box_expr.clone().prop_map(FunctionExpr::Length),
                (
                    proptest::option::of(any::<SqlIdentifier>()),
                    any::<SqlIdentifier>(),
                    proptest::collection::vec(element.clone(), 0..24)
                )
                    .prop_map(|(schema, name, arguments)| FunctionExpr::Udf {
                        schema,
                        name,
                        arguments,
                    })
            ]
            .prop_map(Expr::Call)
            .boxed();
            let case_when = (
                proptest::collection::vec(
                    (element.clone(), element.clone())
                        .prop_map(|(condition, body)| CaseWhenBranch { condition, body }),
                    1..24,
                ),
                option::of(box_expr.clone()),
            )
                .prop_map(|(branches, else_expr)| Expr::CaseWhen {
                    branches,
                    else_expr,
                });
            let base = call
                .clone()
                .prop_union(
                    (any::<UnaryOperator>(), box_expr.clone())
                        .prop_map(|(op, rhs)| Expr::UnaryOp { op, rhs })
                        .boxed(),
                )
                .or(case_when.clone().boxed())
                .or((
                    // FIXME(mvzink): This should be switched back to `box_expr` to test
                    // recursive/nested expressions left of `BETWEEN` once we are no longer testing
                    // `nom-sql`, which doesn't support all expressions in that position.
                    prop_oneof![
                        any::<Literal>().prop_map(Expr::Literal),
                        any::<Column>().prop_map(Expr::Column),
                        call.clone(),
                        case_when.clone(),
                    ],
                    box_expr.clone(),
                    box_expr.clone(),
                    any::<bool>(),
                )
                    .prop_map(|(operand, min, max, negated)| Expr::Between {
                        operand: Box::new(operand),
                        min,
                        max,
                        negated,
                    })
                    .boxed())
                .or((
                    box_expr.clone(),
                    /* TODO: IN (subquery) */
                    proptest::collection::vec(element.clone(), 1..24).prop_map(InValue::List),
                    any::<bool>(),
                )
                    .prop_map(|(lhs, rhs, negated)| Expr::In { lhs, rhs, negated })
                    .boxed())
                .or((
                    box_expr.clone(),
                    any_with::<SqlType>(SqlTypeArbitraryOptions {
                        generate_unsupported: true,
                        dialect: params.unwrap_or(Dialect::MySQL),
                        ..Default::default()
                    }),
                    any_with::<CastStyle>(params),
                    any::<bool>(),
                )
                    .prop_map(move |(expr, ty, style, array)| {
                        // ARRAY syntax is MySQL-specific and only valid with CastStyle::As
                        let array =
                            array && params != Some(Dialect::PostgreSQL) && style == CastStyle::As;
                        Expr::Cast {
                            expr,
                            ty,
                            style,
                            array,
                        }
                    })
                    .boxed())
                .or(proptest::collection::vec(element, 0..24)
                    .prop_map(|exprs| Expr::Array(ArrayArguments::List(exprs)))
                    .boxed());
            // TODO: once we have Arbitrary for SelectStatement
            // any::<Box<SelectStatement>>().prop_map(Expr::NestedSelect),
            // any::<Box<SelectStatement>>().prop_map(Expr::Exists),
            // any::<Box<SelectStatement>>().prop_map(Expr::Array(ArrayArguments::Subquery)),
            if params == Some(Dialect::PostgreSQL) {
                base.or(prop_oneof![
                    (
                        box_expr.clone(),
                        any_with::<BinaryOperator>(BinaryOperatorParameters {
                            for_op_all_any: true
                        }),
                        box_expr.clone(),
                    )
                        .prop_map(|(lhs, op, rhs)| Expr::BinaryOp {
                            lhs,
                            op,
                            rhs
                        },),
                    (
                        box_expr.clone(),
                        any_with::<BinaryOperator>(BinaryOperatorParameters {
                            for_op_all_any: true
                        }),
                        box_expr.clone(),
                    )
                        .prop_map(|(lhs, op, rhs)| Expr::OpAny {
                            lhs,
                            op,
                            rhs
                        },),
                    (
                        box_expr.clone(),
                        any_with::<BinaryOperator>(BinaryOperatorParameters {
                            for_op_all_any: true
                        }),
                        box_expr.clone(),
                    )
                        .prop_map(|(lhs, op, rhs)| Expr::OpSome {
                            lhs,
                            op,
                            rhs
                        },),
                    (
                        box_expr.clone(),
                        any_with::<BinaryOperator>(BinaryOperatorParameters {
                            for_op_all_any: true
                        }),
                        box_expr.clone(),
                    )
                        .prop_map(|(lhs, op, rhs)| Expr::OpAll {
                            lhs,
                            op,
                            rhs
                        },),
                ]
                .boxed())
            } else {
                base
            }
        })
        .boxed()
    }
}

/// Suffixes which can be supplied to operators to convert them into predicates on arrays or
/// subqueries.
///
/// Used for support of `<expr> <op> ANY ...`, `<expr> <op> SOME ...`, and `<expr> <op> ALL ...`
#[derive(Debug, Clone, Copy)]
pub enum OperatorSuffix {
    Any,
    Some,
    All,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Dialect, TryIntoDialect};

    /// Helper to parse a SQL expression string via sqlparser and convert it to the readyset
    /// [`Expr`] type.
    fn parse_expr(dialect: Dialect, input: &str) -> Expr {
        let sqlparser_dialect: Box<dyn sqlparser::dialect::Dialect> = match dialect {
            Dialect::PostgreSQL => Box::new(sqlparser::dialect::PostgreSqlDialect {}),
            Dialect::MySQL => Box::new(sqlparser::dialect::MySqlDialect {}),
        };
        let mut parser = sqlparser::parser::Parser::new(sqlparser_dialect.as_ref())
            .try_with_sql(input)
            .expect("failed to create sqlparser parser");
        let sqlparser_expr = parser.parse_expr().expect("failed to parse expression");
        sqlparser_expr
            .try_into_dialect(dialect)
            .expect("failed to convert sqlparser Expr to readyset Expr")
    }

    /// Test that MOD(x, y) function call desugars to a BinaryOp with Modulo operator.
    #[test]
    fn mod_function_desugars_to_binary_op() {
        let expr = parse_expr(Dialect::MySQL, "MOD(x, y)");

        match expr {
            Expr::BinaryOp { lhs, op, rhs } => {
                assert_eq!(op, BinaryOperator::Modulo);
                assert!(
                    matches!(lhs.as_ref(), Expr::Column(col) if col.name == "x"),
                    "expected lhs to be column 'x', got {:?}",
                    lhs
                );
                assert!(
                    matches!(rhs.as_ref(), Expr::Column(col) if col.name == "y"),
                    "expected rhs to be column 'y', got {:?}",
                    rhs
                );
            }
            other => panic!(
                "expected Expr::BinaryOp with Modulo operator, got {:?}",
                other
            ),
        }
    }

    /// Test that the `%` operator binds tighter than `+`, i.e. `1 + 2 % 3` parses as
    /// `1 + (2 % 3)`. The outer expression should be Add with the RHS being a Modulo expression.
    #[test]
    fn modulo_operator_precedence() {
        let expr = parse_expr(Dialect::MySQL, "1 + 2 % 3");

        match expr {
            Expr::BinaryOp { lhs, op, rhs } => {
                assert_eq!(
                    op,
                    BinaryOperator::Add,
                    "outer operator should be Add, got {:?}",
                    op
                );
                assert!(
                    matches!(lhs.as_ref(), Expr::Literal(Literal::Integer(1))),
                    "expected lhs to be literal 1, got {:?}",
                    lhs
                );
                match rhs.as_ref() {
                    Expr::BinaryOp {
                        lhs: inner_lhs,
                        op: inner_op,
                        rhs: inner_rhs,
                    } => {
                        assert_eq!(
                            inner_op,
                            &BinaryOperator::Modulo,
                            "inner operator should be Modulo, got {:?}",
                            inner_op
                        );
                        assert!(
                            matches!(inner_lhs.as_ref(), Expr::Literal(Literal::Integer(2))),
                            "expected inner lhs to be literal 2, got {:?}",
                            inner_lhs
                        );
                        assert!(
                            matches!(inner_rhs.as_ref(), Expr::Literal(Literal::Integer(3))),
                            "expected inner rhs to be literal 3, got {:?}",
                            inner_rhs
                        );
                    }
                    other => panic!(
                        "expected rhs to be Expr::BinaryOp with Modulo, got {:?}",
                        other
                    ),
                }
            }
            other => panic!("expected outer Expr::BinaryOp with Add, got {:?}", other),
        }
    }

    /// Guard against `FunctionExpr` enum size regressions.
    ///
    /// The `Udf` variant dominates enum size due to its three heap-allocated fields
    /// (`Option<SqlIdentifier>`, `SqlIdentifier`, `Vec<Expr>`). The ideal target is ≤ 48 bytes,
    /// achievable by boxing `Udf` into `Udf(Box<UdfCall>)`. This test uses a conservative
    /// bound to catch further regressions while the boxing refactor is pending.
    #[test]
    fn function_expr_size() {
        use std::mem::size_of;
        assert!(
            size_of::<FunctionExpr>() <= 72,
            "FunctionExpr grew to {} bytes — consider boxing the Udf variant to reduce size",
            size_of::<FunctionExpr>()
        );
    }
}
