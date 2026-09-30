//! The Postgres built-in `path` type, which a [`DfValue`](crate::DfValue) holds as text.

use std::error::Error;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use geo_types::Point;
use itertools::Itertools;
use postgres_protocol::types;
use readyset_errors::{ReadySetError, ReadySetResult};
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

use crate::point::try_parse_point;

/// A Postgres `path`, which unlike a [`geo_types::LineString`] records whether it is closed.
#[derive(Clone, Debug, PartialEq)]
pub struct PostgresPath {
    pub closed: bool,
    pub points: Box<[Point]>,
}

impl<'a> FromSql<'a> for PostgresPath {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let path = types::path_from_sql(raw)?;
        let mut points = Vec::with_capacity(raw.len().saturating_sub(5) / 16);
        let mut iter = path.points();
        while let Some(p) = iter.next()? {
            points.push(Point::new(p.x(), p.y()));
        }
        Ok(Self {
            closed: path.closed(),
            points: points.into(),
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PATH
    }
}

impl ToSql for PostgresPath {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::path_to_sql(self.closed, self.points.iter().map(|p| (p.x(), p.y())), out)?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PATH
    }

    to_sql_checked!();
}

/// Parses a Postgres `path` in any text form Postgres accepts: `[(x1,y1),...]` is open, while
/// `((x1,y1),...)`, `(x1,y1),...` and `x1,y1,...` are closed.
pub fn parse_path(s: &str) -> ReadySetResult<PostgresPath> {
    let trimmed = s.trim();
    let (closed, inner) = match trimmed.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        Some(inner) => (false, inner),
        None => (true, strip_enclosing_parens(trimmed)),
    };
    parse_points(inner)
        .map(|points| PostgresPath {
            closed,
            points: points.into(),
        })
        .ok_or_else(|| ReadySetError::DfValueConversionError {
            src_type: "text".into(),
            target_type: "path".into(),
            details: format!("invalid path value: {s:?}"),
        })
}

/// Strips parentheses that enclose all of `s`, as in `((1,2),(3,4))` but not `(1,2),(3,4)`.
fn strip_enclosing_parens(s: &str) -> &str {
    let Some(inner) = s.strip_prefix('(') else {
        return s;
    };
    let mut depth = 0u32;
    let close = inner.bytes().position(|b| match b {
        b'(' => {
            depth += 1;
            false
        }
        b')' => match depth.checked_sub(1) {
            Some(d) => {
                depth = d;
                false
            }
            None => true,
        },
        _ => false,
    });
    match close {
        Some(i) if i == inner.len() - 1 => &inner[..i],
        Some(_) if !inner.contains('(') => inner,
        _ => s,
    }
}

/// Parses a comma-separated list of points, each either `(x,y)` or a bare `x,y` pair.
fn parse_points(s: &str) -> Option<Vec<Point>> {
    let mut points = Vec::new();
    let mut x = None;
    let mut depth = 0u32;
    let mut start = 0;
    for (i, c) in s.char_indices().chain([(s.len(), ',')]) {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                let item = s[start..i].trim();
                start = i + 1;
                if item.starts_with('(') {
                    if x.is_some() {
                        return None;
                    }
                    points.push(try_parse_point(item)?);
                } else {
                    let v = item.parse::<f64>().ok()?;
                    match x.take() {
                        Some(x) => points.push(Point::new(x, v)),
                        None => x = Some(v),
                    }
                }
            }
            _ => {}
        }
    }
    (depth == 0 && x.is_none() && !points.is_empty()).then_some(points)
}

/// Formats a path as Postgres writes it: `[(x1,y1),...]` when open, `((x1,y1),...)` when closed.
pub fn format_path(path: &PostgresPath) -> String {
    let (open, close) = if path.closed { ('(', ')') } else { ('[', ']') };
    let points = path
        .points
        .iter()
        .format_with(",", |p, f| f(&format_args!("({},{})", p.x(), p.y())));
    format!("{open}{points}{close}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfValue;

    fn path(closed: bool, points: &[(f64, f64)]) -> PostgresPath {
        PostgresPath {
            closed,
            points: points.iter().map(|&(x, y)| Point::new(x, y)).collect(),
        }
    }

    #[test]
    fn parse_accepts_postgres_forms() {
        let pts = [(1.0, 2.0), (3.0, -4.0)];
        for (s, closed) in [
            ("[(1,2),(3,-4)]", false),
            (" [ ( 1 , 2 ) , ( 3 , -4 ) ] ", false),
            ("[1,2,3,-4]", false),
            ("((1,2),(3,-4))", true),
            ("(1,2),(3,-4)", true),
            ("(1,2,3,-4)", true),
            ("1,2,3,-4", true),
            ("1,2,(3,-4)", true),
        ] {
            assert_eq!(parse_path(s).unwrap(), path(closed, &pts), "{s:?}");
        }
    }

    #[test]
    fn parse_accepts_single_point() {
        assert_eq!(parse_path("[(1,2)]").unwrap(), path(false, &[(1.0, 2.0)]));
        assert_eq!(parse_path("(1,2)").unwrap(), path(true, &[(1.0, 2.0)]));
    }

    #[test]
    fn parse_rejects_malformed_input() {
        for s in [
            "",
            "[]",
            "()",
            "1,2,3",
            "[(1,2),(3,4)",
            "((1,2),(3,4)",
            "(1,2),3",
            "1,(2,3),4",
            "(1,2),,(3,4)",
            "[(a,2)]",
            "(1,2),3,4",
        ] {
            assert!(parse_path(s).is_err(), "{s:?}");
        }
    }

    #[test]
    fn format_round_trips() {
        for p in [
            path(false, &[(1.5, -2.0), (0.1, 1e300)]),
            path(true, &[(-0.0, f64::MIN_POSITIVE), (3.0, 4.0), (5.0, 6.0)]),
            path(true, &[(f64::INFINITY, f64::NEG_INFINITY)]),
        ] {
            assert_eq!(parse_path(&format_path(&p)).unwrap(), p);
        }
    }

    #[test]
    fn format_brackets_open_and_parenthesizes_closed() {
        assert_eq!(
            format_path(&path(false, &[(1.0, 2.0), (3.0, 4.0)])),
            "[(1,2),(3,4)]"
        );
        assert_eq!(
            format_path(&path(true, &[(1.0, 2.0), (3.0, 4.0)])),
            "((1,2),(3,4))"
        );
    }

    #[test]
    fn df_value_binary_round_trip_keeps_closed() {
        for s in ["[(1.5,-2),(3,4)]", "((1.5,-2),(3,4))"] {
            let mut buf = BytesMut::new();
            DfValue::from(s).to_sql(&Type::PATH, &mut buf).unwrap();
            assert_eq!(
                DfValue::from_sql(&Type::PATH, &buf).unwrap(),
                DfValue::from(s)
            );
        }
    }

    #[test]
    fn from_sql_rejects_point_count_not_matching_data() {
        for count in [-1i32, 2, i32::MAX] {
            let mut raw = vec![1u8];
            raw.extend_from_slice(&count.to_be_bytes());
            raw.extend_from_slice(&1.0f64.to_be_bytes());
            raw.extend_from_slice(&2.0f64.to_be_bytes());
            assert!(
                PostgresPath::from_sql(&Type::PATH, &raw).is_err(),
                "{count}"
            );
        }
    }

    #[test]
    fn df_value_to_sql_rejects_malformed_path() {
        let mut buf = BytesMut::new();
        assert!(DfValue::from("[(1,2),")
            .to_sql(&Type::PATH, &mut buf)
            .is_err());
    }
}
