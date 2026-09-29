//! Text form of the Postgres built-in `point` type, which a [`DfValue`](crate::DfValue) holds as
//! text.

use geo_types::Point;
use readyset_errors::{ReadySetError, ReadySetResult};

/// Parses a Postgres `point` in either text form Postgres accepts, `(x,y)` or `x,y`, with
/// whitespace permitted around the value and each coordinate.
pub fn parse_point(s: &str) -> ReadySetResult<Point> {
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed);
    let coord = |c: &str| c.trim().parse::<f64>().ok();
    inner
        .split_once(',')
        .and_then(|(x, y)| Some(Point::new(coord(x)?, coord(y)?)))
        .ok_or_else(|| ReadySetError::DfValueConversionError {
            src_type: "text".into(),
            target_type: "point".into(),
            details: format!("invalid point value: {s:?}"),
        })
}

pub fn format_point(point: Point) -> String {
    format!("({},{})", point.x(), point.y())
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::{FromSql, ToSql, Type};

    use super::*;
    use crate::DfValue;

    #[test]
    fn parse_accepts_postgres_forms() {
        for s in ["(1.5,-2)", "1.5,-2", "  ( 1.5 , -2 )  ", "(1.5e0,-2.0)"] {
            assert_eq!(parse_point(s).unwrap(), Point::new(1.5, -2.0), "{s:?}");
        }
    }

    #[test]
    fn parse_accepts_non_finite_coordinates() {
        let p = parse_point("(Infinity,-inf)").unwrap();
        assert_eq!(p, Point::new(f64::INFINITY, f64::NEG_INFINITY));
        assert!(parse_point("(NaN,0)").unwrap().x().is_nan());
    }

    #[test]
    fn parse_rejects_malformed_input() {
        for s in [
            "", "()", "(1)", "(1,2", "1,2)", "(1,2,3)", "(a,2)", "(1,)", "[1,2]",
        ] {
            assert!(parse_point(s).is_err(), "{s:?}");
        }
    }

    #[test]
    fn format_round_trips() {
        for p in [
            Point::new(1.5, -2.0),
            Point::new(0.1, 1e300),
            Point::new(-0.0, f64::MIN_POSITIVE),
            Point::new(f64::INFINITY, f64::NEG_INFINITY),
        ] {
            assert_eq!(parse_point(&format_point(p)).unwrap(), p);
        }
    }

    #[test]
    fn df_value_binary_round_trip() {
        let mut buf = bytes::BytesMut::new();
        DfValue::from("( 1.5 , -2 )")
            .to_sql(&Type::POINT, &mut buf)
            .unwrap();
        assert_eq!(
            &buf[..],
            [1.5f64.to_be_bytes(), (-2f64).to_be_bytes()].concat()
        );
        assert_eq!(
            DfValue::from_sql(&Type::POINT, &buf).unwrap(),
            DfValue::from("(1.5,-2)")
        );
    }

    #[test]
    fn df_value_to_sql_rejects_malformed_point() {
        let mut buf = bytes::BytesMut::new();
        assert!(DfValue::from("(1,").to_sql(&Type::POINT, &mut buf).is_err());
    }
}
