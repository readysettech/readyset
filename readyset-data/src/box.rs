//! Text form of the Postgres built-in `box` type, which a [`DfValue`](crate::DfValue) holds as
//! text.

use geo_types::{Point, Rect};
use readyset_errors::{ReadySetError, ReadySetResult};

use crate::point::try_parse_point;

/// Parses a Postgres `box` in any text form Postgres accepts, `(x1,y1),(x2,y2)`,
/// `((x1,y1),(x2,y2))` or `x1,y1,x2,y2`, in either corner order. Postgres itself writes only
/// the first form, upper-right corner first.
pub fn parse_box(s: &str) -> ReadySetResult<Rect> {
    let trimmed = s.trim();
    let unwrapped = trimmed.strip_prefix('(').and_then(|s| s.strip_suffix(')'));
    [Some(trimmed), unwrapped]
        .into_iter()
        .flatten()
        .find_map(split_corners)
        .map(|(a, b)| Rect::new(a, b))
        .ok_or_else(|| ReadySetError::DfValueConversionError {
            src_type: "text".into(),
            target_type: "box".into(),
            details: format!("invalid box value: {s:?}"),
        })
}

fn split_corners(s: &str) -> Option<(Point, Point)> {
    let mut depth = 0u32;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                if let (Some(a), Some(b)) = (try_parse_point(&s[..i]), try_parse_point(&s[i + 1..]))
                {
                    return Some((a, b));
                }
            }
            _ => {}
        }
    }
    None
}

/// Formats a box upper-right corner first, as Postgres writes it.
pub fn format_box(rect: Rect) -> String {
    let (hi, lo) = (rect.max(), rect.min());
    format!("({},{}),({},{})", hi.x, hi.y, lo.x, lo.y)
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::{FromSql, ToSql, Type};

    use super::*;
    use crate::DfValue;

    #[test]
    fn parse_accepts_postgres_forms() {
        for s in [
            "(3,4),(1,-2)",
            "((3,4),(1,-2))",
            "3,4,1,-2",
            "  ( ( 3 , 4 ) , ( 1 , -2 ) )  ",
            "(1,-2),(3,4)",
            "(1,4),(3,-2)",
        ] {
            assert_eq!(
                parse_box(s).unwrap(),
                Rect::new((1.0, -2.0), (3.0, 4.0)),
                "{s:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_malformed_input() {
        for s in [
            "",
            "(1,2)",
            "1,2,3",
            "(1,2),(3,4",
            "((1,2),(3,4)",
            "(1,2),(3,4),(5,6)",
            "(a,2),(3,4)",
        ] {
            assert!(parse_box(s).is_err(), "{s:?}");
        }
    }

    #[test]
    fn format_round_trips() {
        for r in [
            Rect::new((1.5, -2.0), (3.0, 4.25)),
            Rect::new((0.1, 1e300), (-0.0, f64::MIN_POSITIVE)),
            Rect::new((f64::NEG_INFINITY, 0.0), (f64::INFINITY, 1.0)),
        ] {
            assert_eq!(parse_box(&format_box(r)).unwrap(), r);
        }
    }

    #[test]
    fn format_writes_upper_right_first() {
        assert_eq!(
            format_box(Rect::new((1.0, 4.0), (3.0, -2.0))),
            "(3,4),(1,-2)"
        );
    }

    #[test]
    fn df_value_binary_round_trip() {
        let mut buf = bytes::BytesMut::new();
        DfValue::from("(1,-2),(3,4)")
            .to_sql(&Type::BOX, &mut buf)
            .unwrap();
        assert_eq!(
            DfValue::from_sql(&Type::BOX, &buf).unwrap(),
            DfValue::from("(3,4),(1,-2)")
        );
    }

    #[test]
    fn df_value_to_sql_rejects_malformed_box() {
        let mut buf = bytes::BytesMut::new();
        assert!(DfValue::from("(1,2),(3,")
            .to_sql(&Type::BOX, &mut buf)
            .is_err());
    }
}
