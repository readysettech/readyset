/// Welcome to spatial data type support in Readyset.
///
/// As of this initial writing (May 2025), the goal with supporting
/// spatial data types is only to support the basic data types themselves,
/// and a handful of simple, "display" functions. We are explicitly not
/// supporting operators, functions, nor spatial indices over these types.
/// As Readyset is a cache, we feel there is little to no value in suporting
/// those things as the likelihood of a successful cache hit rate is low.
///
/// We do, however, need to support the basic data types themselves, primarily
/// to allow snapshotting/replicating the data. Supporting the simple display
/// functions, like `ST_AsText()`, are espeically difficult to support and actually
/// do make sense within the scope of Readyset.
///
/// In the current implementation, we take the byte array directly from the upstream,
/// as-is, and store that on disk (`DfValue::ByteArray`), as well as pass that byte array
/// down through the dataflow graph. We do not attempt to create a canonical representation of
/// any of the various upstream formats. This is all for simplicity, as the only time
/// we need to interpret those bytes is in the various display functions, implemented here.
///
/// Further, the current implementation only supports Point data types, although extending this
/// to support other spatial/geometric types is fairly straightforward.
///
/// We do not plan on supporting the postgres native data types, but instead support `PostGIS`.
/// The former being legacy, while the latter is state-of-the-art.
use readyset_data::dialect::SqlEngine;
use readyset_errors::{invalid_query_err, ReadySetError, ReadySetResult};
use tracing::trace;

#[derive(Debug, Clone)]
pub(crate) struct Point {
    pub(crate) x: f64,
    pub(crate) y: f64,
}

impl Point {
    /// Create a new `Point` from a byte array. The interpretation of the byte array is engine-specific.
    pub(crate) fn try_from_bytes(bytes: &[u8], engine: SqlEngine) -> ReadySetResult<Self> {
        match engine {
            SqlEngine::MySQL => Self::try_from_mysql_bytes(bytes),
            SqlEngine::PostgreSQL => Err(invalid_query_err!(
                "Spatial types are not supported for this engine"
            )),
        }
    }

    /// Create a new `Point` from a mysql byte array; this is the format we receive from the binlog replication [0].
    ///
    /// [0]: https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html
    fn try_from_mysql_bytes(bytes: &[u8]) -> ReadySetResult<Self> {
        if bytes.len() != 25 {
            return Err(invalid_query_err!(
                "Point argument must be 25 bytes long, was {:?}",
                bytes.len()
            ));
        }

        let srid = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| invalid_query_err!("Invalid SRID"))?,
        );

        // the last 8 bytes of the value are the Y coordinate,
        // the 8 bytes preceeding that are the X coordinate.
        // so we need to extract those and convert them to f64.
        // but first, we need to derive the byte order.
        // the byte after SRID is the byte order flag, which is 0x01 for little endian, 0x00 for big endian.
        let byte_order = bytes[4];
        let (mut x, mut y) =
            if byte_order == 0x01 {
                // little endian
                (
                    f64::from_le_bytes(
                        bytes[bytes.len() - 16..bytes.len() - 8]
                            .try_into()
                            .map_err(|_| {
                                invalid_query_err!("Invalid X little endian coordinate bytes")
                            })?,
                    ),
                    f64::from_le_bytes(bytes[bytes.len() - 8..].try_into().map_err(|_| {
                        invalid_query_err!("Invalid Y little endian coordinate bytes")
                    })?),
                )
            } else {
                // big endian
                (
                    f64::from_be_bytes(
                        bytes[bytes.len() - 16..bytes.len() - 8]
                            .try_into()
                            .map_err(|_| {
                                invalid_query_err!("Invalid X big endian coordinate bytes")
                            })?,
                    ),
                    f64::from_be_bytes(bytes[bytes.len() - 8..].try_into().map_err(|_| {
                        invalid_query_err!("Invalid Y big endian coordinate bytes")
                    })?),
                )
            };

        // well-known binary type is encoded in the next 4 bytes after the byte order flag,
        // even though it's value fits into a single byte.
        // MySQL uses values from 1 through 7 to indicate Point, LineString, and so on.
        // (https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html)
        let spatial_type = u32::from_le_bytes(
            bytes[5..9]
                .try_into()
                .map_err(|_| invalid_query_err!("Invalid spatial type"))?,
        );
        if spatial_type != 1 {
            return Err(invalid_query_err!(
                "only supporting MySQL Point data types, was {:?}",
                spatial_type
            ));
        }

        // ZOMG, if an SRID is defined (i.e. not 0), then X and Y are swapped in the byte array.
        // To uncover this, we need to look at the mysql source, in particular `Item_func_as_wkt::val_str_ascii`
        // (https://github.com/mysql/mysql-server/blob/8.4/sql/item_geofunc.cc#L3140-L3249). There is a block
        // toward the end of the function with this comment:
        //    The SRS default axis order is lat-long. The storage axis
        //    order is long-lat, so we must reverse coordinates.
        if srid != 0 {
            trace!("swapping X and Y due to SRID {:?}", srid);
            std::mem::swap(&mut x, &mut y);
        }

        Ok(Point { x, y })
    }

    /// Format the point as a string. The format is engine-specific.
    pub fn format(&self, engine: SqlEngine) -> Result<String, ReadySetError> {
        match engine {
            SqlEngine::MySQL => Ok(format!("POINT({} {})", self.x, self.y)),
            SqlEngine::PostgreSQL => Err(invalid_query_err!(
                "Spatial types are not supported for this engine"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_point_bytes(
        srid: u32,
        x: f64,
        y: f64,
        little_endian: bool,
        spatial_type: u32,
    ) -> [u8; 25] {
        let mut bytes = [0u8; 25];
        // SRID
        bytes[0..4].copy_from_slice(&srid.to_le_bytes());
        // Byte order
        bytes[4] = if little_endian { 0x01 } else { 0x00 };
        // Spatial type - always (?) little endian
        bytes[5..9].copy_from_slice(&spatial_type.to_le_bytes());
        if little_endian {
            bytes[9..17].copy_from_slice(&x.to_le_bytes());
            bytes[17..25].copy_from_slice(&y.to_le_bytes());
        } else {
            bytes[9..17].copy_from_slice(&x.to_be_bytes());
            bytes[17..25].copy_from_slice(&y.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn test_valid_point_srid_0_little_endian() {
        let x = 1.5;
        let y = -2.5;
        let bytes = make_point_bytes(0, x, y, true, 1);
        let pt = Point::try_from_bytes(&bytes, SqlEngine::MySQL).unwrap();
        assert!((pt.x - x).abs() < 1e-10);
        assert!((pt.y - y).abs() < 1e-10);
    }

    #[test]
    fn test_valid_point_nonzero_srid_little_endian() {
        let x = 10.0;
        let y = 20.0;
        let srid = 4326;
        let bytes = make_point_bytes(srid, x, y, true, 1);
        let pt = Point::try_from_bytes(&bytes, SqlEngine::MySQL).unwrap();
        // X and Y should be swapped due to nonzero SRID
        assert!((pt.x - y).abs() < 1e-10);
        assert!((pt.y - x).abs() < 1e-10);
    }

    #[test]
    fn test_invalid_length() {
        let bytes = [0u8; 24];
        assert!(Point::try_from_bytes(&bytes, SqlEngine::MySQL).is_err());
    }

    #[test]
    fn test_invalid_spatial_type() {
        let bytes = make_point_bytes(0, 1.0, 2.0, true, 2); // 2 = not Point
        assert!(Point::try_from_bytes(&bytes, SqlEngine::MySQL).is_err());
    }

    #[test]
    fn test_valid_point_big_endian() {
        let x = 3.0;
        let y = 4.0;
        let bytes = make_point_bytes(0, x, y, false, 1);
        let pt = Point::try_from_bytes(&bytes, SqlEngine::MySQL).unwrap();
        assert!((pt.x - x).abs() < 1e-10);
        assert!((pt.y - y).abs() < 1e-10);
    }
}
