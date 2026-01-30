//! Point geometry type support.

use tracing::trace;

use super::{Pair, ReadysetSpatialError, extract_srid};

/// A Point geometry with optional SRID.
#[derive(Debug, Clone)]
pub(crate) struct Point {
    coord: Pair,
    srid: Option<u32>,
}

impl Point {
    /// Create a new `Point` from a mysql byte array; this is the format we receive from the binlog replication.
    ///
    /// Reference: <https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html>
    pub fn try_from_mysql_bytes(bytes: &[u8]) -> Result<Self, ReadysetSpatialError> {
        if bytes.len() != 25 {
            return Err(ReadysetSpatialError::InvalidInput(format!(
                "Point argument must be 25 bytes long, was {:?}",
                bytes.len()
            )));
        }

        // SRID is always little-endian in mysql
        let srid = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid SRID".into()))?,
        );

        // the last 8 bytes of the value are the Y coordinate,
        // the 8 bytes preceding that are the X coordinate.
        // so we need to extract those and convert them to f64.
        // but first, we need to derive the byte order.
        // The byte after SRID is the WKB byte order flag: 0x01 = little-endian, 0x00 = big-endian.
        // All multi-byte WKB fields (type code, coordinates) respect this flag.
        // See module docs for details on WKB byte order handling.
        let is_little_endian = bytes[4] == 0x01;
        let (mut x, mut y) = Self::extract_coordinates(bytes, is_little_endian)?;
        // well-known binary type is encoded in the next 4 bytes after the byte order flag,
        // even though its value fits into a single byte.
        // MySQL uses values from 1 through 7 to indicate Point, LineString, and so on.
        // (https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html)
        let type_bytes: [u8; 4] = bytes[5..9]
            .try_into()
            .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid spatial type".into()))?;
        let spatial_type = if is_little_endian {
            u32::from_le_bytes(type_bytes)
        } else {
            u32::from_be_bytes(type_bytes)
        };
        if spatial_type != 1 {
            return Err(ReadysetSpatialError::InvalidInput(format!(
                "only supporting MySQL Point data types, was {:?}",
                spatial_type
            )));
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

        Ok(Point {
            coord: Pair { x, y },
            srid: Some(srid),
        })
    }

    /// Extract the X and Y coordinates from the byte array.
    ///
    /// The byte array is expected to be in the format of a point, with the X and Y coordinates
    /// at the end of the array. The byte order is determined by the is_little_endian flag.
    fn extract_coordinates(
        bytes: &[u8],
        is_little_endian: bool,
    ) -> Result<(f64, f64), ReadysetSpatialError> {
        if is_little_endian {
            Ok((
                f64::from_le_bytes(
                    bytes[bytes.len() - 16..bytes.len() - 8]
                        .try_into()
                        .map_err(|_| {
                            ReadysetSpatialError::InvalidInput(
                                "Invalid X little endian coordinate bytes".into(),
                            )
                        })?,
                ),
                f64::from_le_bytes(bytes[bytes.len() - 8..].try_into().map_err(|_| {
                    ReadysetSpatialError::InvalidInput(
                        "Invalid Y little endian coordinate bytes".into(),
                    )
                })?),
            ))
        } else {
            Ok((
                f64::from_be_bytes(
                    bytes[bytes.len() - 16..bytes.len() - 8]
                        .try_into()
                        .map_err(|_| {
                            ReadysetSpatialError::InvalidInput(
                                "Invalid X big endian coordinate bytes".into(),
                            )
                        })?,
                ),
                f64::from_be_bytes(bytes[bytes.len() - 8..].try_into().map_err(|_| {
                    ReadysetSpatialError::InvalidInput(
                        "Invalid Y big endian coordinate bytes".into(),
                    )
                })?),
            ))
        }
    }

    /// Create a new `Point` from a postgis byte array. This is the data we receive from the
    /// wal logical replication. The actual format is based on the postgis notion of
    /// "Extended Well-Known Binary" (EWKB). This is much like the MySQL WKB format
    /// (see try_from_mysql_bytes), but explicitly includes the SRID (it's shoveled in the
    /// middle of the byte array).
    ///
    /// Note: we are currently only support 2D points; postgis supports 3D and 4D point.
    ///
    /// Reference: <https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT>
    pub(crate) fn try_from_postgis_bytes(bytes: &[u8]) -> Result<Self, ReadysetSpatialError> {
        if bytes.len() < 21 {
            return Err(ReadysetSpatialError::InvalidInput(format!(
                "Point argument must be at least 21 bytes long, was {:?}",
                bytes.len()
            )));
        }

        // tells us the byte order of the points fields (X, Y), which are at the end of the array.
        let is_little_endian = bytes[0] == 0x01;
        let srid = extract_srid(bytes, is_little_endian)?;
        let (x, y) = Self::extract_coordinates(bytes, is_little_endian)?;
        let coord = Pair { x, y };
        Ok(Point { coord, srid })
    }

    /// Format the point as a MySQL WKT string.
    pub fn format_mysql(&self) -> String {
        format!("POINT({} {})", self.coord.x, self.coord.y)
    }

    /// Format the point as a PostGIS WKT or EWKT string.
    ///
    /// If `print_srid` is true and an SRID is present, returns EWKT format with SRID prefix.
    pub fn format_postgis(&self, print_srid: bool) -> String {
        if let Some(srid) = self.srid.filter(|_| print_srid) {
            // EWKT format
            return format!("SRID={};POINT({} {})", srid, self.coord.x, self.coord.y);
        }
        format!("POINT({} {})", self.coord.x, self.coord.y)
    }
}

/// Create EWKB bytes for a PostGIS Point.
///
/// Format (little-endian, with SRID):
/// - 1 byte: byte order (0x01 = little-endian, 0x00 = big-endian)
/// - 4 bytes: type code (0x20000001 = Point with SRID, 0x00000001 = Point without SRID)
/// - 4 bytes: SRID (only if type has SRID flag)
/// - 8 bytes: X coordinate (f64)
/// - 8 bytes: Y coordinate (f64)
///
/// For use primarily in testing and data generation.
pub fn make_postgis_point_bytes(x: f64, y: f64, srid: Option<u32>, little_endian: bool) -> Vec<u8> {
    let capacity = if srid.is_some() { 25 } else { 21 };
    let mut bytes = Vec::with_capacity(capacity);

    // Byte order
    bytes.push(if little_endian { 0x01 } else { 0x00 });

    // Type code: Point (1) with SRID flag (0x20000000) if SRID is present
    let type_code: u32 = if srid.is_some() { 0x20000001 } else { 1 };
    if little_endian {
        bytes.extend_from_slice(&type_code.to_le_bytes());
    } else {
        bytes.extend_from_slice(&type_code.to_be_bytes());
    }

    // SRID if present
    if let Some(srid) = srid {
        if little_endian {
            bytes.extend_from_slice(&srid.to_le_bytes());
        } else {
            bytes.extend_from_slice(&srid.to_be_bytes());
        }
    }

    // Coordinates
    if little_endian {
        bytes.extend_from_slice(&x.to_le_bytes());
        bytes.extend_from_slice(&y.to_le_bytes());
    } else {
        bytes.extend_from_slice(&x.to_be_bytes());
        bytes.extend_from_slice(&y.to_be_bytes());
    }

    bytes
}

/// Create WKB bytes for a MySQL Point.
///
/// MySQL geometry format:
/// - 4 bytes: SRID (always little-endian)
/// - 1 byte: byte order (0x01 = little-endian, 0x00 = big-endian)
/// - 4 bytes: WKB type (Point = 1)
/// - 8 bytes: X coordinate (f64)
/// - 8 bytes: Y coordinate (f64)
///
/// Note: MySQL always stores SRID in little-endian, but the WKB portion
/// respects the byte order flag.
///
/// For use primarily in testing and data generation.
pub fn make_mysql_point_bytes(x: f64, y: f64, srid: u32, little_endian: bool) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(25);

    // SRID - always little-endian in MySQL
    bytes.extend_from_slice(&srid.to_le_bytes());

    // Byte order
    bytes.push(if little_endian { 0x01 } else { 0x00 });

    // WKB type: Point (1)
    if little_endian {
        bytes.extend_from_slice(&1u32.to_le_bytes());
    } else {
        bytes.extend_from_slice(&1u32.to_be_bytes());
    }

    // Coordinates
    if little_endian {
        bytes.extend_from_slice(&x.to_le_bytes());
        bytes.extend_from_slice(&y.to_le_bytes());
    } else {
        bytes.extend_from_slice(&x.to_be_bytes());
        bytes.extend_from_slice(&y.to_be_bytes());
    }

    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function shared between MySQL and PostgreSQL tests
    fn assert_point_coordinates(pt: &Point, expected_x: f64, expected_y: f64) {
        assert!((pt.coord.x - expected_x).abs() < 1e-10);
        assert!((pt.coord.y - expected_y).abs() < 1e-10);
    }

    mod mysql {
        use super::*;

        #[test]
        fn test_valid_point_srid_0_little_endian() {
            let x = 1.5;
            let y = -2.5;
            let bytes = make_mysql_point_bytes(x, y, 0, true);
            let pt = Point::try_from_mysql_bytes(&bytes).expect("valid point");
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_nonzero_srid_little_endian() {
            let x = 10.0;
            let y = 20.0;
            let srid = 4326;
            let bytes = make_mysql_point_bytes(x, y, srid, true);
            let pt = Point::try_from_mysql_bytes(&bytes).expect("valid point");
            // X and Y should be swapped due to nonzero SRID
            assert_point_coordinates(&pt, y, x);
        }

        #[test]
        fn test_invalid_length() {
            let bytes = [0u8; 24];
            assert!(Point::try_from_mysql_bytes(&bytes).is_err());
        }

        #[test]
        fn test_valid_point_big_endian() {
            let x = 3.0;
            let y = 4.0;
            let bytes = make_mysql_point_bytes(x, y, 0, false);
            let pt = Point::try_from_mysql_bytes(&bytes).expect("valid point");
            assert_point_coordinates(&pt, x, y);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn test_valid_point_no_srid_little_endian() {
            let x = 1.5;
            let y = -2.5;
            let bytes = make_postgis_point_bytes(x, y, None, true);
            let pt = Point::try_from_postgis_bytes(&bytes).expect("valid point");
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_with_srid_little_endian() {
            let x = 10.0;
            let y = 20.0;
            let srid = 4326;
            let bytes = make_postgis_point_bytes(x, y, Some(srid), true);
            let pt = Point::try_from_postgis_bytes(&bytes).expect("valid point");
            assert_point_coordinates(&pt, x, y);
            assert_eq!(pt.srid, Some(srid));
        }

        #[test]
        fn test_invalid_length() {
            let bytes = vec![0u8; 20]; // Too short
            assert!(Point::try_from_postgis_bytes(&bytes).is_err());
        }

        #[test]
        fn test_valid_point_big_endian() {
            let x = 3.0;
            let y = 4.0;
            let bytes = make_postgis_point_bytes(x, y, None, false);
            let pt = Point::try_from_postgis_bytes(&bytes).expect("valid point");
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_big_endian_with_srid() {
            let x = 3.0;
            let y = 4.0;
            let srid = 4326;
            let bytes = make_postgis_point_bytes(x, y, Some(srid), false);
            let pt = Point::try_from_postgis_bytes(&bytes).expect("valid point");
            assert_point_coordinates(&pt, x, y);
            assert_eq!(pt.srid, Some(srid));
        }
    }

    mod ewkb_point_builder {
        use crate::SpatialType;

        use super::*;

        #[test]
        fn little_endian_no_srid() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, None, true);

            assert_eq!(bytes.len(), 21);
            assert_eq!(bytes[0], 0x01); // little-endian marker
            assert_eq!(&bytes[1..5], &1u32.to_le_bytes()); // type code: Point (1)
            assert_eq!(&bytes[5..13], &1.0f64.to_le_bytes()); // X
            assert_eq!(&bytes[13..21], &2.0f64.to_le_bytes()); // Y
        }

        #[test]
        fn little_endian_with_srid() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, Some(4326), true);

            assert_eq!(bytes.len(), 25);
            assert_eq!(bytes[0], 0x01); // little-endian marker
            assert_eq!(&bytes[1..5], &0x20000001u32.to_le_bytes()); // type code with SRID flag
            assert_eq!(&bytes[5..9], &4326u32.to_le_bytes()); // SRID
            assert_eq!(&bytes[9..17], &1.0f64.to_le_bytes()); // X
            assert_eq!(&bytes[17..25], &2.0f64.to_le_bytes()); // Y
        }

        #[test]
        fn big_endian_no_srid() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, None, false);

            assert_eq!(bytes.len(), 21);
            assert_eq!(bytes[0], 0x00); // big-endian marker
            assert_eq!(&bytes[1..5], &1u32.to_be_bytes()); // type code: Point (1)
            assert_eq!(&bytes[5..13], &1.0f64.to_be_bytes()); // X
            assert_eq!(&bytes[13..21], &2.0f64.to_be_bytes()); // Y
        }

        #[test]
        fn big_endian_with_srid() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, Some(4326), false);

            assert_eq!(bytes.len(), 25);
            assert_eq!(bytes[0], 0x00); // big-endian marker
            assert_eq!(&bytes[1..5], &0x20000001u32.to_be_bytes()); // type code with SRID flag
            assert_eq!(&bytes[5..9], &4326u32.to_be_bytes()); // SRID
            assert_eq!(&bytes[9..17], &1.0f64.to_be_bytes()); // X
            assert_eq!(&bytes[17..25], &2.0f64.to_be_bytes()); // Y
        }

        #[test]
        fn capacity_is_exact() {
            // Verify no reallocations happen
            let bytes_no_srid = make_postgis_point_bytes(1.0, 2.0, None, true);
            assert_eq!(bytes_no_srid.len(), bytes_no_srid.capacity());

            let bytes_with_srid = make_postgis_point_bytes(1.0, 2.0, Some(4326), true);
            assert_eq!(bytes_with_srid.len(), bytes_with_srid.capacity());
        }

        #[test]
        fn extreme_coordinates() {
            // Test with extreme but valid f64 values
            let bytes = make_postgis_point_bytes(f64::MAX, f64::MIN, None, true);
            assert_eq!(bytes.len(), 21);
            assert_eq!(&bytes[5..13], &f64::MAX.to_le_bytes());
            assert_eq!(&bytes[13..21], &f64::MIN.to_le_bytes());
        }

        #[test]
        fn detects_point_little_endian() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, None, true);
            let spatial_type = crate::try_get_spatial_type_from_postgres(&bytes)
                .expect("should detect point type");
            assert_eq!(spatial_type, SpatialType::PostgisPoint);
        }

        #[test]
        fn detects_point_big_endian() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, None, false);
            let spatial_type = crate::try_get_spatial_type_from_postgres(&bytes)
                .expect("should detect point type with big-endian");
            assert_eq!(spatial_type, SpatialType::PostgisPoint);
        }

        #[test]
        fn detects_point_with_srid_big_endian() {
            let bytes = make_postgis_point_bytes(1.0, 2.0, Some(4326), false);
            let spatial_type = crate::try_get_spatial_type_from_postgres(&bytes)
                .expect("should detect point type with SRID big-endian");
            assert_eq!(spatial_type, SpatialType::PostgisPoint);
        }
    }
}
