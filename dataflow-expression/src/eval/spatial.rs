use bitflags::bitflags;
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

bitflags! {
    /// Bit flags used in PostGIS type codes to indicate geometry properties.
    ///
    /// The type code in PostGIS contains:
    /// - Bit 28 (0x10000000): Has bounding box flag
    /// - Bit 29 (0x20000000): Has SRID flag
    /// - Bit 30 (0x40000000): Has Z dimension flag
    /// - Bit 31 (0x80000000): Has M dimension flag
    ///
    /// For more details on the type code flags, see:
    /// https://github.com/postgis/postgis/blob/54c1f5671c6ffc7617621bc09a685872cf7695ac/liblwgeom/liblwgeom.h.in#L121-L127
    #[derive(Debug, Clone, Copy)]
    struct PostgisTypeFlags: u32 {
        const HAS_BBOX = 0x10000000;
        const HAS_SRID = 0x20000000;
        const HAS_Z = 0x40000000;
        const HAS_M = 0x80000000;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Point {
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) srid: Option<u32>,
}

impl Point {
    /// Create a new `Point` from a byte array. The interpretation of the byte array is engine-specific.
    pub(crate) fn try_from_bytes(bytes: &[u8], engine: SqlEngine) -> ReadySetResult<Self> {
        match engine {
            SqlEngine::MySQL => Self::try_from_mysql_bytes(bytes),
            SqlEngine::PostgreSQL => Self::try_from_postgis_bytes(bytes),
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
        let is_little_endian = bytes[4] == 0x01;
        let (mut x, mut y) = Self::extract_coordinates(bytes, is_little_endian)?;
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

        Ok(Point {
            x,
            y,
            srid: Some(srid),
        })
    }

    /// Extract the X and Y coordinates from the byte array.
    ///
    /// The byte array is expected to be in the format of a point, with the X and Y coordinates
    /// at the end of the array. The byte order is determined by the is_little_endian flag.
    fn extract_coordinates(bytes: &[u8], is_little_endian: bool) -> ReadySetResult<(f64, f64)> {
        if is_little_endian {
            Ok((
                f64::from_le_bytes(
                    bytes[bytes.len() - 16..bytes.len() - 8]
                        .try_into()
                        .map_err(|_| {
                            invalid_query_err!("Invalid X little endian coordinate bytes")
                        })?,
                ),
                f64::from_le_bytes(
                    bytes[bytes.len() - 8..].try_into().map_err(|_| {
                        invalid_query_err!("Invalid Y little endian coordinate bytes")
                    })?,
                ),
            ))
        } else {
            Ok((
                f64::from_be_bytes(
                    bytes[bytes.len() - 16..bytes.len() - 8]
                        .try_into()
                        .map_err(|_| invalid_query_err!("Invalid X big endian coordinate bytes"))?,
                ),
                f64::from_be_bytes(
                    bytes[bytes.len() - 8..]
                        .try_into()
                        .map_err(|_| invalid_query_err!("Invalid Y big endian coordinate bytes"))?,
                ),
            ))
        }
    }

    /// Create a new `Point` from a postgis byte array. This is the data we receive from the
    /// wal logical replication. The actual format is based on the postgis notion of
    /// "Extended Well-Known Binary" (EWKB) [0]. This is much like the MySQL WKB format [1]
    /// (see try_from_mysql_bytes), but explicitly includes the SRID (it's shoveled in the
    /// middle of the byte array).
    ///
    /// Note: we are currently only support 2D points; postgis supports 3D and 4D point.
    ///
    /// [0]: https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
    /// [1]: https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html
    pub(crate) fn try_from_postgis_bytes(bytes: &[u8]) -> ReadySetResult<Self> {
        if bytes.len() < 21 {
            return Err(invalid_query_err!(
                "Point argument must be at least 21 bytes long, was {:?}",
                bytes.len()
            ));
        }

        // tells us the byte order of the points fields (X, Y), which are at the end of the array.
        let is_little_endian = bytes[0] == 0x01;
        let (x, y) = Self::extract_coordinates(bytes, is_little_endian)?;
        let srid = Self::verify_postgis_type(bytes, is_little_endian)?;

        Ok(Point { x, y, srid })
    }

    /// Extracts the SRID from a PostGIS byte array and validates the geometry type and dimension flags.
    ///
    /// Returns the SRID if present, or None if not. Errors if:
    /// - Geometry type is not a Point
    /// - Has bounding box
    /// - Has Z dimension
    /// - Has M dimension
    fn verify_postgis_type(bytes: &[u8], is_little_endian: bool) -> ReadySetResult<Option<u32>> {
        let type_code = u32::from_le_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| invalid_query_err!("Invalid spatial type"))?,
        );

        // Check if geometry type is Point (1)
        let geometry_type = type_code & 0x0F;
        if geometry_type != 1 {
            invalid_query_err!("Geometry type was not a point, was {:?}", geometry_type);
        }

        let flags = PostgisTypeFlags::from_bits_retain(type_code);

        // Extract SRID if present - it obeys the byte order of the rest of the data block.
        let srid = if flags.contains(PostgisTypeFlags::HAS_SRID) {
            if is_little_endian {
                Some(u32::from_le_bytes(
                    bytes[5..9]
                        .try_into()
                        .map_err(|_| invalid_query_err!("Invalid SRID"))?,
                ))
            } else {
                Some(u32::from_be_bytes(
                    bytes[5..9]
                        .try_into()
                        .map_err(|_| invalid_query_err!("Invalid SRID"))?,
                ))
            }
        } else {
            None
        };

        // Validate dimension flags
        if flags.contains(PostgisTypeFlags::HAS_BBOX) {
            invalid_query_err!("Not supporting postgis bounding boxes");
        }
        if flags.contains(PostgisTypeFlags::HAS_Z) {
            invalid_query_err!("Not supporting postgis points with Z dimensions");
        }
        if flags.contains(PostgisTypeFlags::HAS_M) {
            invalid_query_err!("Not supporting postgis points with M dimensions");
        }

        Ok(srid)
    }

    /// Format the point as a string. The format is engine-specific.
    pub fn format(&self, engine: SqlEngine, print_srid: bool) -> Result<String, ReadySetError> {
        match engine {
            SqlEngine::MySQL => Ok(format!("POINT({} {})", self.x, self.y)),
            SqlEngine::PostgreSQL => {
                if print_srid {
                    if let Some(srid) = self.srid {
                        // EWKT format
                        return Ok(format!("SRID={};POINT({} {})", srid, self.x, self.y));
                    }
                }
                Ok(format!("POINT({} {})", self.x, self.y))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function shared between MySQL and PostgreSQL tests
    fn assert_point_coordinates(pt: &Point, expected_x: f64, expected_y: f64) {
        assert!((pt.x - expected_x).abs() < 1e-10);
        assert!((pt.y - expected_y).abs() < 1e-10);
    }

    mod mysql {
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
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_nonzero_srid_little_endian() {
            let x = 10.0;
            let y = 20.0;
            let srid = 4326;
            let bytes = make_point_bytes(srid, x, y, true, 1);
            let pt = Point::try_from_bytes(&bytes, SqlEngine::MySQL).unwrap();
            // X and Y should be swapped due to nonzero SRID
            assert_point_coordinates(&pt, y, x);
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
            assert_point_coordinates(&pt, x, y);
        }
    }

    mod postgres {
        use super::*;

        fn make_point_bytes(srid: Option<u32>, x: f64, y: f64, little_endian: bool) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(21);

            // Byte order
            bytes.push(if little_endian { 0x01 } else { 0x00 });

            // Type code - Point (1) with SRID flag if present
            let type_code: u32 = if srid.is_some() { 0x20000001 } else { 1 };
            bytes.extend_from_slice(&type_code.to_le_bytes());

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

        #[test]
        fn test_valid_point_no_srid_little_endian() {
            let x = 1.5;
            let y = -2.5;
            let bytes = make_point_bytes(None, x, y, true);
            let pt = Point::try_from_bytes(&bytes, SqlEngine::PostgreSQL).unwrap();
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_with_srid_little_endian() {
            let x = 10.0;
            let y = 20.0;
            let srid = 4326;
            let bytes = make_point_bytes(Some(srid), x, y, true);
            let pt = Point::try_from_bytes(&bytes, SqlEngine::PostgreSQL).unwrap();
            assert_point_coordinates(&pt, x, y);
            assert_eq!(pt.srid, Some(srid));
        }

        #[test]
        fn test_invalid_length() {
            let bytes = vec![0u8; 20]; // Too short
            assert!(Point::try_from_bytes(&bytes, SqlEngine::PostgreSQL).is_err());
        }

        #[test]
        fn test_valid_point_big_endian() {
            let x = 3.0;
            let y = 4.0;
            let bytes = make_point_bytes(None, x, y, false);
            let pt = Point::try_from_bytes(&bytes, SqlEngine::PostgreSQL).unwrap();
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_big_endian_with_srid() {
            let x = 3.0;
            let y = 4.0;
            let srid = 4326;
            let bytes = make_point_bytes(Some(srid), x, y, false);
            let pt = Point::try_from_bytes(&bytes, SqlEngine::PostgreSQL).unwrap();
            assert_point_coordinates(&pt, x, y);
            assert_eq!(pt.srid, Some(srid));
        }
    }
}
