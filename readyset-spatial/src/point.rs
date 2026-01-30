use super::{Pair, ReadysetSpatialError, extract_srid};
use tracing::trace;

#[derive(Debug, Clone)]
pub(crate) struct Point {
    coord: Pair,
    srid: Option<u32>,
}

impl Point {
    /// Create a new `Point` from a mysql byte array; this is the format we receive from the binlog replication [0].
    ///
    /// [0]: https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html
    pub fn try_from_mysql_bytes(bytes: &[u8]) -> Result<Self, ReadysetSpatialError> {
        if bytes.len() != 25 {
            return Err(ReadysetSpatialError::InvalidInput(format!(
                "Point argument must be 25 bytes long, was {:?}",
                bytes.len()
            )));
        }

        let srid = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid SRID".into()))?,
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
                .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid spatial type".into()))?,
        );
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
    /// "Extended Well-Known Binary" (EWKB) [0]. This is much like the MySQL WKB format [1]
    /// (see try_from_mysql_bytes), but explicitly includes the SRID (it's shoveled in the
    /// middle of the byte array).
    ///
    /// Note: we are currently only support 2D points; postgis supports 3D and 4D point.
    ///
    /// [0]: https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
    /// [1]: https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html
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
            let pt = Point::try_from_mysql_bytes(&bytes).unwrap();
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_nonzero_srid_little_endian() {
            let x = 10.0;
            let y = 20.0;
            let srid = 4326;
            let bytes = make_point_bytes(srid, x, y, true, 1);
            let pt = Point::try_from_mysql_bytes(&bytes).unwrap();
            // X and Y should be swapped due to nonzero SRID
            assert_point_coordinates(&pt, y, x);
        }

        #[test]
        fn test_invalid_length() {
            let bytes = [0u8; 24];
            assert!(Point::try_from_mysql_bytes(&bytes).is_err());
        }

        #[test]
        fn test_invalid_spatial_type() {
            let bytes = make_point_bytes(0, 1.0, 2.0, true, 2); // 2 = not Point
            assert!(Point::try_from_mysql_bytes(&bytes).is_err());
        }

        #[test]
        fn test_valid_point_big_endian() {
            let x = 3.0;
            let y = 4.0;
            let bytes = make_point_bytes(0, x, y, false, 1);
            let pt = Point::try_from_mysql_bytes(&bytes).unwrap();
            assert_point_coordinates(&pt, x, y);
        }
    }

    mod postgres {
        use pretty_assertions::assert_eq;

        use super::*;

        fn make_point_bytes(srid: Option<u32>, x: f64, y: f64, little_endian: bool) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(21);

            // Byte order
            bytes.push(if little_endian { 0x01 } else { 0x00 });

            // Type code - Point (1) with SRID flag if present
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

        #[test]
        fn test_valid_point_no_srid_little_endian() {
            let x = 1.5;
            let y = -2.5;
            let bytes = make_point_bytes(None, x, y, true);
            let pt = Point::try_from_postgis_bytes(&bytes).unwrap();
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_with_srid_little_endian() {
            let x = 10.0;
            let y = 20.0;
            let srid = 4326;
            let bytes = make_point_bytes(Some(srid), x, y, true);
            let pt = Point::try_from_postgis_bytes(&bytes).unwrap();
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
            let bytes = make_point_bytes(None, x, y, false);
            let pt = Point::try_from_postgis_bytes(&bytes).unwrap();
            assert_point_coordinates(&pt, x, y);
        }

        #[test]
        fn test_valid_point_big_endian_with_srid() {
            let x = 3.0;
            let y = 4.0;
            let srid = 4326;
            let bytes = make_point_bytes(Some(srid), x, y, false);
            let pt = Point::try_from_postgis_bytes(&bytes).unwrap();
            assert_point_coordinates(&pt, x, y);
            assert_eq!(pt.srid, Some(srid));
        }
    }
}
