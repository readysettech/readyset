use super::spatial::extract_srid;
use super::spatial::Pair;
use readyset_data::dialect::SqlEngine;
use readyset_errors::{invalid_query_err, ReadySetError, ReadySetResult};

#[derive(Debug, Clone)]
/// Polygon structure is composed of one external ring and zero-or-more holes within the external initial ring
/// for a Ring to be considered valid, the number of points should be al least 4 points and the ending point must be the same value of the starting point.
///
///
///  valid   SELECT ST_IsValid('POLYGON ((20 180, 180 180, 180 20, 20 20, 20 180))');
///          st_isvalid
///          ------------
///          t
///          (1 row)
///
///  valid   SELECT ST_IsValid(ST_GeomFromEWKT('POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(6 6,8 6,8 8,6 8,6 6))'));
///          st_isvalid
///          ------------
///          t
///          (1 row)

///
///  invalid  SELECT ST_IsValid('POLYGON ((20 180, 180 180, 180 20, 20 20, 30, 40))');
///           ERROR:  parse error - invalid geometry
///
///  invalid  SELECT ST_IsValid('POLYGON ((20 180, 180 180, 20 180))');
///           ERROR:  geometry requires more points
///
/// For more, please check this: https://postgis.net/docs/manual-3.6/using_postgis_dbmanagement.html#PostGIS_Geometry  Section (4.4. Geometry Validation)
/// https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
/// https://libgeos.org/specifications/wkb/
pub(crate) struct Polygon {
    exterior_ring: Vec<Pair>,
    holes: Vec<Vec<Pair>>,
    srid: Option<u32>,
}

const NUM_OF_BYTES_U32: usize = std::mem::size_of::<u32>();
const NUM_OF_BYTES_F64: usize = std::mem::size_of::<f64>();
const NUM_OF_BYTES_PAIR: usize = std::mem::size_of::<Pair>();

impl Polygon {
    /// Create a new `Polygon` from a postgis byte array. This is the data we receive from the
    /// wal logical replication. The actual format is based on the postgis notion of
    /// "Extended Well-Known Binary" (EWKB) [0]
    ///
    /// Note: we are currently only support 2D Postgis polygons.
    ///
    /// [0]: https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
    pub(crate) fn try_from_postgis_bytes(bytes: &[u8]) -> ReadySetResult<Self> {
        // Empty polygon have 9 Bytes long
        // 01 03 00 00 00 00 00 00 00
        if bytes.len() < 9 {
            return Err(invalid_query_err!(
                "Polygon argument must be at least 9 bytes long, was {:?}",
                bytes.len()
            ));
        }

        // tells us the byte order of the pair fields (X, Y).
        let is_little_endian = bytes[0] == 0x01;
        let srid = extract_srid(&bytes, is_little_endian)?;
        let (external_ring, internal_rings) = Self::extract_rings(&bytes, is_little_endian, srid)?;

        Ok(Polygon {
            exterior_ring: external_ring,
            holes: internal_rings,
            srid: srid,
        })
    }

    fn extract_bytes_u32(
        bytes: &[u8],
        is_little_endian: bool,
        start: usize,
    ) -> ReadySetResult<u32> {
        let end = start + NUM_OF_BYTES_U32;
        let arr: [u8; 4] = bytes
            .get(start..end)
            .ok_or_else(|| invalid_query_err!("Insufficient bytes for u32 at offset {}", start))?
            .try_into()
            .map_err(|_| invalid_query_err!("Invalid byte slice length for u32"))?;

        Ok(if is_little_endian {
            u32::from_le_bytes(arr)
        } else {
            u32::from_be_bytes(arr)
        })
    }

    fn extract_bytes_f64(
        bytes: &[u8],
        is_little_endian: bool,
        start: usize,
    ) -> ReadySetResult<f64> {
        let end = start + NUM_OF_BYTES_F64;
        let arr: [u8; 8] = bytes
            .get(start..end)
            .ok_or_else(|| invalid_query_err!("Insufficient bytes for f64 at offset {}", start))?
            .try_into()
            .map_err(|_| invalid_query_err!("Invalid byte slice length for f64"))?;

        Ok(if is_little_endian {
            f64::from_le_bytes(arr)
        } else {
            f64::from_be_bytes(arr)
        })
    }

    fn extract_pair(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> ReadySetResult<Pair> {
        if bytes.len() < start_byte + NUM_OF_BYTES_PAIR {
            return Err(invalid_query_err!("Invalid polygon byte array size"));
        }
        let x = Self::extract_bytes_f64(&bytes, is_little_endian, start_byte)?;
        let y = Self::extract_bytes_f64(&bytes, is_little_endian, start_byte + NUM_OF_BYTES_F64)?;

        Ok(Pair { x: x, y: y })
    }
    fn extract_ring(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> ReadySetResult<Vec<Pair>> {
        let num_pairs = Self::extract_bytes_u32(&bytes, is_little_endian, start_byte)?; // First 4 bytes
        let mut pairs: Vec<Pair> = Vec::new();
        let mut offset = start_byte + NUM_OF_BYTES_U32;
        for _num in 0..num_pairs {
            let pair = Self::extract_pair(&bytes, is_little_endian, offset)?;
            pairs.push(pair);
            offset += NUM_OF_BYTES_PAIR; // 16 byte for each pair
        }
        Ok(pairs)
    }

    fn extract_rings(
        bytes: &[u8],
        is_little_endian: bool,
        srid: Option<u32>,
    ) -> ReadySetResult<(Vec<Pair>, Vec<Vec<Pair>>)> {
        let mut start_byte: usize = 9;
        if srid == None {
            start_byte = 5;
        }
        let number_of_rings = Self::extract_bytes_u32(&bytes, is_little_endian, start_byte)?;
        if number_of_rings == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // extract exterior ring
        let mut offset = start_byte + NUM_OF_BYTES_U32;
        let external_ring = Self::extract_ring(&bytes, is_little_endian, offset)?;

        // extract internal rings
        let mut internal_rings: Vec<Vec<Pair>> = Vec::new();
        let mut previous_ring_size = external_ring.len() * NUM_OF_BYTES_PAIR + NUM_OF_BYTES_U32;
        for _ring_num in 1..number_of_rings {
            offset += previous_ring_size;
            let ring = Self::extract_ring(&bytes, is_little_endian, offset)?;
            previous_ring_size = ring.len() * NUM_OF_BYTES_PAIR + NUM_OF_BYTES_U32;
            internal_rings.push(ring);
        }

        Ok((external_ring, internal_rings))
    }

    fn format_ring(ring: &Vec<Pair>) -> String {
        let mut ring_str = String::new();
        for pair in ring {
            ring_str.push_str(&format!("{} {},", pair.x, pair.y));
        }
        // Remove the trailing comma
        if ring_str.ends_with(',') {
            ring_str.pop();
        }
        ring_str
    }
    /// Format the polygon as a string. The format is engine-specific.
    pub fn format(&self, engine: SqlEngine, print_srid: bool) -> Result<String, ReadySetError> {
        match engine {
            SqlEngine::PostgreSQL => {
                let mut polygon_string = String::new();
                if print_srid {
                    if let Some(srid) = self.srid {
                        polygon_string.push_str(&format!("SRID={};", srid));
                    }
                }
                polygon_string.push_str("POLYGON");

                // handle empty polygon
                if self.exterior_ring.len() == 0 {
                    polygon_string.push_str(" EMPTY");
                    return Ok(polygon_string);
                }

                polygon_string.push_str(&format!("(({})", Self::format_ring(&self.exterior_ring)));

                for ring in &self.holes {
                    polygon_string.push_str(&format!(",({})", Self::format_ring(&ring)));
                }
                polygon_string.push(')');
                Ok(polygon_string)
            }
            SqlEngine::MySQL => Err(invalid_query_err!("Unsupported")),
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    mod postgres {

        use super::*;
        use pretty_assertions::assert_eq;

        fn make_ring_bytes(bytes: &mut Vec<u8>, ring: &Vec<Pair>, is_little_endian: bool) {
            let num_of_pairs = ring.len() as u32;

            if is_little_endian {
                bytes.extend_from_slice(&num_of_pairs.to_le_bytes());
            } else {
                bytes.extend_from_slice(&num_of_pairs.to_be_bytes());
            }

            for pair in ring {
                if is_little_endian {
                    bytes.extend_from_slice(&pair.x.to_le_bytes());
                    bytes.extend_from_slice(&pair.y.to_le_bytes());
                } else {
                    bytes.extend_from_slice(&pair.x.to_be_bytes());
                    bytes.extend_from_slice(&pair.y.to_be_bytes());
                }
            }
        }

        fn make_polygon_bytes(
            srid: Option<u32>,
            external_ring: Option<&Vec<Pair>>,
            holes: Option<&Vec<Vec<Pair>>>,
            is_little_endian: bool,
        ) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(9); // Initial Capacity for empty PostGIS Polygon is 9 bytes

            bytes.push(if is_little_endian { 0x01 } else { 0x00 });

            let type_code: u32 = if srid.is_some() { 0x20000003 } else { 3 };
            bytes.extend_from_slice(&type_code.to_le_bytes());

            // Srid
            if let Some(srid) = srid {
                if is_little_endian {
                    bytes.extend_from_slice(&srid.to_le_bytes());
                } else {
                    bytes.extend_from_slice(&srid.to_be_bytes());
                }
            }

            if let Some(external_ring) = external_ring {
                let mut num_of_rings: u32 = 1;

                if let Some(holes) = holes {
                    num_of_rings += holes.len() as u32;
                }

                // write num_of_rings to the bytes array
                if is_little_endian {
                    bytes.extend_from_slice(&num_of_rings.to_le_bytes());
                } else {
                    bytes.extend_from_slice(&num_of_rings.to_be_bytes());
                }

                make_ring_bytes(&mut bytes, &external_ring, is_little_endian);

                if let Some(holes) = holes {
                    for hole in holes {
                        make_ring_bytes(&mut bytes, hole, is_little_endian);
                    }
                }
            } else {
                bytes.extend_from_slice(&u32::to_le_bytes(0));
            }

            bytes
        }

        #[test]
        fn test_valid_empty_polygon() {
            let bytes = make_polygon_bytes(None, None, None, true);
            let polygon = Polygon::try_from_postgis_bytes(&bytes).unwrap();
            let format = polygon.format(SqlEngine::PostgreSQL, false).unwrap();
            assert_eq!(format, "POLYGON EMPTY");
        }

        #[test]
        fn test_valid_empty_polygon_with_srid() {
            let bytes = make_polygon_bytes(Some(4326), None, None, true);
            let polygon = Polygon::try_from_postgis_bytes(&bytes).unwrap();
            let format = polygon.format(SqlEngine::PostgreSQL, true).unwrap();
            assert_eq!(format, "SRID=4326;POLYGON EMPTY");
        }

        #[test]
        fn test_invalid_empty_polygon() {
            let bytes = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
            assert!(Polygon::try_from_postgis_bytes(&bytes).is_err());
        }

        #[test]
        fn test_valid_polygon_le_be() {
            let external_skelton: Vec<Pair> = vec![
                Pair { x: 0.0, y: 0.0 },
                Pair { x: 0.0, y: 1000.0 },
                Pair {
                    x: 1000.0,
                    y: 1000.0,
                },
                Pair { x: 1000.0, y: 0.0 },
                Pair { x: 0.0, y: 0.0 },
            ];

            let internal_holes: Vec<Vec<Pair>> = vec![vec![
                Pair { x: 200.0, y: 200.0 },
                Pair { x: 200.0, y: 800.0 },
                Pair { x: 800.0, y: 800.0 },
                Pair { x: 800.0, y: 200.0 },
                Pair { x: 200.0, y: 200.0 },
            ]];

            // little endian
            let bytes_le =
                make_polygon_bytes(None, Some(&external_skelton), Some(&internal_holes), true);
            let polygon_le = Polygon::try_from_postgis_bytes(&bytes_le).unwrap();

            // big endian
            let bytes_be =
                make_polygon_bytes(None, Some(&external_skelton), Some(&internal_holes), false);
            let polygon_be = Polygon::try_from_postgis_bytes(&bytes_be).unwrap();
            assert_eq!(polygon_le.format(SqlEngine::PostgreSQL, false).unwrap(), "POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))");
            assert_eq!(polygon_be.format(SqlEngine::PostgreSQL, false).unwrap(), "POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))");
        }

        #[test]
        fn test_valid_polygon_le_be_with_srid() {
            let external_skelton: Vec<Pair> = vec![
                Pair { x: 0.0, y: 0.0 },
                Pair { x: 0.0, y: 1000.0 },
                Pair {
                    x: 1000.0,
                    y: 1000.0,
                },
                Pair { x: 1000.0, y: 0.0 },
                Pair { x: 0.0, y: 0.0 },
            ];

            let internal_holes: Vec<Vec<Pair>> = vec![vec![
                Pair { x: 200.0, y: 200.0 },
                Pair { x: 200.0, y: 800.0 },
                Pair { x: 800.0, y: 800.0 },
                Pair { x: 800.0, y: 200.0 },
                Pair { x: 200.0, y: 200.0 },
            ]];

            // little endian
            let bytes_le = make_polygon_bytes(
                Some(3857),
                Some(&external_skelton),
                Some(&internal_holes),
                true,
            );
            let polygon_le = Polygon::try_from_postgis_bytes(&bytes_le).unwrap();

            // big endian
            let bytes_be = make_polygon_bytes(
                Some(3857),
                Some(&external_skelton),
                Some(&internal_holes),
                false,
            );
            let polygon_be = Polygon::try_from_postgis_bytes(&bytes_be).unwrap();
            assert_eq!(polygon_le.format(SqlEngine::PostgreSQL, true).unwrap(), "SRID=3857;POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))");
            assert_eq!(polygon_be.format(SqlEngine::PostgreSQL, true).unwrap(), "SRID=3857;POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))");
        }

        #[test]
        fn test_invalid_polygon_bytes() {
            let bytes = vec![1u8; 20]; // invalid bytes
            assert!(Polygon::try_from_postgis_bytes(&bytes).is_err());
        }
    }
}
