use super::spatial::Pair;
use super::spatial::PostgisTypeFlags;
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

const NUM_OF_BYTES_U32: usize = 4;
const NUM_OF_BYTES_F64: usize = 8;
const NUM_OF_BYTES_PAIR: usize = 16;

impl Polygon {
    /// Create a new `Polygon` from a postgis byte array. This is the data we receive from the
    /// wal logical replication. The actual format is based on the postgis notion of
    /// "Extended Well-Known Binary" (EWKB) [0]. This is much like the MySQL WKB format [1]
    /// (see try_from_mysql_bytes), but explicitly includes the SRID (it's shoveled in the
    /// middle of the byte array).
    ///
    /// Note: we are currently only support 2D polygons.
    ///
    /// [0]: https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
    /// [1]: https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html
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
        let srid = Self::extract_srid(&bytes, is_little_endian)?;
        let (external_ring, internal_rings) = Self::extract_rings(&bytes, is_little_endian, srid)?;

        Ok(Polygon {
            exterior_ring: external_ring,
            holes: internal_rings,
            srid: srid,
        })
    }

    /// Extracts the SRID from a PostGIS byte array and validates the geometry type and dimension flags.
    ///
    /// Returns the SRID if present, or None if not. Errors if:
    /// - Geometry type is not a Point
    /// - Has bounding box
    /// - Has Z dimension
    /// - Has M dimension
    fn extract_srid(bytes: &[u8], is_little_endian: bool) -> ReadySetResult<Option<u32>> {
        let type_code = u32::from_le_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| invalid_query_err!("Invalid spatial type"))?,
        );

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

    fn extract_bytes_u32(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> ReadySetResult<u32> {
        let range = start_byte..(start_byte + NUM_OF_BYTES_U32);
        if is_little_endian {
            Ok(u32::from_le_bytes(bytes[range].try_into().map_err(
                |_| invalid_query_err!("Invalid little endian coordinate bytes"),
            )?))
        } else {
            Ok(u32::from_be_bytes(bytes[range].try_into().map_err(
                |_| invalid_query_err!("Invalid big endian coordinate bytes"),
            )?))
        }
    }

    fn extract_bytes_f64(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> ReadySetResult<f64> {
        let range = start_byte..(start_byte + NUM_OF_BYTES_F64);
        if is_little_endian {
            Ok(f64::from_le_bytes(bytes[range].try_into().map_err(
                |_| invalid_query_err!("Invalid little endian coordinate bytes"),
            )?))
        } else {
            Ok(f64::from_be_bytes(bytes[range].try_into().map_err(
                |_| invalid_query_err!("Invalid big endian coordinate bytes"),
            )?))
        }
    }

    fn extract_number_of_rings(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> ReadySetResult<u32> {
        Self::extract_bytes_u32(&bytes, is_little_endian, start_byte)
    }

    fn extract_number_of_pairs(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> ReadySetResult<u32> {
        if bytes.len() < start_byte + NUM_OF_BYTES_U32 {
            return Err(invalid_query_err!("Invalid polygon byte array size"));
        }
        Self::extract_bytes_u32(&bytes, is_little_endian, start_byte)
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
        let num_pairs = Self::extract_number_of_pairs(&bytes, is_little_endian, start_byte)?; // First 4 bytes
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
        let number_of_rings = Self::extract_number_of_rings(&bytes, is_little_endian, start_byte)?;
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
                        polygon_string.push_str(&format!("SRID={};",srid));
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
                    polygon_string.push_str(&format!(",({})",Self::format_ring(&ring)));
                }
                polygon_string.push(')');
                Ok(polygon_string)
            }
            SqlEngine::MySQL => Err(invalid_query_err!("Unsupported")),
        }
    }
}
