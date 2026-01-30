//! Polygon geometry type support.

use super::{Pair, ReadysetSpatialError, extract_srid};

const NUM_OF_BYTES_U32: usize = std::mem::size_of::<u32>();
const NUM_OF_BYTES_F64: usize = std::mem::size_of::<f64>();
const NUM_OF_BYTES_PAIR: usize = std::mem::size_of::<Pair>();

/// Polygon structure is composed of one external ring and zero-or-more holes within the external initial ring.
/// For a Ring to be considered valid, the number of points should be at least 4 points and the ending point
/// must be the same value of the starting point.
///
/// Examples:
/// ```sql
/// -- valid
/// SELECT ST_IsValid('POLYGON ((20 180, 180 180, 180 20, 20 20, 20 180))');
///
/// -- valid (with holes)
/// SELECT ST_IsValid(ST_GeomFromEWKT('POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(6 6,8 6,8 8,6 8,6 6))'));
///
/// -- invalid (parse error)
/// SELECT ST_IsValid('POLYGON ((20 180, 180 180, 180 20, 20 20, 30, 40))');
///
/// -- invalid (not enough points)
/// SELECT ST_IsValid('POLYGON ((20 180, 180 180, 20 180))');
/// ```
///
/// References:
/// - <https://postgis.net/docs/manual-3.6/using_postgis_dbmanagement.html#PostGIS_Geometry> (Section 4.4. Geometry Validation)
/// - <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>
/// - <https://libgeos.org/specifications/wkb/>
#[derive(Debug, Clone)]
pub(crate) struct Polygon {
    exterior_ring: Vec<Pair>,
    holes: Vec<Vec<Pair>>,
    srid: Option<u32>,
}

impl Polygon {
    /// Create a new `Polygon` from a postgis byte array. This is the data we receive from the
    /// wal logical replication. The actual format is based on the postgis notion of
    /// "Extended Well-Known Binary" (EWKB).
    ///
    /// Note: we are currently only support 2D Postgis polygons.
    ///
    /// Reference: <https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT>
    pub(crate) fn try_from_postgis_bytes(bytes: &[u8]) -> Result<Self, ReadysetSpatialError> {
        // Empty polygon have 9 Bytes long
        // 01 03 00 00 00 00 00 00 00
        if bytes.len() < 9 {
            return Err(ReadysetSpatialError::InvalidInput(format!(
                "Polygon argument must be at least 9 bytes long, was {:?}",
                bytes.len()
            )));
        }

        // tells us the byte order of the pair fields (X, Y).
        let is_little_endian = bytes[0] == 0x01;
        let srid = extract_srid(bytes, is_little_endian)?;
        let (external_ring, internal_rings) = Self::extract_rings(bytes, is_little_endian, srid)?;

        Ok(Polygon {
            exterior_ring: external_ring,
            holes: internal_rings,
            srid,
        })
    }

    fn extract_bytes_u32(
        bytes: &[u8],
        is_little_endian: bool,
        start: usize,
    ) -> Result<u32, ReadysetSpatialError> {
        let end = start + NUM_OF_BYTES_U32;
        let arr: [u8; 4] = bytes
            .get(start..end)
            .ok_or_else(|| {
                ReadysetSpatialError::InvalidInput(format!(
                    "Insufficient bytes for u32 at offset {}",
                    start
                ))
            })?
            .try_into()
            .map_err(|_| {
                ReadysetSpatialError::InvalidInput("Invalid byte slice length for u32".into())
            })?;

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
    ) -> Result<f64, ReadysetSpatialError> {
        let end = start + NUM_OF_BYTES_F64;
        let arr: [u8; 8] = bytes
            .get(start..end)
            .ok_or_else(|| {
                ReadysetSpatialError::InvalidInput(format!(
                    "Insufficient bytes for f64 at offset {}",
                    start
                ))
            })?
            .try_into()
            .map_err(|_| {
                ReadysetSpatialError::InvalidInput("Invalid byte slice length for f64".into())
            })?;

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
    ) -> Result<Pair, ReadysetSpatialError> {
        if bytes.len() < start_byte + NUM_OF_BYTES_PAIR {
            return Err(ReadysetSpatialError::InvalidInput(
                "Invalid polygon byte array size".into(),
            ));
        }
        let x = Self::extract_bytes_f64(bytes, is_little_endian, start_byte)?;
        let y = Self::extract_bytes_f64(bytes, is_little_endian, start_byte + NUM_OF_BYTES_F64)?;

        Ok(Pair { x, y })
    }

    fn extract_ring(
        bytes: &[u8],
        is_little_endian: bool,
        start_byte: usize,
    ) -> Result<Vec<Pair>, ReadysetSpatialError> {
        let num_pairs = Self::extract_bytes_u32(bytes, is_little_endian, start_byte)?; // First 4 bytes
        let mut pairs: Vec<Pair> = Vec::new();
        let mut offset = start_byte + NUM_OF_BYTES_U32;
        for _num in 0..num_pairs {
            let pair = Self::extract_pair(bytes, is_little_endian, offset)?;
            pairs.push(pair);
            offset += NUM_OF_BYTES_PAIR; // 16 byte for each pair
        }
        Ok(pairs)
    }

    fn extract_rings(
        bytes: &[u8],
        is_little_endian: bool,
        srid: Option<u32>,
    ) -> Result<(Vec<Pair>, Vec<Vec<Pair>>), ReadysetSpatialError> {
        let mut start_byte: usize = 9;
        if srid.is_none() {
            start_byte = 5;
        }
        let number_of_rings = Self::extract_bytes_u32(bytes, is_little_endian, start_byte)?;
        if number_of_rings == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // extract exterior ring
        let mut offset = start_byte + NUM_OF_BYTES_U32;
        let external_ring = Self::extract_ring(bytes, is_little_endian, offset)?;

        // extract internal rings
        let mut internal_rings: Vec<Vec<Pair>> = Vec::new();
        let mut previous_ring_size = external_ring.len() * NUM_OF_BYTES_PAIR + NUM_OF_BYTES_U32;
        for _ring_num in 1..number_of_rings {
            offset += previous_ring_size;
            let ring = Self::extract_ring(bytes, is_little_endian, offset)?;
            previous_ring_size = ring.len() * NUM_OF_BYTES_PAIR + NUM_OF_BYTES_U32;
            internal_rings.push(ring);
        }

        Ok((external_ring, internal_rings))
    }

    fn format_ring(ring: &[Pair]) -> String {
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

    /// Format the polygon as a PostGIS WKT or EWKT string.
    ///
    /// If `print_srid` is true and an SRID is present, returns EWKT format with SRID prefix.
    pub fn format_postgis(&self, print_srid: bool) -> String {
        let mut polygon_string = String::new();
        if let Some(srid) = self.srid.filter(|_| print_srid) {
            polygon_string.push_str(&format!("SRID={};", srid));
        }
        polygon_string.push_str("POLYGON");

        // handle empty polygon
        if self.exterior_ring.is_empty() {
            polygon_string.push_str(" EMPTY");
            return polygon_string;
        }

        polygon_string.push_str(&format!("(({})", Self::format_ring(&self.exterior_ring)));

        for ring in &self.holes {
            polygon_string.push_str(&format!(",({})", Self::format_ring(ring)));
        }
        polygon_string.push(')');
        polygon_string
    }
}

/// Write a ring (sequence of coordinate pairs) to the byte buffer.
///
/// # Debug Assertions
///
/// In debug builds, this function asserts:
/// - The ring has at least 4 points (OGC Simple Features requirement)
/// - The ring is closed (first point == last point)
/// - All coordinates are finite (not NaN or Inf)
///
/// These assertions help catch invalid input during development and testing.
/// In release builds, invalid input will produce invalid EWKB bytes.
///
/// For use primarily in testing and data generation.
fn write_ring_bytes(bytes: &mut Vec<u8>, ring: &[(f64, f64)], little_endian: bool) {
    // Debug assertions for polygon ring validity
    debug_assert!(
        ring.len() >= 4,
        "Polygon ring must have at least 4 points (including closing point), got {}",
        ring.len()
    );

    if let (Some(first), Some(last)) = (ring.first(), ring.last()) {
        debug_assert!(
            first.0 == last.0 && first.1 == last.1,
            "Polygon ring must be closed (first point must equal last point). First: ({}, {}), Last: ({}, {})",
            first.0,
            first.1,
            last.0,
            last.1
        );
    }

    debug_assert!(
        ring.iter().all(|(x, y)| x.is_finite() && y.is_finite()),
        "All polygon coordinates must be finite (not NaN or Inf)"
    );

    let num_points = ring.len() as u32;

    if little_endian {
        bytes.extend_from_slice(&num_points.to_le_bytes());
    } else {
        bytes.extend_from_slice(&num_points.to_be_bytes());
    }

    for (x, y) in ring {
        if little_endian {
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        } else {
            bytes.extend_from_slice(&x.to_be_bytes());
            bytes.extend_from_slice(&y.to_be_bytes());
        }
    }
}

/// Create EWKB bytes for a PostGIS Polygon.
///
/// This is a low-level function that expects valid input. It is intended for use in
/// testing and data generation where callers ensure polygon validity.
///
/// # Polygon Validity Requirements
///
/// A valid polygon ring must:
/// - Have at least 4 points (OGC Simple Features requirement for a triangle)
/// - Be closed (last point must equal first point)
/// - Have all coordinates finite (not NaN or Inf)
///
/// The `exterior_ring` parameter should already include the closing point.
///
/// # Format (little-endian, with SRID)
///
/// - 1 byte: byte order (0x01 = little-endian, 0x00 = big-endian)
/// - 4 bytes: type code (0x20000003 = Polygon with SRID, 0x00000003 = Polygon without SRID)
/// - 4 bytes: SRID (only if type has SRID flag)
/// - 4 bytes: number of rings
/// - For each ring:
///   - 4 bytes: number of points
///   - For each point: 8 bytes X + 8 bytes Y
///
/// For use primarily in testing and data generation.
pub fn make_postgis_polygon_bytes(
    exterior_ring: Option<&[(f64, f64)]>,
    holes: Option<&[Vec<(f64, f64)>]>,
    srid: Option<u32>,
    little_endian: bool,
) -> Vec<u8> {
    // Calculate exact capacity to avoid reallocations
    // Header: 1 (byte order) + 4 (type code) + 4 (SRID if present) + 4 (num rings)
    let header_size = 1 + 4 + if srid.is_some() { 4 } else { 0 } + 4;

    // Each ring: 4 (num points) + 16 * num_points (8 bytes per coordinate)
    let ring_size = |points: usize| 4 + 16 * points;

    let rings_size = if let Some(exterior) = exterior_ring {
        let exterior_size = ring_size(exterior.len());
        let holes_size = holes
            .map(|h| h.iter().map(|hole| ring_size(hole.len())).sum())
            .unwrap_or(0);
        exterior_size + holes_size
    } else {
        0
    };

    let mut bytes = Vec::with_capacity(header_size + rings_size);

    // Byte order
    bytes.push(if little_endian { 0x01 } else { 0x00 });

    // Type code: Polygon (3) with SRID flag (0x20000000) if SRID is present
    let type_code: u32 = if srid.is_some() { 0x20000003 } else { 3 };
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

    // Rings
    if let Some(exterior) = exterior_ring {
        let mut num_rings: u32 = 1;
        if let Some(h) = holes {
            num_rings += h.len() as u32;
        }

        if little_endian {
            bytes.extend_from_slice(&num_rings.to_le_bytes());
        } else {
            bytes.extend_from_slice(&num_rings.to_be_bytes());
        }

        write_ring_bytes(&mut bytes, exterior, little_endian);

        if let Some(h) = holes {
            for hole in h {
                write_ring_bytes(&mut bytes, hole, little_endian);
            }
        }
    } else {
        // Empty polygon - 0 rings
        if little_endian {
            bytes.extend_from_slice(&0u32.to_le_bytes());
        } else {
            bytes.extend_from_slice(&0u32.to_be_bytes());
        }
    }

    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_empty_polygon() {
        let bytes = make_postgis_polygon_bytes(None, None, None, true);
        let polygon = Polygon::try_from_postgis_bytes(&bytes).expect("valid empty polygon");
        let format = polygon.format_postgis(false);
        assert_eq!(format, "POLYGON EMPTY");
    }

    #[test]
    fn test_valid_empty_polygon_with_srid() {
        let bytes = make_postgis_polygon_bytes(None, None, Some(4326), true);
        let polygon =
            Polygon::try_from_postgis_bytes(&bytes).expect("valid empty polygon with SRID");
        let format = polygon.format_postgis(true);
        assert_eq!(format, "SRID=4326;POLYGON EMPTY");
    }

    #[test]
    fn test_invalid_empty_polygon() {
        let bytes = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        assert!(Polygon::try_from_postgis_bytes(&bytes).is_err());
    }

    #[test]
    fn test_valid_polygon_le_be() {
        let exterior_ring: Vec<(f64, f64)> = vec![
            (0.0, 0.0),
            (0.0, 1000.0),
            (1000.0, 1000.0),
            (1000.0, 0.0),
            (0.0, 0.0),
        ];

        let holes: Vec<Vec<(f64, f64)>> = vec![vec![
            (200.0, 200.0),
            (200.0, 800.0),
            (800.0, 800.0),
            (800.0, 200.0),
            (200.0, 200.0),
        ]];

        // little endian
        let bytes_le = make_postgis_polygon_bytes(Some(&exterior_ring), Some(&holes), None, true);
        let polygon_le =
            Polygon::try_from_postgis_bytes(&bytes_le).expect("valid polygon little endian");

        // big endian
        let bytes_be = make_postgis_polygon_bytes(Some(&exterior_ring), Some(&holes), None, false);
        let polygon_be =
            Polygon::try_from_postgis_bytes(&bytes_be).expect("valid polygon big endian");

        assert_eq!(
            polygon_le.format_postgis(false),
            "POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))"
        );
        assert_eq!(
            polygon_be.format_postgis(false),
            "POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))"
        );
    }

    #[test]
    fn test_valid_polygon_le_be_with_srid() {
        let exterior_ring: Vec<(f64, f64)> = vec![
            (0.0, 0.0),
            (0.0, 1000.0),
            (1000.0, 1000.0),
            (1000.0, 0.0),
            (0.0, 0.0),
        ];

        let holes: Vec<Vec<(f64, f64)>> = vec![vec![
            (200.0, 200.0),
            (200.0, 800.0),
            (800.0, 800.0),
            (800.0, 200.0),
            (200.0, 200.0),
        ]];

        // little endian
        let bytes_le =
            make_postgis_polygon_bytes(Some(&exterior_ring), Some(&holes), Some(3857), true);
        let polygon_le = Polygon::try_from_postgis_bytes(&bytes_le)
            .expect("valid polygon with SRID little endian");

        // big endian
        let bytes_be =
            make_postgis_polygon_bytes(Some(&exterior_ring), Some(&holes), Some(3857), false);
        let polygon_be =
            Polygon::try_from_postgis_bytes(&bytes_be).expect("valid polygon with SRID big endian");

        assert_eq!(
            polygon_le.format_postgis(true),
            "SRID=3857;POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))"
        );
        assert_eq!(
            polygon_be.format_postgis(true),
            "SRID=3857;POLYGON((0 0,0 1000,1000 1000,1000 0,0 0),(200 200,200 800,800 800,800 200,200 200))"
        );
    }

    #[test]
    fn test_invalid_polygon_bytes() {
        let bytes = vec![1u8; 20]; // invalid bytes
        assert!(Polygon::try_from_postgis_bytes(&bytes).is_err());
    }

    mod ewkb_polygon_builder {
        use crate::SpatialType;

        use super::*;

        #[test]
        fn empty_polygon_no_srid() {
            let bytes = make_postgis_polygon_bytes(None, None, None, true);

            assert_eq!(bytes.len(), 9); // 1 + 4 + 4 (byte order + type + num_rings)
            assert_eq!(bytes[0], 0x01); // little-endian
            assert_eq!(&bytes[1..5], &3u32.to_le_bytes()); // type code: Polygon (3)
            assert_eq!(&bytes[5..9], &0u32.to_le_bytes()); // 0 rings
        }

        #[test]
        fn empty_polygon_with_srid() {
            let bytes = make_postgis_polygon_bytes(None, None, Some(4326), true);

            assert_eq!(bytes.len(), 13); // 1 + 4 + 4 + 4 (+ SRID)
            assert_eq!(bytes[0], 0x01);
            assert_eq!(&bytes[1..5], &0x20000003u32.to_le_bytes()); // type with SRID flag
            assert_eq!(&bytes[5..9], &4326u32.to_le_bytes()); // SRID
            assert_eq!(&bytes[9..13], &0u32.to_le_bytes()); // 0 rings
        }

        #[test]
        fn simple_triangle_little_endian() {
            let ring: [(f64, f64); 4] = [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
            let bytes = make_postgis_polygon_bytes(Some(&ring), None, None, true);

            // Expected size: 1 + 4 + 4 + 4 + (4 * 16) = 77
            assert_eq!(bytes.len(), 77);
            assert_eq!(bytes[0], 0x01); // little-endian
            assert_eq!(&bytes[1..5], &3u32.to_le_bytes()); // Polygon type
            assert_eq!(&bytes[5..9], &1u32.to_le_bytes()); // 1 ring
            assert_eq!(&bytes[9..13], &4u32.to_le_bytes()); // 4 points in ring
        }

        #[test]
        fn simple_triangle_big_endian() {
            let ring: [(f64, f64); 4] = [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
            let bytes = make_postgis_polygon_bytes(Some(&ring), None, None, false);

            assert_eq!(bytes.len(), 77);
            assert_eq!(bytes[0], 0x00); // big-endian
            assert_eq!(&bytes[1..5], &3u32.to_be_bytes()); // Polygon type
            assert_eq!(&bytes[5..9], &1u32.to_be_bytes()); // 1 ring
            assert_eq!(&bytes[9..13], &4u32.to_be_bytes()); // 4 points in ring
        }

        #[test]
        fn polygon_with_hole() {
            let exterior: Vec<(f64, f64)> = vec![
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ];
            let hole: Vec<(f64, f64)> =
                vec![(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)];
            let holes = vec![hole];

            let bytes = make_postgis_polygon_bytes(Some(&exterior), Some(&holes), None, true);

            // 1 + 4 + 4 + (4 + 5*16) + (4 + 5*16) = 1 + 4 + 4 + 84 + 84 = 177
            assert_eq!(bytes.len(), 177);
            assert_eq!(&bytes[5..9], &2u32.to_le_bytes()); // 2 rings
        }

        #[test]
        fn capacity_is_exact() {
            let ring: [(f64, f64); 4] = [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
            let bytes = make_postgis_polygon_bytes(Some(&ring), None, Some(4326), true);
            assert_eq!(bytes.len(), bytes.capacity());
        }

        #[test]
        fn detects_polygon_little_endian() {
            let ring: [(f64, f64); 4] = [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
            let bytes = make_postgis_polygon_bytes(Some(&ring), None, None, true);
            let spatial_type = crate::try_get_spatial_type_from_postgres(&bytes)
                .expect("should detect polygon type");
            assert_eq!(spatial_type, SpatialType::PostgisPolygon);
        }

        #[test]
        fn detects_polygon_big_endian() {
            let ring: [(f64, f64); 4] = [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
            let bytes = make_postgis_polygon_bytes(Some(&ring), None, None, false);
            let spatial_type = crate::try_get_spatial_type_from_postgres(&bytes)
                .expect("should detect polygon type with big-endian");
            assert_eq!(spatial_type, SpatialType::PostgisPolygon);
        }

        #[test]
        fn detects_polygon_with_srid_big_endian() {
            let ring: [(f64, f64); 4] = [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
            let bytes = make_postgis_polygon_bytes(Some(&ring), None, Some(4326), false);
            let spatial_type = crate::try_get_spatial_type_from_postgres(&bytes)
                .expect("should detect polygon type with SRID big-endian");
            assert_eq!(spatial_type, SpatialType::PostgisPolygon);
        }
    }
}
