//! Spatial data type support for Readyset.
//!
//! As of this initial writing (May 2025), the goal with supporting
//! spatial data types is only to support the basic data types themselves,
//! and a handful of simple, "display" functions. We are explicitly not
//! supporting operators, functions, nor spatial indices over these types.
//! As Readyset is a cache, we feel there is little to no value in supporting
//! those things as the likelihood of a successful cache hit rate is low.
//!
//! We do, however, need to support the basic data types themselves, primarily
//! to allow snapshotting/replicating the data. Supporting the simple display
//! functions, like `ST_AsText()`, are reasonable to support and actually do make
//! sense within the scope of Readyset.
//!
//! In the current implementation, we take the byte array directly from the upstream,
//! as-is, and store that on disk (`DfValue::ByteArray`), as well as pass that byte array
//! down through the dataflow graph. We do not attempt to create a canonical representation of
//! any of the various upstream formats. This is all for simplicity, as the only time
//! we need to interpret those bytes is in the various display functions, implemented here.
//!
//! Further, the current implementation only supports `Point` and `Polygon` data types,
//! although extending this to support other spatial/geometric types is fairly straightforward.
//!
//! We do not plan on supporting the postgres native data types, but instead support `PostGIS`.
//! The former being legacy, while the latter is state-of-the-art.
//!
//! As of (Jan 2025) PostGIS Polygon is supported, extending support to MySQL Polygon is straightforward.
//!
//! # WKB Byte Order
//!
//! Both MySQL and PostGIS use Well-Known Binary (WKB) format for geometry storage.
//! The WKB spec (OGC Simple Features) allows either byte order:
//! - `0x00` = Big-endian (XDR)
//! - `0x01` = Little-endian (NDR)
//!
//! The byte order flag determines how to interpret all multi-byte fields in the WKB
//! portion (type code, coordinates). We must respect this flag when parsing.
//!
//! **MySQL-specific notes:**
//! - MySQL internally stores geometries as little-endian and binlog replication data
//!   will typically be little-endian.
//! - The SRID prefix (first 4 bytes) is always little-endian regardless of the WKB byte order.
//! - MySQL has had historical bugs with big-endian WKB handling (see MySQL bugs #12839, #107435).
//!
//! **PostGIS-specific notes:**
//! - PostGIS uses EWKB (Extended WKB) which embeds the SRID within the WKB structure.
//! - The SRID in EWKB respects the byte order flag (unlike MySQL's prefix).
//!
//! References:
//! - <https://github.com/postgis/postgis/blob/master/doc/ZMSgeoms.txt> (for ewkb)
//! - <https://github.com/postgis/postgis/blob/master/doc/bnf-wkb.txt> (for wkb)
//! - <https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html> (MySQL WKB format)

mod point;
mod polygon;

use bitflags::bitflags;
use point::Point;
use polygon::Polygon;

// Re-export builder functions for testing and data generation
pub use point::{make_mysql_point_bytes, make_postgis_point_bytes};
pub use polygon::make_postgis_polygon_bytes;
use thiserror::Error;

/// Error type for spatial operations in the readyset-spatial crate.
#[derive(Debug, Error)]
pub enum ReadysetSpatialError {
    /// Invalid input data (malformed bytes, wrong length, etc.)
    #[error("{0}")]
    InvalidInput(String),
    /// Unsupported spatial operation or type
    #[error("{0}")]
    Unsupported(String),
}

/// WGS 84 - the most common SRID for geographic coordinates (latitude/longitude)
pub const SRID_WGS84: u32 = 4326;

/// Enumeration of supported spatial types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialType {
    /// PostGIS Point geometry
    PostgisPoint,
    /// PostGIS Polygon geometry
    PostgisPolygon,
    /// MySQL Point geometry
    MysqlPoint,
}

/// A coordinate pair (x, y).
#[derive(Debug, Clone)]
pub(crate) struct Pair {
    pub(crate) x: f64,
    pub(crate) y: f64,
}

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
    /// <https://github.com/postgis/postgis/blob/54c1f5671c6ffc7617621bc09a685872cf7695ac/liblwgeom/liblwgeom.h.in#L121-L127>
    #[derive(Debug, Clone, Copy)]
    struct PostgisTypeFlags: u32 {
        const HAS_BBOX = 0x10000000;
        const HAS_SRID = 0x20000000;
        const HAS_Z = 0x40000000;
        const HAS_M = 0x80000000;
    }
}

/// Extracts the SRID from a PostGIS byte array and validates the dimension flags.
///
/// Returns the SRID if present, or None if not. Errors if:
/// - Has bounding box
/// - Has Z dimension
/// - Has M dimension
pub(crate) fn extract_srid(
    bytes: &[u8],
    is_little_endian: bool,
) -> Result<Option<u32>, ReadysetSpatialError> {
    let type_code = if is_little_endian {
        u32::from_le_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid spatial type".into()))?,
        )
    } else {
        u32::from_be_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid spatial type".into()))?,
        )
    };

    let flags = PostgisTypeFlags::from_bits_retain(type_code);

    // Extract SRID if present - it obeys the byte order of the rest of the data block.
    let srid = if flags.contains(PostgisTypeFlags::HAS_SRID) {
        if is_little_endian {
            Some(u32::from_le_bytes(bytes[5..9].try_into().map_err(
                |_| ReadysetSpatialError::InvalidInput("Invalid SRID".into()),
            )?))
        } else {
            Some(u32::from_be_bytes(bytes[5..9].try_into().map_err(
                |_| ReadysetSpatialError::InvalidInput("Invalid SRID".into()),
            )?))
        }
    } else {
        None
    };

    // Validate dimension flags
    if flags.contains(PostgisTypeFlags::HAS_BBOX) {
        return Err(ReadysetSpatialError::InvalidInput(
            "Not supporting postgis bounding boxes".into(),
        ));
    }
    if flags.contains(PostgisTypeFlags::HAS_Z) {
        return Err(ReadysetSpatialError::InvalidInput(
            "Not supporting postgis geometrical shapes with Z dimensions".into(),
        ));
    }
    if flags.contains(PostgisTypeFlags::HAS_M) {
        return Err(ReadysetSpatialError::InvalidInput(
            "Not supporting postgis geometrical shapes with M dimensions".into(),
        ));
    }

    Ok(srid)
}

pub(crate) fn try_get_spatial_type_from_mysql(
    bytes: &[u8],
) -> Result<SpatialType, ReadysetSpatialError> {
    if bytes.len() < 9 {
        return Err(ReadysetSpatialError::InvalidInput(
            "Input too short for spatial type".into(),
        ));
    }

    // WKB type respects the byte order flag at bytes[4]
    let is_little_endian = bytes[4] == 0x01;
    let type_bytes: [u8; 4] = bytes[5..9]
        .try_into()
        .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid spatial type bytes".into()))?;
    let wkb_type = if is_little_endian {
        u32::from_le_bytes(type_bytes)
    } else {
        u32::from_be_bytes(type_bytes)
    };

    match wkb_type {
        1 => Ok(SpatialType::MysqlPoint),
        _ => Err(ReadysetSpatialError::Unsupported(format!(
            "Unsupported spatial type: {:?}",
            wkb_type
        ))),
    }
}

pub(crate) fn try_get_spatial_type_from_postgres(
    bytes: &[u8],
) -> Result<SpatialType, ReadysetSpatialError> {
    if bytes.len() < 5 {
        return Err(ReadysetSpatialError::InvalidInput(
            "Input too short for spatial type".into(),
        ));
    }

    // Byte order flag at bytes[0]: 0x01 = little-endian, 0x00 = big-endian
    let is_little_endian = bytes[0] == 0x01;
    let type_bytes: [u8; 4] = bytes[1..5]
        .try_into()
        .map_err(|_| ReadysetSpatialError::InvalidInput("Invalid spatial type".into()))?;
    let wkb_type = if is_little_endian {
        u32::from_le_bytes(type_bytes)
    } else {
        u32::from_be_bytes(type_bytes)
    };

    let geometry_type = wkb_type & 0xFF;
    match geometry_type {
        1 => Ok(SpatialType::PostgisPoint),
        3 => Ok(SpatialType::PostgisPolygon),
        _ => Err(ReadysetSpatialError::Unsupported(format!(
            "Unsupported spatial type: {:?}",
            wkb_type
        ))),
    }
}

/// Convert MySQL spatial bytes to text representation (WKT format).
///
/// This function parses the raw bytes and formats them as WKT (Well-Known Text).
pub fn try_get_mysql_spatial_text(bytes: &[u8]) -> Result<String, ReadysetSpatialError> {
    let spatial_type = try_get_spatial_type_from_mysql(bytes)?;
    match spatial_type {
        SpatialType::MysqlPoint => {
            let point = Point::try_from_mysql_bytes(bytes)?;
            Ok(point.format_mysql())
        }
        _ => Err(ReadysetSpatialError::Unsupported(format!(
            "Unsupported MySQL spatial type: {:?}",
            spatial_type
        ))),
    }
}

/// Convert PostGIS spatial bytes to text representation (WKT or EWKT format).
///
/// This function parses the raw bytes and formats them as WKT (Well-Known Text)
/// or EWKT (Extended Well-Known Text) depending on the `print_srid` flag.
pub fn try_get_postgis_spatial_text(
    bytes: &[u8],
    print_srid: bool,
) -> Result<String, ReadysetSpatialError> {
    let spatial_type = try_get_spatial_type_from_postgres(bytes)?;
    match spatial_type {
        SpatialType::PostgisPoint => {
            let point = Point::try_from_postgis_bytes(bytes)?;
            Ok(point.format_postgis(print_srid))
        }
        SpatialType::PostgisPolygon => {
            let polygon = Polygon::try_from_postgis_bytes(bytes)?;
            Ok(polygon.format_postgis(print_srid))
        }
        _ => Err(ReadysetSpatialError::Unsupported(format!(
            "Unsupported PostGIS spatial type: {:?}",
            spatial_type
        ))),
    }
}
