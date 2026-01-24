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
///
/// As of (Jan 2025) PostGIS Polygon is supported, extending support to MySQL Polygon is straightforward.
use readyset_data::dialect::SqlEngine;
use readyset_errors::{invalid_query_err, ReadySetResult};

use super::point::Point;
use super::polygon::Polygon;

pub enum SpatialType {
    PostgisPoint,
    PostgisPolygon,
    MysqlPoint,
}

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
    /// https://github.com/postgis/postgis/blob/54c1f5671c6ffc7617621bc09a685872cf7695ac/liblwgeom/liblwgeom.h.in#L121-L127
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct PostgisTypeFlags: u32 {
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
pub(crate) fn extract_srid(bytes: &[u8], is_little_endian: bool) -> ReadySetResult<Option<u32>> {
    let type_code = if is_little_endian {
        u32::from_le_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| invalid_query_err!("Invalid spatial type"))?,
        )
    } else {
        u32::from_be_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| invalid_query_err!("Invalid spatial type"))?,
        )
    };

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
        return Err(invalid_query_err!("Not supporting postgis bounding boxes"));
    }
    if flags.contains(PostgisTypeFlags::HAS_Z) {
        return Err(invalid_query_err!(
            "Not supporting postgis geometrical shapes with Z dimensions"
        ));
    }
    if flags.contains(PostgisTypeFlags::HAS_M) {
        return Err(invalid_query_err!(
            "Not supporting postgis geometrical shapes with M dimensions"
        ));
    }

    Ok(srid)
}

fn try_get_spatial_type_from_mysql(bytes: &[u8]) -> ReadySetResult<SpatialType> {
    if bytes.len() < 9 {
        return Err(invalid_query_err!("Input too short for spatial type"));
    }

    let wkb_type = u32::from_le_bytes(
        bytes[5..9]
            .try_into()
            .map_err(|_| invalid_query_err!("Invalid spatial type bytes"))?,
    );

    match wkb_type {
        1 => Ok(SpatialType::MysqlPoint),
        _ => Err(invalid_query_err!(
            "Unsupported spatial type: {:?}",
            wkb_type
        )),
    }
}

fn try_get_spatial_type_from_postgres(bytes: &[u8]) -> ReadySetResult<SpatialType> {
    if bytes.len() < 5 {
        return Err(invalid_query_err!("Input too short for spatial type"));
    }
    let wkb_type = u32::from_le_bytes(
        bytes[1..5]
            .try_into()
            .map_err(|_| invalid_query_err!("Invalid spatial type"))?,
    );

    let geometry_type = wkb_type & 0xFF;
    match geometry_type {
        1 => Ok(SpatialType::PostgisPoint),
        3 => Ok(SpatialType::PostgisPolygon),
        _ => Err(invalid_query_err!(
            "Unsupported spatial type: {:?}",
            wkb_type
        )),
    }
}

fn try_get_spatial_type(bytes: &[u8], engine: SqlEngine) -> ReadySetResult<SpatialType> {
    match engine {
        SqlEngine::MySQL => try_get_spatial_type_from_mysql(bytes),
        SqlEngine::PostgreSQL => try_get_spatial_type_from_postgres(bytes),
    }
}

pub(crate) fn try_get_spatial_text(
    bytes: &[u8],
    engine: SqlEngine,
    flags: bool,
) -> ReadySetResult<String> {
    let spatial_type = try_get_spatial_type(bytes, engine)?;
    match spatial_type {
        SpatialType::MysqlPoint | SpatialType::PostgisPoint => {
            let point = Point::try_from_bytes(bytes, engine)?;
            Ok(point.format(engine, flags)?)
        }
        SpatialType::PostgisPolygon => {
            let polygon = Polygon::try_from_postgis_bytes(bytes)?;
            Ok(polygon.format(engine, flags)?)
        }
    }
}
