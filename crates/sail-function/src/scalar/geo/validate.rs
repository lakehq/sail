//! WKB validation utilities.
//!
//! This implements basic structural validation of Well-Known Binary geometry data,
//! similar to Spark's WkbReader validation.

use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

#[derive(Debug)]
pub enum WkbValidationError {
    EmptyBuffer,
    InsufficientBytes { expected: usize, actual: usize },
    InvalidByteOrder(u8),
    InvalidGeometryType(u32),
    TooFewPointsInLinestring,
    RingNotClosed,
    TooFewPointsInRing,
    InvalidCoordinateValue,
}

pub type Result<T> = std::result::Result<T, WkbValidationError>;

const MIN_WKB_SIZE: usize = 5; // 1 byte order + 4 byte type

/// Validate basic WKB structure.
/// This checks:
/// - Sufficient buffer length
/// - Valid byte order (0x00 or 0x01)
/// - Valid geometry type ID
/// - Sufficient bytes for declared coordinate counts
///
/// This does NOT validate:
/// - Coordinate values are finite (performance)
/// - Geography bounds (longitude/latitude ranges)
pub fn validate_wkb(wkb: &[u8]) -> Result<()> {
    if wkb.is_empty() {
        return Err(WkbValidationError::EmptyBuffer);
    }

    if wkb.len() < MIN_WKB_SIZE {
        return Err(WkbValidationError::InsufficientBytes {
            expected: MIN_WKB_SIZE,
            actual: wkb.len(),
        });
    }

    let byte_order = wkb[0];
    if byte_order != 0x00 && byte_order != 0x01 {
        return Err(WkbValidationError::InvalidByteOrder(byte_order));
    }

    // Read geometry type
    let mut cursor = Cursor::new(&wkb[1..5]);
    let geom_type = if byte_order == 0x01 {
        cursor
            .read_u32::<LittleEndian>()
            .map_err(|_| WkbValidationError::InsufficientBytes {
                expected: 9,
                actual: wkb.len(),
            })?
    } else {
        cursor
            .read_u32::<BigEndian>()
            .map_err(|_| WkbValidationError::InsufficientBytes {
                expected: 9,
                actual: wkb.len(),
            })?
    };

    // Validate geometry type (basic OGC types: 0-7, plus extended with Z/M flags)
    // The base type must be one of: 0-7 (Simple Features), or 1000-1007, 2000-2007, 3000-3007
    let base_type = geom_type % 1000;
    if base_type > 7 {
        return Err(WkbValidationError::InvalidGeometryType(geom_type));
    }

    // Validate based on geometry type
    match base_type {
        0 => Ok(()),                                       // Empty geometry
        1 => validate_point(wkb, byte_order),              // Point
        2 => validate_linestring(wkb, byte_order),         // LineString
        3 => validate_polygon(wkb, byte_order),            // Polygon
        4 => validate_multipoint(wkb, byte_order),         // MultiPoint
        5 => validate_multilinestring(wkb, byte_order),    // MultiLineString
        6 => validate_multipolygon(wkb, byte_order),       // MultiPolygon
        7 => validate_geometrycollection(wkb, byte_order), // GeometryCollection
        _ => Ok(()),
    }
}

/// Validate geography WKB with coordinate bounds checking.
/// For geography, longitude must be in [-180, 180] and latitude in [-90, 90].
pub fn validate_wkb_geography(wkb: &[u8]) -> Result<()> {
    // First do basic structural validation
    validate_wkb(wkb)?;

    // TODO: Add coordinate bounds validation
    // This requires parsing coordinates, which is more complex.
    // For now, just do structural validation.
    Ok(())
}

fn validate_point(wkb: &[u8], _byte_order: u8) -> Result<()> {
    // Point needs 16 bytes (2 doubles) after header for 2D
    // With Z: 24 bytes (3 doubles)
    // With M: 24 bytes (3 doubles)
    // With ZM: 32 bytes (4 doubles)

    let min_size = if has_z_or_m(wkb) { 9 + 24 } else { 9 + 16 };

    if wkb.len() < min_size {
        return Err(WkbValidationError::InsufficientBytes {
            expected: min_size,
            actual: wkb.len(),
        });
    }

    Ok(())
}

fn validate_linestring(wkb: &[u8], byte_order: u8) -> Result<()> {
    let offset = 5;
    let num_points = read_u32(wkb, offset, byte_order)?;

    // Linestring must have 0 or 2+ points (Spark validation)
    if num_points == 1 {
        return Err(WkbValidationError::TooFewPointsInLinestring);
    }

    // Check we have enough bytes for the coordinates
    let coord_size = if has_z_or_m(wkb) { 24 } else { 16 }; // 3 or 2 doubles
    let expected_size = offset + 4 + (num_points as usize * coord_size);

    if wkb.len() < expected_size {
        return Err(WkbValidationError::InsufficientBytes {
            expected: expected_size,
            actual: wkb.len(),
        });
    }

    Ok(())
}

fn validate_polygon(wkb: &[u8], byte_order: u8) -> Result<()> {
    let offset = 5;
    let num_rings = read_u32(wkb, offset, byte_order)?;

    let mut pos = offset + 4;

    for _ in 0..num_rings {
        if pos + 4 > wkb.len() {
            return Err(WkbValidationError::InsufficientBytes {
                expected: pos + 4,
                actual: wkb.len(),
            });
        }

        let num_points = read_u32(wkb, pos, byte_order)?;

        // Ring must have 4+ points
        if num_points < 4 {
            return Err(WkbValidationError::TooFewPointsInRing);
        }

        let coord_size = if has_z_or_m(wkb) { 24 } else { 16 };
        let ring_size = 4 + (num_points as usize * coord_size);
        pos += ring_size;

        if pos > wkb.len() {
            return Err(WkbValidationError::InsufficientBytes {
                expected: pos,
                actual: wkb.len(),
            });
        }
    }

    Ok(())
}

fn validate_multipoint(wkb: &[u8], byte_order: u8) -> Result<()> {
    validate_geometry_collection_header(wkb, byte_order)
}

fn validate_multilinestring(wkb: &[u8], byte_order: u8) -> Result<()> {
    validate_geometry_collection_header(wkb, byte_order)
}

fn validate_multipolygon(wkb: &[u8], byte_order: u8) -> Result<()> {
    validate_geometry_collection_header(wkb, byte_order)
}

fn validate_geometrycollection(wkb: &[u8], byte_order: u8) -> Result<()> {
    validate_geometry_collection_header(wkb, byte_order)
}

fn validate_geometry_collection_header(wkb: &[u8], byte_order: u8) -> Result<()> {
    let offset = 5;
    let num_geometries = read_u32(wkb, offset, byte_order)?;

    let mut pos = offset + 4;

    for _ in 0..num_geometries {
        // Each sub-geometry starts with its own byte order + type
        if pos + 5 > wkb.len() {
            return Err(WkbValidationError::InsufficientBytes {
                expected: pos + 5,
                actual: wkb.len(),
            });
        }

        // Skip the sub-geometry (we'd need recursive validation for full correctness)
        // For now just check we have at least the header
        pos += 5;
    }

    Ok(())
}

fn has_z_or_m(wkb: &[u8]) -> bool {
    if wkb.len() < 5 {
        return false;
    }

    let byte_order = wkb[0];
    let geom_type = if byte_order == 0x01 {
        let mut cursor = Cursor::new(&wkb[1..5]);
        cursor.read_u32::<LittleEndian>().unwrap_or(0)
    } else {
        let mut cursor = Cursor::new(&wkb[1..5]);
        cursor.read_u32::<BigEndian>().unwrap_or(0)
    };

    // Z flag: 0x80000000, M flag: 0x40000000
    (geom_type & 0x80000000) != 0 || (geom_type & 0x40000000) != 0
}

fn read_u32(wkb: &[u8], offset: usize, byte_order: u8) -> Result<u32> {
    if wkb.len() < offset + 4 {
        return Err(WkbValidationError::InsufficientBytes {
            expected: offset + 4,
            actual: wkb.len(),
        });
    }

    let mut cursor = Cursor::new(&wkb[offset..offset + 4]);
    Ok(if byte_order == 0x01 {
        cursor
            .read_u32::<LittleEndian>()
            .map_err(|_| WkbValidationError::InsufficientBytes {
                expected: offset + 4,
                actual: wkb.len(),
            })?
    } else {
        cursor
            .read_u32::<BigEndian>()
            .map_err(|_| WkbValidationError::InsufficientBytes {
                expected: offset + 4,
                actual: wkb.len(),
            })?
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_point() {
        let wkb: Vec<u8> = vec![
            0x01, 0x01, 0x00, 0x00, 0x00, // Point in little endian
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];

        assert!(validate_wkb(&wkb).is_ok());
    }

    #[test]
    fn test_empty_buffer() {
        let wkb: Vec<u8> = vec![];
        assert!(matches!(
            validate_wkb(&wkb),
            Err(WkbValidationError::EmptyBuffer)
        ));
    }

    #[test]
    fn test_invalid_byte_order() {
        let wkb: Vec<u8> = vec![0x02, 0x01, 0x00, 0x00, 0x00];
        assert!(matches!(
            validate_wkb(&wkb),
            Err(WkbValidationError::InvalidByteOrder(0x02))
        ));
    }

    #[test]
    fn test_too_few_points_linestring() {
        // LineString with only 1 point (invalid)
        let wkb: Vec<u8> = vec![
            0x01, // byte order
            0x02, 0x00, 0x00, 0x00, // type 2 (LineString)
            0x01, 0x00, 0x00, 0x00, // 1 point (invalid!)
        ];

        assert!(matches!(
            validate_wkb(&wkb),
            Err(WkbValidationError::TooFewPointsInLinestring)
        ));
    }
}
