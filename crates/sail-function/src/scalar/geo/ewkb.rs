//! EWKB (Extended Well-Known Binary) utilities for handling per-row SRID.
//!
//! EWKB is PostGIS-style: the SRID is embedded in the binary after the byte order
//! and geometry type, using a flag (0x20000000) to indicate SRID presence.
//!
//! Format: [byte_order][type|0x20000000][srid][...rest of WKB...]
//!
//! This is used internally for `Geometry(ANY)` columns where different rows may
//! have different SRIDs. For columns with a specific SRID (e.g., Geometry(4326)),
//! we store plain WKB without the SRID overhead.

const EWKB_SRID_FLAG: u32 = 0x20000000;

#[derive(Debug)]
pub enum EWkbError {
    InvalidByteOrder(u8),
    InvalidWkb,
    InsufficientBytes,
}

pub type Result<T> = std::result::Result<T, EWkbError>;

/// Returns true if the EWKB flag is set (SRID is present)
fn has_srid_flag(type_with_flags: u32) -> bool {
    (type_with_flags & EWKB_SRID_FLAG) != 0
}

/// Extract the base geometry type by stripping the SRID flag
fn base_type(type_with_flags: u32) -> u32 {
    type_with_flags & !EWKB_SRID_FLAG
}

/// Read a u32 from bytes at a given offset, handling endianness
fn read_u32_le(bytes: &[u8], offset: usize) -> Option<u32> {
    if bytes.len() < offset + 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]))
}

fn read_u32_be(bytes: &[u8], offset: usize) -> Option<u32> {
    if bytes.len() < offset + 4 {
        return None;
    }
    Some(u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]))
}

/// Write a u32 to bytes at a given offset, handling endianness
fn write_u32_le(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_u32_be(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

/// Convert plain WKB to EWKB by inserting SRID.
///
/// WKB: [byte_order][type][...coords...]
/// EWKB: [byte_order][type|0x20000000][srid][...coords...]
pub fn wkb_to_ewkb(wkb: &[u8], srid: i32) -> Result<Vec<u8>> {
    if wkb.is_empty() {
        return Err(EWkbError::InsufficientBytes);
    }

    let byte_order = wkb[0];
    if byte_order != 0x00 && byte_order != 0x01 {
        return Err(EWkbError::InvalidByteOrder(byte_order));
    }

    if wkb.len() < 5 {
        return Err(EWkbError::InsufficientBytes);
    }

    let geom_type = if byte_order == 0x01 {
        read_u32_le(wkb, 1).ok_or(EWkbError::InsufficientBytes)?
    } else {
        read_u32_be(wkb, 1).ok_or(EWkbError::InsufficientBytes)?
    };

    // Set SRID flag
    let geom_type_with_flag = geom_type | EWKB_SRID_FLAG;

    let mut output = Vec::with_capacity(wkb.len() + 4);

    // Copy byte order
    output.push(byte_order);

    // Write modified type
    if byte_order == 0x01 {
        write_u32_le(&mut output, geom_type_with_flag);
    } else {
        write_u32_be(&mut output, geom_type_with_flag);
    }

    // Write SRID
    if byte_order == 0x01 {
        output.extend_from_slice(&srid.to_le_bytes());
    } else {
        output.extend_from_slice(&srid.to_be_bytes());
    }

    // Copy rest of the original WKB
    output.extend_from_slice(&wkb[5..]);

    Ok(output)
}

/// Convert EWKB to plain WKB by removing SRID.
///
/// Returns error if the SRID flag is not set.
pub fn ewkb_to_wkb(ewkb: &[u8]) -> Result<Vec<u8>> {
    if ewkb.is_empty() {
        return Err(EWkbError::InsufficientBytes);
    }

    let byte_order = ewkb[0];
    if byte_order != 0x00 && byte_order != 0x01 {
        return Err(EWkbError::InvalidByteOrder(byte_order));
    }

    if ewkb.len() < 9 {
        return Err(EWkbError::InsufficientBytes);
    }

    // Read geometry type with flags
    let geom_type_with_flags = if byte_order == 0x01 {
        read_u32_le(ewkb, 1).ok_or(EWkbError::InsufficientBytes)?
    } else {
        read_u32_be(ewkb, 1).ok_or(EWkbError::InsufficientBytes)?
    };

    // Check SRID flag is set
    if !has_srid_flag(geom_type_with_flags) {
        return Err(EWkbError::InvalidWkb);
    }

    // Remove SRID flag to get base type
    let base_geom_type = base_type(geom_type_with_flags);

    // Build output: byte order + base type + rest (skipping SRID)
    let mut output = Vec::with_capacity(ewkb.len() - 4);
    output.push(byte_order);

    if byte_order == 0x01 {
        write_u32_le(&mut output, base_geom_type);
    } else {
        write_u32_be(&mut output, base_geom_type);
    }

    // Skip the SRID (bytes 5-8) and copy the rest
    output.extend_from_slice(&ewkb[9..]);

    Ok(output)
}

/// Read the SRID from EWKB bytes.
/// Returns None if the SRID flag is not set (plain WKB).
pub fn ewkb_read_srid(ewkb: &[u8]) -> Result<Option<i32>> {
    if ewkb.is_empty() {
        return Err(EWkbError::InsufficientBytes);
    }

    let byte_order = ewkb[0];
    if byte_order != 0x00 && byte_order != 0x01 {
        return Err(EWkbError::InvalidByteOrder(byte_order));
    }

    if ewkb.len() < 9 {
        return Err(EWkbError::InsufficientBytes);
    }

    // Read geometry type with flags
    let geom_type_with_flags = if byte_order == 0x01 {
        read_u32_le(ewkb, 1).ok_or(EWkbError::InsufficientBytes)?
    } else {
        read_u32_be(ewkb, 1).ok_or(EWkbError::InsufficientBytes)?
    };

    // Check if SRID flag is set
    if !has_srid_flag(geom_type_with_flags) {
        return Ok(None);
    }

    // Read SRID
    let srid = if byte_order == 0x01 {
        let bytes: [u8; 4] = ewkb[5..9]
            .try_into()
            .map_err(|_| EWkbError::InsufficientBytes)?;
        i32::from_le_bytes(bytes)
    } else {
        let bytes: [u8; 4] = ewkb[5..9]
            .try_into()
            .map_err(|_| EWkbError::InsufficientBytes)?;
        i32::from_be_bytes(bytes)
    };

    Ok(Some(srid))
}

/// Overwrite the SRID in existing EWKB bytes.
/// Returns error if the SRID flag is not set.
pub fn ewkb_set_srid(ewkb: &[u8], new_srid: i32) -> Result<Vec<u8>> {
    if ewkb.is_empty() {
        return Err(EWkbError::InsufficientBytes);
    }

    let byte_order = ewkb[0];
    if byte_order != 0x00 && byte_order != 0x01 {
        return Err(EWkbError::InvalidByteOrder(byte_order));
    }

    if ewkb.len() < 9 {
        return Err(EWkbError::InsufficientBytes);
    }

    // Verify SRID flag is set
    let geom_type_with_flags = if byte_order == 0x01 {
        read_u32_le(ewkb, 1).ok_or(EWkbError::InsufficientBytes)?
    } else {
        read_u32_be(ewkb, 1).ok_or(EWkbError::InsufficientBytes)?
    };

    if !has_srid_flag(geom_type_with_flags) {
        return Err(EWkbError::InvalidWkb);
    }

    // Copy everything except the SRID bytes, then write new SRID
    let mut output = Vec::with_capacity(ewkb.len());
    output.extend_from_slice(&ewkb[..5]); // byte order + type with flag
                                          // Write new SRID
    if byte_order == 0x01 {
        output.extend_from_slice(&new_srid.to_le_bytes());
    } else {
        output.extend_from_slice(&new_srid.to_be_bytes());
    }
    output.extend_from_slice(&ewkb[9..]); // rest of the data

    Ok(output)
}

/// Check if bytes appear to be EWKB (have SRID flag set)
pub fn is_ewkb(bytes: &[u8]) -> bool {
    if bytes.len() < 5 {
        return false;
    }

    let byte_order = bytes[0];
    if byte_order != 0x00 && byte_order != 0x01 {
        return false;
    }

    let geom_type_with_flags = if byte_order == 0x01 {
        read_u32_le(bytes, 1).unwrap_or(0)
    } else {
        read_u32_be(bytes, 1).unwrap_or(0)
    };

    has_srid_flag(geom_type_with_flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wkb_to_ewkb_roundtrip() {
        // Simple point: POINT(1 2)
        let wkb: Vec<u8> = vec![
            0x01, // byte order (little endian)
            0x01, 0x00, 0x00, 0x00, // type 1 (Point)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];

        let ewkb = wkb_to_ewkb(&wkb, 4326).unwrap();

        // Verify we can read the SRID back
        let srid = ewkb_read_srid(&ewkb).unwrap();
        assert_eq!(srid, Some(4326));

        // Verify we can convert back to WKB
        let wkb_back = ewkb_to_wkb(&ewkb).unwrap();
        assert_eq!(wkb, wkb_back);
    }

    #[test]
    fn test_ewkb_big_endian() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian)
            0x00, 0x00, 0x00, 0x01, // type 1 (Point)
            0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 1.0
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 2.0
        ];

        let ewkb = wkb_to_ewkb(&wkb, 3857).unwrap();
        let srid = ewkb_read_srid(&ewkb).unwrap();
        assert_eq!(srid, Some(3857));

        let wkb_back = ewkb_to_wkb(&ewkb).unwrap();
        assert_eq!(wkb, wkb_back);
    }

    #[test]
    fn test_is_ewkb() {
        let wkb: Vec<u8> = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        let ewkb = wkb_to_ewkb(&wkb, 4326).unwrap();

        assert!(!is_ewkb(&wkb));
        assert!(is_ewkb(&ewkb));
    }
}
