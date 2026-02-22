use std::error::Error;
use std::fmt;

const BYTE_SIZE: usize = 1;
const INT_SIZE: usize = 4;
const DOUBLE_SIZE: usize = 8;

const BIG_ENDIAN: u8 = 0;
const LITTLE_ENDIAN: u8 = 1;

const DIM_OFFSET_2D: i32 = 0;
const DIM_OFFSET_Z: i32 = 1000;
const DIM_OFFSET_M: i32 = 2000;
const DIM_OFFSET_ZM: i32 = 3000;

const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;
const MIN_LATITUDE: f64 = -90.0;
const MAX_LATITUDE: f64 = 90.0;

#[derive(Debug, Clone, PartialEq)]
pub enum WkbError {
    EmptyInput,
    InputTooShort,
    InvalidByteOrder(u8),
    InvalidType(i32),
    UnsupportedDimension(i32),
    UnexpectedEndOfBuffer {
        expected: usize,
        remaining: usize,
        position: usize,
    },
    NonFiniteCoordinate {
        value: f64,
        position: usize,
    },
    InvalidCoordinateValue {
        message: String,
        position: usize,
    },
    TooFewPointsInLineString {
        expected_min: i32,
        actual: i32,
        position: usize,
    },
    RingNotClosed {
        position: usize,
    },
    TooFewPointsInRing {
        expected_min: i32,
        actual: i32,
        position: usize,
    },
    ExpectedPointInMultiPoint {
        position: usize,
    },
    ExpectedLineStringInMultiLineString {
        position: usize,
    },
    ExpectedPolygonInMultiPolygon {
        position: usize,
    },
    DimensionMismatch {
        expected_has_z: bool,
        actual_has_z: bool,
        expected_has_m: bool,
        actual_has_m: bool,
        position: usize,
    },
    InvalidGeometryType {
        r#type: i32,
        position: usize,
    },
    GeographyBoundsViolation {
        message: String,
        position: usize,
    },
}

impl fmt::Display for WkbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WkbError::EmptyInput => write!(f, "WKB data is empty or null"),
            WkbError::InputTooShort => write!(f, "WKB data too short"),
            WkbError::InvalidByteOrder(b) => write!(f, "Invalid byte order: {}", b),
            WkbError::InvalidType(t) => write!(f, "Invalid or unsupported type: {}", t),
            WkbError::UnsupportedDimension(d) => write!(f, "Unsupported dimension: {}", d),
            WkbError::UnexpectedEndOfBuffer {
                expected,
                remaining,
                position,
            } => {
                write!(
                    f,
                    "Unexpected end of WKB buffer: expected {} bytes at position {}, but only {} remaining",
                    expected, position, remaining
                )
            }
            WkbError::NonFiniteCoordinate { value, position } => {
                write!(
                    f,
                    "Non-finite coordinate value ({}) found at position {}",
                    value, position
                )
            }
            WkbError::InvalidCoordinateValue { message, position } => {
                write!(
                    f,
                    "Invalid coordinate value at position {}: {}",
                    position, message
                )
            }
            WkbError::TooFewPointsInLineString {
                expected_min,
                actual,
                position,
            } => {
                write!(
                    f,
                    "Too few points in linestring at position {}: expected at least {}, got {}",
                    position, expected_min, actual
                )
            }
            WkbError::RingNotClosed { position } => {
                write!(f, "Ring is not closed at position {}", position)
            }
            WkbError::TooFewPointsInRing {
                expected_min,
                actual,
                position,
            } => {
                write!(
                    f,
                    "Too few points in ring at position {}: expected at least {}, got {}",
                    position, expected_min, actual
                )
            }
            WkbError::ExpectedPointInMultiPoint { position } => {
                write!(f, "Expected Point in MultiPoint at position {}", position)
            }
            WkbError::ExpectedLineStringInMultiLineString { position } => {
                write!(
                    f,
                    "Expected LineString in MultiLineString at position {}",
                    position
                )
            }
            WkbError::ExpectedPolygonInMultiPolygon { position } => {
                write!(
                    f,
                    "Expected Polygon in MultiPolygon at position {}",
                    position
                )
            }
            WkbError::DimensionMismatch {
                expected_has_z,
                actual_has_z,
                expected_has_m,
                actual_has_m,
                position,
            } => {
                write!(
                    f,
                    "Dimension mismatch at position {}: expected Z={}, M={}, got Z={}, M={}",
                    position, expected_has_z, expected_has_m, actual_has_z, actual_has_m
                )
            }
            WkbError::InvalidGeometryType { r#type, position } => {
                write!(
                    f,
                    "Invalid or unsupported geometry type: {} at position {}",
                    r#type, position
                )
            }
            WkbError::GeographyBoundsViolation { message, position } => {
                write!(
                    f,
                    "Geography bounds violation at position {}: {}",
                    position, message
                )
            }
        }
    }
}

impl Error for WkbError {}

pub struct WkbReader {
    is_geography: bool,
    buffer: Vec<u8>,
    position: usize,
    byte_order: u8,
    expected_has_z: bool,
    expected_has_m: bool,
}

impl WkbReader {
    pub fn new(is_geography: bool) -> Self {
        Self {
            is_geography,
            buffer: Vec::new(),
            position: 0,
            byte_order: BIG_ENDIAN,
            expected_has_z: false,
            expected_has_m: false,
        }
    }

    pub fn validate(&mut self, wkb: &[u8]) -> Result<(), WkbError> {
        if wkb.is_empty() {
            return Err(WkbError::EmptyInput);
        }

        if wkb.len() < BYTE_SIZE + INT_SIZE {
            return Err(WkbError::InputTooShort);
        }

        self.buffer = wkb.to_vec();
        self.position = 0;

        self.read_byte_order()?;
        self.read_geometry(0)
    }

    fn read_byte_order(&mut self) -> Result<(), WkbError> {
        let byte_order = self.read_byte()?;
        if byte_order != BIG_ENDIAN && byte_order != LITTLE_ENDIAN {
            return Err(WkbError::InvalidByteOrder(byte_order));
        }
        self.byte_order = byte_order;
        Ok(())
    }

    fn read_byte(&mut self) -> Result<u8, WkbError> {
        if self.position >= self.buffer.len() {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: 1,
                remaining: 0,
                position: self.position,
            });
        }
        let b = self.buffer[self.position];
        self.position += 1;
        Ok(b)
    }

    fn read_int(&mut self) -> Result<i32, WkbError> {
        let remaining = self.buffer.len() - self.position;
        if remaining < INT_SIZE {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: INT_SIZE,
                remaining,
                position: self.position,
            });
        }
        let bytes = &self.buffer[self.position..self.position + INT_SIZE];
        self.position += INT_SIZE;
        let val = if self.byte_order == LITTLE_ENDIAN {
            i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        } else {
            i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        };
        Ok(val)
    }

    fn read_double(&mut self) -> Result<f64, WkbError> {
        let remaining = self.buffer.len() - self.position;
        if remaining < DOUBLE_SIZE {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: DOUBLE_SIZE,
                remaining,
                position: self.position,
            });
        }
        let bytes = &self.buffer[self.position..self.position + DOUBLE_SIZE];
        self.position += DOUBLE_SIZE;
        let val = if self.byte_order == LITTLE_ENDIAN {
            f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        } else {
            f64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        };
        Ok(val)
    }

    fn get_base_type(&self, wkb_type: i32) -> i32 {
        let base = wkb_type;
        for offset in [DIM_OFFSET_ZM, DIM_OFFSET_M, DIM_OFFSET_Z] {
            if base > offset && base <= offset + 7 {
                return base - offset;
            }
        }
        base
    }

    fn get_dimension_count(&self, wkb_type: i32) -> usize {
        let base = wkb_type;
        for (offset, dims) in [(DIM_OFFSET_ZM, 4), (DIM_OFFSET_M, 3), (DIM_OFFSET_Z, 3)] {
            if base > offset && base <= offset + 7 {
                return dims;
            }
        }
        2
    }

    fn has_z(&self, wkb_type: i32) -> bool {
        let base = self.get_base_type(wkb_type);
        let offset = wkb_type - base;
        offset == DIM_OFFSET_Z || offset == DIM_OFFSET_ZM
    }

    fn has_m(&self, wkb_type: i32) -> bool {
        let base = self.get_base_type(wkb_type);
        let offset = wkb_type - base;
        offset == DIM_OFFSET_M || offset == DIM_OFFSET_ZM
    }

    fn is_valid_wkb_type(&self, wkb_type: i32) -> bool {
        let base = self.get_base_type(wkb_type);
        if !(1..=7).contains(&base) {
            return false;
        }
        let offset = wkb_type - base;
        offset == DIM_OFFSET_2D
            || offset == DIM_OFFSET_Z
            || offset == DIM_OFFSET_M
            || offset == DIM_OFFSET_ZM
    }

    fn read_geometry(&mut self, depth: usize) -> Result<(), WkbError> {
        if self.buffer.len() - self.position < INT_SIZE {
            return Err(WkbError::InputTooShort);
        }

        let type_start_pos = self.position;
        let type_and_dim = self.read_int()?;

        if !self.is_valid_wkb_type(type_and_dim) {
            return Err(WkbError::InvalidGeometryType {
                r#type: type_and_dim,
                position: type_start_pos,
            });
        }

        let geo_type = self.get_base_type(type_and_dim);
        let dimension_count = self.get_dimension_count(type_and_dim);
        let has_z = self.has_z(type_and_dim);
        let has_m = self.has_m(type_and_dim);

        if depth > 0 && (has_z != self.expected_has_z || has_m != self.expected_has_m) {
            return Err(WkbError::DimensionMismatch {
                expected_has_z: self.expected_has_z,
                actual_has_z: has_z,
                expected_has_m: self.expected_has_m,
                actual_has_m: has_m,
                position: type_start_pos,
            });
        }

        if depth == 0 {
            self.expected_has_z = has_z;
            self.expected_has_m = has_m;
        }

        match geo_type {
            1 => self.read_point(dimension_count, has_z, has_m, true),
            2 => self.read_linestring(dimension_count, has_z, has_m),
            3 => self.read_polygon(dimension_count, has_z, has_m),
            4 => self.read_multipoint(has_z, has_m),
            5 => self.read_multilinestring(has_z, has_m),
            6 => self.read_multipolygon(has_z, has_m),
            7 => self.read_geometry_collection(has_z, has_m),
            _ => Err(WkbError::InvalidType(geo_type)),
        }
    }

    fn read_point(
        &mut self,
        dimension_count: usize,
        _has_z: bool,
        _has_m: bool,
        allow_empty: bool,
    ) -> Result<(), WkbError> {
        let coords_start_pos = self.position;
        let mut coords = Vec::with_capacity(dimension_count);

        for _ in 0..dimension_count {
            let value = self.read_double()?;
            if !allow_empty && !value.is_finite() {
                return Err(WkbError::NonFiniteCoordinate {
                    value,
                    position: coords_start_pos,
                });
            }
            if value.is_finite() {
                coords.push(value);
            }
        }

        if self.is_geography && coords.len() >= 2 {
            let lon = coords[0];
            let lat = coords[1];

            if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&lon) {
                return Err(WkbError::GeographyBoundsViolation {
                    message: format!("longitude {} is out of range [-180, 180]", lon),
                    position: coords_start_pos,
                });
            }

            if !(MIN_LATITUDE..=MAX_LATITUDE).contains(&lat) {
                return Err(WkbError::GeographyBoundsViolation {
                    message: format!("latitude {} is out of range [-90, 90]", lat),
                    position: coords_start_pos,
                });
            }
        }

        Ok(())
    }

    fn read_linestring(
        &mut self,
        dimension_count: usize,
        has_z: bool,
        has_m: bool,
    ) -> Result<(), WkbError> {
        let num_points_pos = self.position;
        let num_points = self.read_int()?;

        if num_points < 2 {
            return Err(WkbError::TooFewPointsInLineString {
                expected_min: 2,
                actual: num_points,
                position: num_points_pos,
            });
        }

        for _ in 0..num_points {
            self.read_internal_point(dimension_count, has_z, has_m)?;
        }

        Ok(())
    }

    fn read_internal_point(
        &mut self,
        dimension_count: usize,
        _has_z: bool,
        _has_m: bool,
    ) -> Result<(), WkbError> {
        let coords_start_pos = self.position;

        let mut coords = Vec::with_capacity(dimension_count);
        for _ in 0..dimension_count {
            let value = self.read_double()?;
            if !value.is_finite() {
                return Err(WkbError::NonFiniteCoordinate {
                    value,
                    position: coords_start_pos,
                });
            }
            coords.push(value);
        }

        if self.is_geography && dimension_count >= 2 {
            let lon = coords[0];
            let lat = coords[1];

            if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&lon) {
                return Err(WkbError::GeographyBoundsViolation {
                    message: format!("longitude {} is out of range [-180, 180]", lon),
                    position: coords_start_pos,
                });
            }

            if !(MIN_LATITUDE..=MAX_LATITUDE).contains(&lat) {
                return Err(WkbError::GeographyBoundsViolation {
                    message: format!("latitude {} is out of range [-90, 90]", lat),
                    position: coords_start_pos,
                });
            }
        }

        Ok(())
    }

    fn read_polygon(
        &mut self,
        dimension_count: usize,
        has_z: bool,
        has_m: bool,
    ) -> Result<(), WkbError> {
        let num_rings = self.read_int()?;
        if num_rings < 0 {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: 4,
                remaining: 0,
                position: self.position - 4,
            });
        }

        for _ in 0..num_rings {
            self.read_ring(dimension_count, has_z, has_m)?;
        }

        Ok(())
    }

    fn read_ring(
        &mut self,
        dimension_count: usize,
        _has_z: bool,
        _has_m: bool,
    ) -> Result<(), WkbError> {
        let num_points_pos = self.position;
        let num_points = self.read_int()?;

        if num_points < 4 {
            return Err(WkbError::TooFewPointsInRing {
                expected_min: 4,
                actual: num_points,
                position: num_points_pos,
            });
        }

        let mut first_coords: Option<Vec<f64>> = None;
        let mut last_coords: Option<Vec<f64>> = None;

        for i in 0..num_points {
            let coords_start_pos = self.position;
            let mut coords = Vec::with_capacity(dimension_count);

            for _ in 0..dimension_count {
                let value = self.read_double()?;
                if !value.is_finite() {
                    return Err(WkbError::NonFiniteCoordinate {
                        value,
                        position: coords_start_pos,
                    });
                }
                coords.push(value);
            }

            if self.is_geography && dimension_count >= 2 {
                let lon = coords[0];
                let lat = coords[1];
                if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&lon)
                    || !(MIN_LATITUDE..=MAX_LATITUDE).contains(&lat)
                {
                    return Err(WkbError::GeographyBoundsViolation {
                        message: format!(
                            "coordinate ({}, {}) is out of geography bounds [-180, 180] x [-90, 90]",
                            lon, lat
                        ),
                        position: coords_start_pos,
                    });
                }
            }

            if i == 0 {
                first_coords = Some(coords);
            } else if i == num_points - 1 {
                last_coords = Some(coords);
            }
        }

        if let (Some(first), Some(last)) = (first_coords, last_coords) {
            if first != last {
                return Err(WkbError::RingNotClosed {
                    position: num_points_pos,
                });
            }
        }

        Ok(())
    }

    fn read_multipoint(
        &mut self,
        expected_has_z: bool,
        expected_has_m: bool,
    ) -> Result<(), WkbError> {
        let num_geometries = self.read_int()?;
        if num_geometries < 0 {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: 4,
                remaining: 0,
                position: self.position - 4,
            });
        }

        for _ in 0..num_geometries {
            let pos = self.position;
            self.read_nested_geometry(expected_has_z, expected_has_m, |geo_type| {
                if geo_type != 1 {
                    Err(WkbError::ExpectedPointInMultiPoint { position: pos })
                } else {
                    Ok(())
                }
            })?;
        }

        Ok(())
    }

    fn read_multilinestring(
        &mut self,
        expected_has_z: bool,
        expected_has_m: bool,
    ) -> Result<(), WkbError> {
        let num_geometries = self.read_int()?;
        if num_geometries < 0 {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: 4,
                remaining: 0,
                position: self.position - 4,
            });
        }

        for _ in 0..num_geometries {
            let pos = self.position;
            self.read_nested_geometry(expected_has_z, expected_has_m, |geo_type| {
                if geo_type != 2 {
                    Err(WkbError::ExpectedLineStringInMultiLineString { position: pos })
                } else {
                    Ok(())
                }
            })?;
        }

        Ok(())
    }

    fn read_multipolygon(
        &mut self,
        expected_has_z: bool,
        expected_has_m: bool,
    ) -> Result<(), WkbError> {
        let num_geometries = self.read_int()?;
        if num_geometries < 0 {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: 4,
                remaining: 0,
                position: self.position - 4,
            });
        }

        for _ in 0..num_geometries {
            let pos = self.position;
            self.read_nested_geometry(expected_has_z, expected_has_m, |geo_type| {
                if geo_type != 3 {
                    Err(WkbError::ExpectedPolygonInMultiPolygon { position: pos })
                } else {
                    Ok(())
                }
            })?;
        }

        Ok(())
    }

    fn read_geometry_collection(
        &mut self,
        expected_has_z: bool,
        expected_has_m: bool,
    ) -> Result<(), WkbError> {
        let num_geometries = self.read_int()?;
        if num_geometries < 0 {
            return Err(WkbError::UnexpectedEndOfBuffer {
                expected: 4,
                remaining: 0,
                position: self.position - 4,
            });
        }

        for _ in 0..num_geometries {
            self.read_nested_geometry(expected_has_z, expected_has_m, |_| Ok(()))?;
        }

        Ok(())
    }

    fn read_nested_geometry<F>(
        &mut self,
        expected_has_z: bool,
        expected_has_m: bool,
        type_check: F,
    ) -> Result<(), WkbError>
    where
        F: FnOnce(i32) -> Result<(), WkbError>,
    {
        let byte_order = self.read_byte()?;
        if byte_order != BIG_ENDIAN && byte_order != LITTLE_ENDIAN {
            return Err(WkbError::InvalidByteOrder(byte_order));
        }
        let saved_byte_order = self.byte_order;
        self.byte_order = byte_order;

        let type_start_pos = self.position;
        let type_and_dim = self.read_int()?;

        if !self.is_valid_wkb_type(type_and_dim) {
            self.byte_order = saved_byte_order;
            return Err(WkbError::InvalidGeometryType {
                r#type: type_and_dim,
                position: type_start_pos,
            });
        }

        let geo_type = self.get_base_type(type_and_dim);
        if let Err(e) = type_check(geo_type) {
            self.byte_order = saved_byte_order;
            return Err(e);
        }

        let has_z = self.has_z(type_and_dim);
        let has_m = self.has_m(type_and_dim);

        if has_z != expected_has_z || has_m != expected_has_m {
            let err = self.position;
            self.byte_order = saved_byte_order;
            return Err(WkbError::DimensionMismatch {
                expected_has_z,
                actual_has_z: has_z,
                expected_has_m,
                actual_has_m: has_m,
                position: err,
            });
        }

        let dimension_count = self.get_dimension_count(type_and_dim);

        match geo_type {
            1 => self.read_point(dimension_count, has_z, has_m, false)?,
            2 => self.read_linestring(dimension_count, has_z, has_m)?,
            3 => self.read_polygon(dimension_count, has_z, has_m)?,
            4 => self.read_multipoint(has_z, has_m)?,
            5 => self.read_multilinestring(has_z, has_m)?,
            6 => self.read_multipolygon(has_z, has_m)?,
            7 => self.read_geometry_collection(has_z, has_m)?,
            _ => {
                self.byte_order = saved_byte_order;
                return Err(WkbError::InvalidType(geo_type));
            }
        }

        self.byte_order = saved_byte_order;
        Ok(())
    }
}

pub fn validate_geometry(wkb: &[u8]) -> Result<(), WkbError> {
    let mut reader = WkbReader::new(false);
    reader.validate(wkb)
}

pub fn validate_geography(wkb: &[u8]) -> Result<(), WkbError> {
    let mut reader = WkbReader::new(true);
    reader.validate(wkb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_point() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x01, // type (Point)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_valid_linestring() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x02, // type (LineString)
            0x00, 0x00, 0x00, 0x02, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x1 = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y1 = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x2 = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y2 = 2.0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_too_few_points_linestring() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x02, // type (LineString)
            0x00, 0x00, 0x00, 0x01, // num points = 1 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::TooFewPointsInLineString { .. })
        ));
    }

    #[test]
    fn test_empty_input() {
        let wkb: Vec<u8> = vec![];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::EmptyInput)));
    }

    #[test]
    fn test_invalid_byte_order() {
        let wkb: Vec<u8> = vec![
            0x02, // invalid byte order
            0x00, 0x00, 0x00, 0x01,
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidByteOrder(2))));
    }

    #[test]
    fn test_invalid_type() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian)
            0x00, 0x00, 0x00, 0x99, // invalid type
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidGeometryType { .. })));
    }

    #[test]
    fn test_input_too_short() {
        // Not enough bytes for header
        let wkb: Vec<u8> = vec![0x00, 0x00, 0x00];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InputTooShort)));
    }

    #[test]
    fn test_nested_dimension_mismatch() {
        // MultiPoint containing a PointZ when parent expects 2D
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x04, // type (MultiPoint) - 2D
            0x00, 0x00, 0x00, 0x01, // num points = 1
            // Point Z (nested) - 3D!
            0x00, // byte order
            0x00, 0x00, 0x03, 0xe9, // type (PointZ = 1001)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // z
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_valid_multilinestring() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x05, // type (MultiLineString)
            0x00, 0x00, 0x00, 0x01, // num line strings = 1
            // LineString (nested)
            0x00, // byte order
            0x00, 0x00, 0x00, 0x02, // type (LineString)
            0x00, 0x00, 0x00, 0x02, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // (0, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // (1, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_valid_multipolygon() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x06, // type (MultiPolygon)
            0x00, 0x00, 0x00, 0x01, // num polygons = 1
            // Polygon (nested)
            0x00, // byte order
            0x00, 0x00, 0x00, 0x03, // type (Polygon)
            0x00, 0x00, 0x00, 0x01, // num rings = 1
            0x00, 0x00, 0x00, 0x04, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // (0, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf0, 0x3f, // (1, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf0, 0x3f, // (1, 1)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // (0, 0) - closed
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_valid_polygon() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x03, // type (Polygon)
            0x00, 0x00, 0x00, 0x01, // num rings = 1
            0x00, 0x00, 0x00, 0x04, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // (0, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf0, 0x3f, // (1, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf0, 0x3f, // (1, 1)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // (0, 0) - closed
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_too_few_points_in_ring() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x03, // type (Polygon)
            0x00, 0x00, 0x00, 0x01, // num rings = 1
            0x00, 0x00, 0x00, 0x03, // num points = 3 (invalid - needs 4)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::TooFewPointsInRing { .. })));
    }

    #[test]
    fn test_ring_not_closed() {
        // Ring with 4 points where last != first
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x03, // type (Polygon)
            0x00, 0x00, 0x00, 0x01, // num rings = 1
            0x00, 0x00, 0x00, 0x04, // num points = 4
            // Point 1: (0, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // Point 2: (1, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // Point 3: (1, 1)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf0, 0x3f, // Point 4: (0, 1) - NOT equal to point 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf0, 0x3f,
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::RingNotClosed { .. })));
    }

    #[test]
    fn test_valid_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x07, // type (GeometryCollection)
            0x00, 0x00, 0x00, 0x01, // num geometries = 1
            // Point (nested)
            0x00, // byte order
            0x00, 0x00, 0x00, 0x01, // type (Point)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_empty_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x07, // type (GeometryCollection)
            0x00, 0x00, 0x00, 0x00, // num geometries = 0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_point_with_z() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x03, 0xe9, // type (PointZ = 1001)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // z = 3.0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_point_with_m() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x07, 0xd1, // type (PointM = 2001)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // m = 3.0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_point_with_zm() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x0b, 0xb9, // type (PointZM = 3001)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // z = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // m = 4.0
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_geography_longitude_out_of_range() {
        // 200.0 in big endian = 0x4079000000000000 -> bytes: 40 79 00 00 00 00 00 00
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x01, // type (Point)
            0x40, 0x79, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 200.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        let result = validate_geography(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::GeographyBoundsViolation { .. })
        ));
    }

    #[test]
    fn test_geography_latitude_out_of_range() {
        // 100.0 in big endian = 0x4059000000000000 -> bytes: 40 59 00 00 00 00 00 00
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x01, // type (Point)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
            0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 100.0
        ];
        let result = validate_geography(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::GeographyBoundsViolation { .. })
        ));
    }

    #[test]
    fn test_geography_valid_bounds() {
        // 120.0 = 0x405e000000000000, 45.0 = 0x4046800000000000
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x01, // type (Point)
            0x40, 0x5e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 120.0
            0x40, 0x46, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 45.0
        ];
        assert!(validate_geography(&wkb).is_ok());
    }

    #[test]
    fn test_negative_count_multipoint() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x04, // type (MultiPoint)
            0xff, 0xff, 0xff, 0xff, // num points = -1 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::UnexpectedEndOfBuffer { .. })
        ));
    }

    #[test]
    fn test_negative_count_multilinestring() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x05, // type (MultiLineString)
            0xff, 0xff, 0xff, 0xff, // num line strings = -1 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::UnexpectedEndOfBuffer { .. })
        ));
    }

    #[test]
    fn test_negative_count_multipolygon() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x06, // type (MultiPolygon)
            0xff, 0xff, 0xff, 0xff, // num polygons = -1 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::UnexpectedEndOfBuffer { .. })
        ));
    }

    #[test]
    fn test_negative_count_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x07, // type (GeometryCollection)
            0xff, 0xff, 0xff, 0xff, // num geometries = -1 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::UnexpectedEndOfBuffer { .. })
        ));
    }

    #[test]
    fn test_nested_invalid_type_in_multipoint() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x04, // type (MultiPoint)
            0x00, 0x00, 0x00, 0x01, // num points = 1
            0x00, // byte order
            0x00, 0x00, 0x00, 0x02, // type (LineString) - wrong type!
            0x00, 0x00, 0x00, 0x02, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::ExpectedPointInMultiPoint { .. })
        ));
    }

    #[test]
    fn test_nested_invalid_type_in_multilinestring() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x05, // type (MultiLineString)
            0x00, 0x00, 0x00, 0x01, // num line strings = 1
            0x00, // byte order
            0x00, 0x00, 0x00, 0x01, // type (Point) - wrong type!
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x40,
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::ExpectedLineStringInMultiLineString { .. })
        ));
    }

    #[test]
    fn test_nested_invalid_type_in_multipolygon() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x06, // type (MultiPolygon)
            0x00, 0x00, 0x00, 0x01, // num polygons = 1
            0x00, // byte order
            0x00, 0x00, 0x00, 0x01, // type (Point) - wrong type!
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x40,
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(
            result,
            Err(WkbError::ExpectedPolygonInMultiPolygon { .. })
        ));
    }

    #[test]
    fn test_nested_invalid_type_in_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x07, // type (GeometryCollection)
            0x00, 0x00, 0x00, 0x01, // num geometries = 1
            0x00, // byte order
            0x00, 0x00, 0x00, 0x63, // type = 99 (invalid)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidGeometryType { .. })));
    }

    #[test]
    fn test_nested_invalid_byte_order() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian outer
            0x00, 0x00, 0x00, 0x07, // type (GeometryCollection)
            0x00, 0x00, 0x00, 0x01, // num geometries = 1
            0x02, // byte order = 2 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidByteOrder(2))));
    }

    #[test]
    fn test_polygon_with_hole() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x03, // type (Polygon)
            0x00, 0x00, 0x00, 0x02, // num rings = 2 (exterior + 1 hole)
            // Exterior ring
            0x00, 0x00, 0x00, 0x05, // num points = 5
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // (0, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x10, 0x40, // (4, 0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x10, 0x40, // (4, 4)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // (0, 4)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // (0, 0) - closed
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Hole ring
            0x00, 0x00, 0x00, 0x05, // num points = 5
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x40, // (1, 1)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xe8, 0x40, // (3, 1)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xe8, 0x40, // (3, 3)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xe8, 0x40, // (1, 3)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xe8, 0x40, // (1, 1) - closed
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x40,
        ];
        assert!(validate_geometry(&wkb).is_ok());
    }

    #[test]
    fn test_invalid_type_zero() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x00, // type = 0 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidGeometryType { .. })));
    }

    #[test]
    fn test_type_8_and_above_invalid() {
        let wkb: Vec<u8> = vec![
            0x00, // byte order (big endian = 0)
            0x00, 0x00, 0x00, 0x08, // type = 8 (invalid!)
        ];
        let result = validate_geometry(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidGeometryType { .. })));
    }
}
