use std::collections::HashMap;

use datafusion::arrow::datatypes as adt;
use serde::Deserialize;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::{data_type as sdt, DataType};

/// Raw GeoArrow extension metadata deserialized from JSON.
#[derive(Debug, Deserialize)]
struct RawGeoArrowMetadata {
    edges: Option<String>,
    /// CRS can be a string ("OGC:CRS84", "EPSG:3857") or a PROJJSON object.
    crs: Option<serde_json::Value>,
}

/// Valid edge interpolation values per the GeoArrow spec.
/// Omitted edges indicate planar/linear interpolation (Geometry).
const VALID_EDGES: &[&str] = &["spherical", "vincenty", "thomas", "andoyer", "karney"];

/// GeoArrow extension metadata extracted from Arrow field metadata.
#[derive(Debug, Clone)]
struct GeoArrowMetadata {
    /// Edge interpolation algorithm. `None` means planar/linear (Geometry).
    /// Any value present indicates Geography (non-planar edges).
    edges: Option<String>,
    /// Spatial Reference System Identifier (SRID). -1 for mixed SRID.
    srid: i32,
}

/// Parse a CRS authority:code string to SRID integer.
///
/// Only the CRS values supported by Spark 4.1 are accepted:
/// - "OGC:CRS84" -> SRID 4326 (WGS84)
/// - "EPSG:3857" -> SRID 3857 (Web Mercator)
/// - "SRID:0"    -> SRID 0 (unspecified)
///
/// Mixed SRID (-1) is represented by omitting the CRS field entirely.
fn crs_to_srid(crs: &str) -> SparkResult<i32> {
    match crs {
        "OGC:CRS84" => Ok(4326),
        "EPSG:3857" => Ok(3857),
        "SRID:0" => Ok(0),
        _ => Err(SparkError::invalid(format!(
            "unsupported CRS: {crs}. Supported values: OGC:CRS84, EPSG:3857, SRID:0"
        ))),
    }
}

/// Parse a CRS JSON value to SRID integer.
///
/// The value can be:
/// - A string like "OGC:CRS84", "EPSG:3857", etc. (handled by `crs_to_srid`)
/// - A PROJJSON object with an "id" member containing "authority" and "code"
///   (used by GeoPandas, DuckDB, SedonaDB)
fn crs_value_to_srid(value: &serde_json::Value) -> SparkResult<i32> {
    match value {
        serde_json::Value::String(s) => crs_to_srid(s),
        serde_json::Value::Object(obj) => {
            // Extract authority:code from PROJJSON "id" member
            let id = obj.get("id").ok_or_else(|| {
                SparkError::invalid("PROJJSON CRS missing 'id' member for authority:code")
            })?;
            let authority = id
                .get("authority")
                .and_then(|v| v.as_str())
                .ok_or_else(|| SparkError::invalid("PROJJSON 'id' missing 'authority'"))?;
            let code = id
                .get("code")
                .map(|v| {
                    if let Some(s) = v.as_str() {
                        s.to_string()
                    } else if let Some(n) = v.as_number() {
                        n.to_string()
                    } else {
                        String::new()
                    }
                })
                .ok_or_else(|| SparkError::invalid("PROJJSON 'id' missing 'code'"))?;
            let auth_code = format!("{authority}:{code}");
            crs_to_srid(&auth_code)
        }
        _ => Err(SparkError::invalid(format!(
            "unsupported CRS value type: expected string or object, got {value}"
        ))),
    }
}

impl GeoArrowMetadata {
    /// Parse GeoArrow metadata from JSON string.
    fn from_json(metadata: &str) -> SparkResult<Self> {
        let raw: RawGeoArrowMetadata = serde_json::from_str(metadata)
            .map_err(|e| SparkError::invalid(format!("invalid geoarrow metadata JSON: {e}")))?;

        let edges = match raw.edges {
            Some(e) if VALID_EDGES.contains(&e.as_str()) => Some(e),
            Some(e) => {
                return Err(SparkError::invalid(format!(
                    "unsupported edges value: {e}. Valid values: {VALID_EDGES:?}"
                )))
            }
            None => None,
        };
        let srid = match raw.crs {
            Some(crs_value) => crs_value_to_srid(&crs_value)?,
            // Absent CRS means mixed/unknown SRID
            None => -1,
        };

        Ok(Self { edges, srid })
    }

    /// Returns true if this represents a Geography type (any non-planar edge interpolation).
    fn is_geography(&self) -> bool {
        self.edges.is_some()
    }
}

impl TryFrom<adt::Field> for sdt::StructField {
    type Error = SparkError;

    fn try_from(field: adt::Field) -> SparkResult<sdt::StructField> {
        let is_udt = field.metadata().keys().any(|k| k.starts_with("udt."));
        let is_geoarrow = field.extension_type_name() == Some("geoarrow.wkb");

        let data_type = if is_udt {
            DataType {
                kind: Some(sdt::Kind::Udt(Box::new(sdt::Udt {
                    r#type: "udt".to_string(),
                    jvm_class: field.metadata().get("udt.jvm_class").cloned(),
                    python_class: field.metadata().get("udt.python_class").cloned(),
                    serialized_python_class: field
                        .metadata()
                        .get("udt.serialized_python_class")
                        .cloned(),
                    sql_type: Some(Box::new(field.data_type().clone().try_into()?)),
                }))),
            }
        } else if is_geoarrow {
            // Parse geoarrow extension metadata to determine Geometry vs Geography
            let ext_metadata = field
                .metadata()
                .get("ARROW:extension:metadata")
                .cloned()
                .unwrap_or_default();
            let geo_meta = GeoArrowMetadata::from_json(&ext_metadata)?;

            if geo_meta.is_geography() {
                DataType {
                    kind: Some(sdt::Kind::Geography(sdt::Geography {
                        srid: geo_meta.srid,
                        type_variation_reference: 0,
                    })),
                }
            } else {
                DataType {
                    kind: Some(sdt::Kind::Geometry(sdt::Geometry {
                        srid: geo_meta.srid,
                        type_variation_reference: 0,
                    })),
                }
            }
        } else {
            field.data_type().clone().try_into()?
        };
        // FIXME: The metadata. prefix is managed by Sail and the convention should be respected everywhere.
        // Also filter out Arrow extension metadata (geoarrow, etc.) which is internal to Sail
        let metadata = &field
            .metadata()
            .iter()
            .filter(|(k, _)| !k.starts_with("udt.") && !k.starts_with("ARROW:extension:"))
            .map(|(k, v)| {
                let parsed = serde_json::from_str::<serde_json::Value>(v)
                    .unwrap_or_else(|_| serde_json::Value::String(v.clone()));
                Ok((k.strip_prefix("metadata.").unwrap_or(k), parsed))
            })
            .collect::<SparkResult<HashMap<_, serde_json::Value>>>()?;
        let metadata = serde_json::to_string(metadata)?;
        Ok(sdt::StructField {
            name: field.name().clone(),
            data_type: Some(data_type),
            nullable: field.is_nullable(),
            metadata: Some(metadata),
        })
    }
}

/// Reference: https://github.com/apache/spark/blob/bb17665955ad536d8c81605da9a59fb94b6e0162/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
impl TryFrom<adt::DataType> for DataType {
    type Error = SparkError;

    fn try_from(data_type: adt::DataType) -> SparkResult<DataType> {
        use sdt::Kind;

        let error =
            |x: &adt::DataType| SparkError::unsupported(format!("cast {x:?} to Spark data type"));
        let kind = match data_type {
            adt::DataType::Null => Kind::Null(sdt::Null::default()),
            adt::DataType::Binary
            | adt::DataType::FixedSizeBinary(_)
            | adt::DataType::LargeBinary
            | adt::DataType::BinaryView => Kind::Binary(sdt::Binary::default()),
            adt::DataType::Boolean => Kind::Boolean(sdt::Boolean::default()),
            // TODO: cast unsigned integer types to signed integer types in the query output,
            //   and return an error if unsigned integer types are found here.
            adt::DataType::UInt8 | adt::DataType::Int8 => Kind::Byte(sdt::Byte::default()),
            adt::DataType::UInt16 | adt::DataType::Int16 => Kind::Short(sdt::Short::default()),
            adt::DataType::UInt32 | adt::DataType::Int32 => Kind::Integer(sdt::Integer::default()),
            adt::DataType::UInt64 | adt::DataType::Int64 => Kind::Long(sdt::Long::default()),
            adt::DataType::Float16 => return Err(error(&data_type)),
            adt::DataType::Float32 => Kind::Float(sdt::Float::default()),
            adt::DataType::Float64 => Kind::Double(sdt::Double::default()),
            adt::DataType::Decimal128(precision, scale)
            | adt::DataType::Decimal256(precision, scale) => Kind::Decimal(sdt::Decimal {
                scale: Some(scale as i32),
                precision: Some(precision as i32),
                type_variation_reference: 0,
            }),
            // FIXME: This mapping might not always be correct due to converting to Arrow data types and back.
            //  For example, this originally may have been a `Kind::Char` or `Kind::VarChar` in Spark.
            //  We retain the original type information in the spec, but it is lost after converting to Arrow.
            adt::DataType::Utf8 | adt::DataType::LargeUtf8 | adt::DataType::Utf8View => {
                Kind::String(sdt::String::default())
            }
            adt::DataType::Date32 => Kind::Date(sdt::Date::default()),
            adt::DataType::Date64 => return Err(error(&data_type)),
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None) => {
                Kind::TimestampNtz(sdt::TimestampNtz::default())
            }
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, Some(_)) => {
                Kind::Timestamp(sdt::Timestamp::default())
            }
            adt::DataType::Timestamp(adt::TimeUnit::Second, _)
            | adt::DataType::Timestamp(adt::TimeUnit::Millisecond, _)
            | adt::DataType::Timestamp(adt::TimeUnit::Nanosecond, _) => {
                return Err(error(&data_type))
            }
            adt::DataType::Time32(adt::TimeUnit::Second) => Kind::Time(sdt::Time {
                precision: Some(0),
                type_variation_reference: 0,
            }),
            adt::DataType::Time32(adt::TimeUnit::Millisecond) => Kind::Time(sdt::Time {
                precision: Some(3),
                type_variation_reference: 0,
            }),
            adt::DataType::Time64(adt::TimeUnit::Microsecond) => Kind::Time(sdt::Time {
                precision: Some(6),
                type_variation_reference: 0,
            }),
            adt::DataType::Time64(adt::TimeUnit::Nanosecond) => return Err(error(&data_type)),
            adt::DataType::Time32(_) | adt::DataType::Time64(_) => return Err(error(&data_type)),
            adt::DataType::Interval(adt::IntervalUnit::MonthDayNano) => {
                Kind::CalendarInterval(sdt::CalendarInterval::default())
            }
            adt::DataType::Interval(adt::IntervalUnit::YearMonth) => {
                Kind::YearMonthInterval(sdt::YearMonthInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Interval(adt::IntervalUnit::DayTime) => {
                Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Duration(adt::TimeUnit::Microsecond) => {
                Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Duration(
                adt::TimeUnit::Second | adt::TimeUnit::Millisecond | adt::TimeUnit::Nanosecond,
            ) => return Err(error(&data_type)),
            adt::DataType::List(field)
            | adt::DataType::FixedSizeList(field, _)
            | adt::DataType::LargeList(field)
            | adt::DataType::ListView(field)
            | adt::DataType::LargeListView(field) => {
                let field = sdt::StructField::try_from(field.as_ref().clone())?;
                Kind::Array(Box::new(sdt::Array {
                    element_type: field.data_type.map(Box::new),
                    contains_null: field.nullable,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Struct(fields) => Kind::Struct(sdt::Struct {
                fields: fields
                    .into_iter()
                    .map(|f| f.as_ref().clone().try_into())
                    .collect::<SparkResult<Vec<sdt::StructField>>>()?,
                type_variation_reference: 0,
            }),
            adt::DataType::Map(ref field, ref _keys_sorted) => {
                let field = sdt::StructField::try_from(field.as_ref().clone())?;
                let Some(DataType {
                    kind: Some(Kind::Struct(sdt::Struct { fields, .. })),
                }) = field.data_type
                else {
                    return Err(error(&data_type));
                };
                let [key_field, value_field] = fields.as_slice() else {
                    return Err(error(&data_type));
                };
                Kind::Map(Box::new(sdt::Map {
                    key_type: key_field.data_type.clone().map(Box::new),
                    value_type: value_field.data_type.clone().map(Box::new),
                    value_contains_null: value_field.nullable,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Union { .. }
            | adt::DataType::Dictionary { .. }
            | adt::DataType::RunEndEncoded(_, _)
            | adt::DataType::Decimal32(_, _)
            | adt::DataType::Decimal64(_, _) => return Err(error(&data_type)),
        };
        Ok(DataType { kind: Some(kind) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SparkResult;

    #[test]
    fn test_geoarrow_metadata_parsing() -> SparkResult<()> {
        // Test Geometry (no edges) with Spark 4.1 CRS format
        let metadata = r#"{"crs":"OGC:CRS84"}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.edges, None);
        assert_eq!(parsed.srid, 4326);
        assert!(!parsed.is_geography());

        // Test Geography (spherical edges) with Spark 4.1 CRS format
        let metadata = r#"{"crs":"OGC:CRS84","edges":"spherical"}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.edges, Some("spherical".to_string()));
        assert_eq!(parsed.srid, 4326);
        assert!(parsed.is_geography());

        // Test other valid GeoArrow edge types are accepted as Geography
        for edge_type in &["vincenty", "thomas", "andoyer", "karney"] {
            let metadata = format!(r#"{{"crs":"OGC:CRS84","edges":"{edge_type}"}}"#);
            let parsed = GeoArrowMetadata::from_json(&metadata)?;
            assert!(parsed.is_geography());
        }

        // Test Web Mercator projection (Geometry, no edges)
        let metadata = r#"{"crs":"EPSG:3857"}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, 3857);
        assert!(!parsed.is_geography());

        // Test absent CRS means mixed/unknown SRID
        let metadata = r#"{"edges":"spherical"}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, -1);

        // Test empty metadata (no CRS, no edges) = Geometry with mixed SRID
        let metadata = r#"{}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.edges, None);
        assert_eq!(parsed.srid, -1);
        assert!(!parsed.is_geography());

        // Test PROJJSON CRS (used by GeoPandas, DuckDB, SedonaDB)
        let metadata = r#"{"crs":{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84 (CRS84)","id":{"authority":"OGC","code":"CRS84"}},"edges":"spherical"}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, 4326);
        assert!(parsed.is_geography());

        // Test PROJJSON with numeric EPSG code (Geometry)
        let metadata = r#"{"crs":{"type":"GeographicCRS","name":"NAD83","id":{"authority":"EPSG","code":3857}}}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, 3857);
        assert!(!parsed.is_geography());

        // Test SRID:0 (unspecified)
        let metadata = r#"{"crs":"SRID:0"}"#;
        let parsed = GeoArrowMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, 0);

        // Test error on invalid JSON
        assert!(GeoArrowMetadata::from_json("invalid").is_err());

        // Test error on unsupported CRS (Spark 4.1 only supports OGC:CRS84, EPSG:3857, SRID:0)
        assert!(GeoArrowMetadata::from_json(r#"{"crs":"EPSG:32632"}"#).is_err());
        assert!(GeoArrowMetadata::from_json(r#"{"crs":"EPSG:4326"}"#).is_err());
        assert!(GeoArrowMetadata::from_json(r#"{"crs":"UNKNOWN:FOO"}"#).is_err());

        // Test error on invalid edges value
        assert!(GeoArrowMetadata::from_json(r#"{"edges":"planar"}"#).is_err());
        assert!(GeoArrowMetadata::from_json(r#"{"edges":"invalid"}"#).is_err());

        // Test error on PROJJSON without id member
        assert!(GeoArrowMetadata::from_json(r#"{"crs":{"type":"GeographicCRS"}}"#).is_err());

        // Test error on PROJJSON with unsupported authority:code
        assert!(GeoArrowMetadata::from_json(
            r#"{"crs":{"type":"GeographicCRS","id":{"authority":"EPSG","code":32632}}}"#
        )
        .is_err());

        Ok(())
    }

    #[test]
    fn test_geoarrow_field_to_proto_geometry() -> SparkResult<()> {
        // Create an Arrow field with geoarrow.wkb metadata for Geometry
        // Geometry omits "edges" (defaults to planar in GeoArrow)
        let metadata: HashMap<String, String> = [
            (
                "ARROW:extension:name".to_string(),
                "geoarrow.wkb".to_string(),
            ),
            (
                "ARROW:extension:metadata".to_string(),
                r#"{"crs":"OGC:CRS84"}"#.to_string(),
            ),
        ]
        .into_iter()
        .collect();
        let field = adt::Field::new("geom", adt::DataType::Binary, true).with_metadata(metadata);

        let proto_field: sdt::StructField = field.try_into()?;

        assert_eq!(proto_field.name, "geom");
        assert_eq!(
            proto_field.data_type,
            Some(DataType {
                kind: Some(sdt::Kind::Geometry(sdt::Geometry {
                    srid: 4326,
                    type_variation_reference: 0,
                })),
            })
        );

        Ok(())
    }

    #[test]
    fn test_geoarrow_field_to_proto_geography() -> SparkResult<()> {
        // Create an Arrow field with geoarrow.wkb metadata for Geography (spherical)
        let metadata: HashMap<String, String> = [
            (
                "ARROW:extension:name".to_string(),
                "geoarrow.wkb".to_string(),
            ),
            (
                "ARROW:extension:metadata".to_string(),
                r#"{"crs":"OGC:CRS84","edges":"spherical"}"#.to_string(),
            ),
        ]
        .into_iter()
        .collect();
        let field = adt::Field::new("geog", adt::DataType::Binary, true).with_metadata(metadata);

        let proto_field: sdt::StructField = field.try_into()?;

        assert_eq!(proto_field.name, "geog");
        assert_eq!(
            proto_field.data_type,
            Some(DataType {
                kind: Some(sdt::Kind::Geography(sdt::Geography {
                    srid: 4326,
                    type_variation_reference: 0,
                })),
            })
        );

        Ok(())
    }

    #[test]
    fn test_geoarrow_field_mixed_srid() -> SparkResult<()> {
        // Test mixed SRID (-1): CRS and edges are omitted from metadata
        let metadata: HashMap<String, String> = [
            (
                "ARROW:extension:name".to_string(),
                "geoarrow.wkb".to_string(),
            ),
            ("ARROW:extension:metadata".to_string(), r#"{}"#.to_string()),
        ]
        .into_iter()
        .collect();
        let field = adt::Field::new("geom", adt::DataType::Binary, true).with_metadata(metadata);

        let proto_field: sdt::StructField = field.try_into()?;

        assert_eq!(
            proto_field.data_type,
            Some(DataType {
                kind: Some(sdt::Kind::Geometry(sdt::Geometry {
                    srid: -1,
                    type_variation_reference: 0,
                })),
            })
        );

        Ok(())
    }

    #[test]
    fn test_time_arrow_to_proto() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Kind, Time};

        // Time32 Second -> TIME(0)
        let arrow_type = adt::DataType::Time32(adt::TimeUnit::Second);
        assert_eq!(
            crate::spark::connect::DataType::try_from(arrow_type)?,
            DataType {
                kind: Some(Kind::Time(Time {
                    precision: Some(0),
                    type_variation_reference: 0,
                })),
            }
        );

        // Time32 Millisecond -> TIME(3)
        let arrow_type = adt::DataType::Time32(adt::TimeUnit::Millisecond);
        assert_eq!(
            crate::spark::connect::DataType::try_from(arrow_type)?,
            DataType {
                kind: Some(Kind::Time(Time {
                    precision: Some(3),
                    type_variation_reference: 0,
                })),
            }
        );

        // Time64 Microsecond -> TIME(6)
        let arrow_type = adt::DataType::Time64(adt::TimeUnit::Microsecond);
        assert_eq!(
            crate::spark::connect::DataType::try_from(arrow_type)?,
            DataType {
                kind: Some(Kind::Time(Time {
                    precision: Some(6),
                    type_variation_reference: 0,
                })),
            }
        );
        Ok(())
    }

    #[test]
    fn test_time_arrow_to_proto_unsupported() {
        // Invalid Arrow Time32 combinations (only Second and Millisecond are valid)
        let arrow_type = adt::DataType::Time32(adt::TimeUnit::Microsecond);
        assert!(DataType::try_from(arrow_type).is_err());

        let arrow_type = adt::DataType::Time32(adt::TimeUnit::Nanosecond);
        assert!(DataType::try_from(arrow_type).is_err());

        // Invalid Arrow Time64 combinations (only Microsecond and Nanosecond are valid)
        let arrow_type = adt::DataType::Time64(adt::TimeUnit::Second);
        assert!(DataType::try_from(arrow_type).is_err());

        let arrow_type = adt::DataType::Time64(adt::TimeUnit::Millisecond);
        assert!(DataType::try_from(arrow_type).is_err());

        // Time64 Nanosecond - valid Arrow but rejected by Spark (only precision 0, 3, 6 supported)
        let arrow_type = adt::DataType::Time64(adt::TimeUnit::Nanosecond);
        assert!(DataType::try_from(arrow_type).is_err());
    }
}
