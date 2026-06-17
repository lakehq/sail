use arrow_schema::extension::ExtensionType;
use datafusion::arrow::datatypes as adt;
use parquet_variant_compute::VariantType;
use sail_common::geoarrow::extension::{GeoArrowMetadata, GeoArrowWkbType};
use sail_common::spec;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::{data_type as sdt, DataType};

/// Spark geometry and geography type metadata.
#[derive(Debug, Clone)]
struct SparkGeoMetadata {
    kind: SparkGeoKind,
    /// Spatial Reference System Identifier (SRID). -1 for mixed SRID.
    srid: i32,
}

#[derive(Debug, Clone)]
enum SparkGeoKind {
    Geometry,
    Geography,
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

impl TryFrom<GeoArrowMetadata> for SparkGeoMetadata {
    type Error = SparkError;

    fn try_from(metadata: GeoArrowMetadata) -> SparkResult<Self> {
        let srid = match metadata.crs {
            Some(crs) => crs_to_srid(&crs.authority_code())?,
            // Absent CRS means mixed/unknown SRID.
            None => -1,
        };
        // `Some` edge (non-planar edges) indicates Geography.
        // `None` edge (planar/linear edges) indicates Geometry.
        let kind = match metadata.edges {
            Some(_) => SparkGeoKind::Geography,
            None => SparkGeoKind::Geometry,
        };

        Ok(Self { kind, srid })
    }
}

impl SparkGeoMetadata {
    /// Parse Spark geo metadata from JSON string.
    #[cfg(test)]
    fn from_json(metadata: &str) -> SparkResult<Self> {
        let metadata: GeoArrowMetadata = serde_json::from_str(metadata)
            .map_err(|e| SparkError::invalid(format!("invalid geoarrow metadata JSON: {e}")))?;
        metadata.try_into()
    }
}

impl TryFrom<adt::Field> for sdt::StructField {
    type Error = SparkError;

    fn try_from(field: adt::Field) -> SparkResult<sdt::StructField> {
        let udt_metadata = field.metadata().get(spec::SAIL_SPARK_UDT_METADATA_KEY);
        let extension_type_name = field.extension_type_name();

        let data_type = if let Some(udt_metadata) = udt_metadata {
            let udt_metadata: spec::SparkUdtMetadata = serde_json::from_str(udt_metadata)?;
            DataType {
                kind: Some(sdt::Kind::Udt(Box::new(sdt::Udt {
                    r#type: "udt".to_string(),
                    jvm_class: udt_metadata.jvm_class,
                    python_class: udt_metadata.python_class,
                    serialized_python_class: udt_metadata.serialized_python_class,
                    sql_type: Some(Box::new(field.data_type().clone().try_into()?)),
                }))),
            }
        } else if extension_type_name == Some(GeoArrowWkbType::NAME) {
            let ext = field.try_extension_type::<GeoArrowWkbType>()?;
            let meta: SparkGeoMetadata = ext.metadata.try_into()?;

            match meta.kind {
                SparkGeoKind::Geography => DataType {
                    kind: Some(sdt::Kind::Geography(sdt::Geography {
                        srid: meta.srid,
                        type_variation_reference: 0,
                    })),
                },
                SparkGeoKind::Geometry => DataType {
                    kind: Some(sdt::Kind::Geometry(sdt::Geometry {
                        srid: meta.srid,
                        type_variation_reference: 0,
                    })),
                },
            }
        } else if extension_type_name == Some(VariantType::NAME) {
            field.try_extension_type::<VariantType>()?;
            DataType {
                kind: Some(sdt::Kind::Variant(sdt::Variant {
                    type_variation_reference: 0,
                })),
            }
        } else {
            field.data_type().clone().try_into()?
        };
        Ok(sdt::StructField {
            name: field.name().clone(),
            data_type: Some(data_type),
            nullable: field.is_nullable(),
            metadata: field.metadata().get(spec::SPARK_METADATA_JSON_KEY).cloned(),
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
    use std::collections::HashMap;

    use super::*;
    use crate::error::SparkResult;

    #[test]
    fn test_geoarrow_metadata_parsing() -> SparkResult<()> {
        // Test Geometry (no edges) with Spark 4.1 CRS format
        let metadata = r#"{"crs":"OGC:CRS84"}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert!(matches!(parsed.kind, SparkGeoKind::Geometry));
        assert_eq!(parsed.srid, 4326);

        // Test Geography (spherical edges) with Spark 4.1 CRS format
        let metadata = r#"{"crs":"OGC:CRS84","edges":"spherical"}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert!(matches!(parsed.kind, SparkGeoKind::Geography));
        assert_eq!(parsed.srid, 4326);

        // Test other valid GeoArrow edge types are accepted as Geography
        for edge_type in &["vincenty", "thomas", "andoyer", "karney"] {
            let metadata = format!(r#"{{"crs":"OGC:CRS84","edges":"{edge_type}"}}"#);
            let parsed = SparkGeoMetadata::from_json(&metadata)?;
            assert!(matches!(parsed.kind, SparkGeoKind::Geography));
        }

        // Test Web Mercator projection (Geometry, no edges)
        let metadata = r#"{"crs":"EPSG:3857"}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, 3857);
        assert!(matches!(parsed.kind, SparkGeoKind::Geometry));

        // Test absent CRS means mixed/unknown SRID
        let metadata = r#"{"edges":"spherical"}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, -1);

        // Test empty metadata (no CRS, no edges) = Geometry with mixed SRID
        let metadata = r#"{}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert!(matches!(parsed.kind, SparkGeoKind::Geometry));
        assert_eq!(parsed.srid, -1);

        // Test PROJJSON CRS (used by GeoPandas, DuckDB, SedonaDB)
        let metadata = r#"{"crs":{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84 (CRS84)","id":{"authority":"OGC","code":"CRS84"}},"edges":"spherical"}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert!(matches!(parsed.kind, SparkGeoKind::Geography));
        assert_eq!(parsed.srid, 4326);

        // Test PROJJSON with numeric EPSG code (Geometry)
        let metadata = r#"{"crs":{"type":"GeographicCRS","name":"NAD83","id":{"authority":"EPSG","code":3857}}}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert!(matches!(parsed.kind, SparkGeoKind::Geometry));
        assert_eq!(parsed.srid, 3857);

        // Test SRID:0 (unspecified)
        let metadata = r#"{"crs":"SRID:0"}"#;
        let parsed = SparkGeoMetadata::from_json(metadata)?;
        assert_eq!(parsed.srid, 0);

        // Test error on invalid JSON
        assert!(SparkGeoMetadata::from_json("invalid").is_err());

        // Test error on unsupported CRS (Spark 4.1 only supports OGC:CRS84, EPSG:3857, SRID:0)
        assert!(SparkGeoMetadata::from_json(r#"{"crs":"EPSG:32632"}"#).is_err());
        assert!(SparkGeoMetadata::from_json(r#"{"crs":"EPSG:4326"}"#).is_err());
        assert!(SparkGeoMetadata::from_json(r#"{"crs":"UNKNOWN:FOO"}"#).is_err());

        // Test error on invalid edges value
        assert!(SparkGeoMetadata::from_json(r#"{"edges":"planar"}"#).is_err());
        assert!(SparkGeoMetadata::from_json(r#"{"edges":"invalid"}"#).is_err());

        // Test error on PROJJSON without id member
        assert!(SparkGeoMetadata::from_json(r#"{"crs":{"type":"GeographicCRS"}}"#).is_err());

        // Test error on PROJJSON with unsupported authority:code
        assert!(SparkGeoMetadata::from_json(
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
                arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
                GeoArrowWkbType::NAME.to_string(),
            ),
            (
                arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
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
                arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
                GeoArrowWkbType::NAME.to_string(),
            ),
            (
                arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
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
                arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
                GeoArrowWkbType::NAME.to_string(),
            ),
            (
                arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
                r#"{}"#.to_string(),
            ),
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
            DataType::try_from(arrow_type)?,
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
            DataType::try_from(arrow_type)?,
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
            DataType::try_from(arrow_type)?,
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
