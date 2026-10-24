use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema as adt;
use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use parquet_variant_compute::VariantType;
use sail_common::error::{CommonError, CommonResult};
use sail_common::geoarrow::extension::GeoArrowWkbType;
use sail_common::spec;
use sail_common::spec::{
    SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME,
};
use serde_json::json;

use crate::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

/// Converts spec data types, fields, and schemas to Arrow without query state.
pub struct DataTypeResolver {
    pub session_timezone: Arc<str>,
    pub default_timestamp_ltz: bool,
    pub use_large_var_types: bool,
}

/// Map SRID to CRS string for Arrow metadata.
///
/// Returns `None` for SRID -1 (mixed) since there is no single CRS for the column.
/// Only SRIDs supported by Spark 4.1 are accepted.
///
/// References:
///
/// - `org.apache.spark.sql.types.GeometryType#crs`
/// - `org.apache.spark.sql.types.GeographyType#crs`
/// - `org.apache.spark.sql.catalyst.util.CartesianSpatialReferenceSystemMapper`
/// - `org.apache.spark.sql.catalyst.util.GeographicSpatialReferenceSystemMapper`
fn srid_to_crs(srid: i32) -> CommonResult<Option<String>> {
    match srid {
        4326 => Ok(Some("OGC:CRS84".to_string())),
        3857 => Ok(Some("EPSG:3857".to_string())),
        0 => Ok(Some("SRID:0".to_string())),
        -1 => Ok(None),
        _ => Err(CommonError::invalid(format!(
            "unsupported SRID: {srid}. Supported values: 0, 3857, 4326, or ANY (-1)"
        ))),
    }
}

/// Validate SRID for Geometry type against Spark 4.1's CartesianSpatialReferenceSystemMapper.
/// Valid SRIDs: 0 (unspecified), 3857 (Web Mercator), 4326 (WGS84), -1 (mixed).
fn validate_geometry_srid(srid: i32) -> CommonResult<()> {
    if !matches!(srid, 0 | 3857 | 4326 | -1) {
        return Err(CommonError::invalid(format!(
            "SRID {srid} is not valid for GEOMETRY. Valid values: 0, 3857, 4326, or ANY"
        )));
    }
    Ok(())
}

/// Validate SRID for Geography type against Spark 4.1's GeographicSpatialReferenceSystemMapper.
/// Valid SRIDs: 4326 (WGS84), -1 (mixed).
fn validate_geography_srid(srid: i32) -> CommonResult<()> {
    if !matches!(srid, 4326 | -1) {
        return Err(CommonError::invalid(format!(
            "SRID {srid} is not valid for GEOGRAPHY. Valid values: 4326 or ANY"
        )));
    }
    Ok(())
}

impl DataTypeResolver {
    fn arrow_binary_type(&self) -> adt::DataType {
        if self.use_large_var_types {
            adt::DataType::LargeBinary
        } else {
            adt::DataType::Binary
        }
    }

    fn arrow_string_type(&self) -> adt::DataType {
        if self.use_large_var_types {
            adt::DataType::LargeUtf8
        } else {
            adt::DataType::Utf8
        }
    }

    /// References:
    ///   org.apache.spark.sql.util.ArrowUtils#toArrowType
    ///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
    pub fn resolve_data_type(&self, data_type: &spec::DataType) -> CommonResult<adt::DataType> {
        use spec::DataType;

        match data_type {
            DataType::Null => Ok(adt::DataType::Null),
            DataType::Boolean => Ok(adt::DataType::Boolean),
            DataType::Int8 => Ok(adt::DataType::Int8),
            DataType::Int16 => Ok(adt::DataType::Int16),
            DataType::Int32 => Ok(adt::DataType::Int32),
            DataType::Int64 => Ok(adt::DataType::Int64),
            DataType::UInt8 => Ok(adt::DataType::UInt8),
            DataType::UInt16 => Ok(adt::DataType::UInt16),
            DataType::UInt32 => Ok(adt::DataType::UInt32),
            DataType::UInt64 => Ok(adt::DataType::UInt64),
            DataType::Float16 => Ok(adt::DataType::Float16),
            DataType::Float32 => Ok(adt::DataType::Float32),
            DataType::Float64 => Ok(adt::DataType::Float64),
            DataType::Timestamp {
                time_unit,
                timestamp_type,
            } => Ok(adt::DataType::Timestamp(
                Self::resolve_time_unit(time_unit)?,
                self.resolve_timezone(timestamp_type)?,
            )),
            DataType::Date32 => Ok(adt::DataType::Date32),
            DataType::Date64 => Ok(adt::DataType::Date64),
            DataType::Time32 { time_unit } => {
                Ok(adt::DataType::Time32(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Time64 { time_unit } => {
                Ok(adt::DataType::Time64(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Duration { time_unit } => {
                Ok(adt::DataType::Duration(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Interval {
                interval_unit,
                start_field: _,
                end_field: _,
            } => match interval_unit {
                spec::IntervalUnit::YearMonth => {
                    Ok(adt::DataType::Interval(adt::IntervalUnit::YearMonth))
                }
                // Spark's DayTimeInterval has microsecond precision.
                // Arrow's IntervalUnit::DayTime has millisecond precision.
                // Use Duration to preserve microsecond precision.
                spec::IntervalUnit::DayTime => {
                    Ok(adt::DataType::Duration(adt::TimeUnit::Microsecond))
                }
                spec::IntervalUnit::MonthDayNano => {
                    Ok(adt::DataType::Interval(adt::IntervalUnit::MonthDayNano))
                }
            },
            DataType::Binary => Ok(adt::DataType::Binary),
            DataType::FixedSizeBinary { size } => Ok(adt::DataType::FixedSizeBinary(*size)),
            DataType::LargeBinary => Ok(adt::DataType::LargeBinary),
            DataType::BinaryView => Ok(adt::DataType::BinaryView),
            DataType::Utf8 => Ok(adt::DataType::Utf8),
            DataType::LargeUtf8 => Ok(adt::DataType::LargeUtf8),
            DataType::Utf8View => Ok(adt::DataType::Utf8View),
            DataType::List {
                data_type,
                nullable,
            } => {
                let field = spec::Field {
                    name: SAIL_LIST_FIELD_NAME.to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::List(Arc::new(self.resolve_field(&field)?)))
            }
            DataType::FixedSizeList {
                data_type,
                nullable,
                length,
            } => {
                let field = spec::Field {
                    name: SAIL_LIST_FIELD_NAME.to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::FixedSizeList(
                    Arc::new(self.resolve_field(&field)?),
                    *length,
                ))
            }
            DataType::LargeList {
                data_type,
                nullable,
            } => {
                let field = spec::Field {
                    name: SAIL_LIST_FIELD_NAME.to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::LargeList(Arc::new(
                    self.resolve_field(&field)?,
                )))
            }
            DataType::Struct { fields } => Ok(adt::DataType::Struct(self.resolve_fields(fields)?)),
            DataType::Union {
                union_fields,
                union_mode,
            } => {
                let union_fields = union_fields
                    .iter()
                    .map(|(i, field)| Ok((*i, Arc::new(self.resolve_field(field)?))))
                    .collect::<CommonResult<_>>()?;
                Ok(adt::DataType::Union(
                    union_fields,
                    Self::resolve_union_mode(union_mode),
                ))
            }
            DataType::Dictionary {
                key_type,
                value_type,
            } => Ok(adt::DataType::Dictionary(
                Box::new(self.resolve_data_type(key_type)?),
                Box::new(self.resolve_data_type(value_type)?),
            )),
            DataType::Decimal128 { precision, scale } => {
                Ok(adt::DataType::Decimal128(*precision, *scale))
            }
            DataType::Decimal256 { precision, scale } => {
                Ok(adt::DataType::Decimal256(*precision, *scale))
            }
            DataType::Map {
                key_type,
                value_type,
                value_type_nullable,
                keys_sorted,
            } => {
                let fields = spec::Fields::from(vec![
                    spec::Field {
                        name: SAIL_MAP_KEY_FIELD_NAME.to_string(),
                        data_type: *key_type.clone(),
                        nullable: false,
                        metadata: vec![],
                    },
                    spec::Field {
                        name: SAIL_MAP_VALUE_FIELD_NAME.to_string(),
                        data_type: *value_type.clone(),
                        nullable: *value_type_nullable,
                        metadata: vec![],
                    },
                ]);
                Ok(adt::DataType::Map(
                    Arc::new(adt::Field::new(
                        SAIL_MAP_FIELD_NAME,
                        adt::DataType::Struct(self.resolve_fields(&fields)?),
                        false,
                    )),
                    *keys_sorted,
                ))
            }
            DataType::Geometry { srid: _ } => {
                // Geometry types are stored as Binary (WKB-encoded)
                // Extension type metadata is added at the Field level, not DataType level
                // See resolve_field() for metadata handling
                Ok(adt::DataType::Binary)
            }
            DataType::Geography {
                srid: _,
                algorithm: _,
            } => {
                // Geography types are stored as Binary (WKB-encoded)
                // Extension type metadata is added at the Field level, not DataType level
                // See resolve_field() for metadata handling
                Ok(adt::DataType::Binary)
            }
            DataType::ConfiguredUtf8 { utf8_type: _ } => {
                // FIXME: Currently `length` and `utf8_type` is lost in translation.
                //  This impacts accuracy if `spec::ConfiguredUtf8Type` is `VarChar` or `Char`.
                Ok(self.arrow_string_type())
            }
            DataType::ConfiguredBinary => Ok(self.arrow_binary_type()),
            DataType::Variant => {
                // Variant layout using Binary for PySpark compatibility.
                // parquet-variant uses BinaryView internally but we convert to Binary
                let fields = adt::Fields::from(vec![
                    adt::Field::new(VARIANT_VALUE_FIELD_NAME, adt::DataType::Binary, false),
                    variant_metadata_field(adt::DataType::Binary, false),
                ]);
                Ok(adt::DataType::Struct(fields))
            }
            DataType::UserDefined { .. } => Err(CommonError::unsupported(
                "user defined data type should only exist in a field",
            )),
        }
    }

    pub fn resolve_field(&self, field: &spec::Field) -> CommonResult<adt::Field> {
        let spec::Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let mut metadata: HashMap<String, String> = metadata.iter().cloned().collect();
        if let spec::DataType::Interval {
            interval_unit,
            start_field,
            end_field,
        } = data_type
            && let Some(interval) =
                spec::SparkIntervalMetadata::try_new(*interval_unit, *start_field, *end_field)?
        {
            metadata.insert(
                spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                interval.to_json()?,
            );
        }
        let data_type = match data_type {
            spec::DataType::UserDefined {
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => {
                let udt = spec::SparkUdtMetadata {
                    jvm_class: jvm_class.clone(),
                    python_class: python_class.clone(),
                    serialized_python_class: serialized_python_class.clone(),
                };
                metadata.insert(
                    spec::SAIL_SPARK_UDT_METADATA_KEY.to_string(),
                    serde_json::to_string(&udt).map_err(|e| {
                        CommonError::internal(format!("failed to serialize UDT metadata: {e}"))
                    })?,
                );
                sql_type
            }
            spec::DataType::Geometry { srid } => {
                validate_geometry_srid(*srid)?;
                // Add geoarrow extension type metadata for WKB-encoded geometries.
                // ARROW:extension:* keys follow the Apache Arrow extension type standard
                // and are automatically filtered from Spark client responses.
                // Edges default to planar in GeoArrow, so we omit them for Geometry.
                metadata.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    GeoArrowWkbType::NAME.to_string(),
                );
                let mut ext = json!({});
                if let Some(crs) = srid_to_crs(*srid)? {
                    ext["crs"] = serde_json::Value::String(crs);
                }
                metadata.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), ext.to_string());
                data_type
            }
            spec::DataType::Geography { srid, algorithm: _ } => {
                validate_geography_srid(*srid)?;
                // Add geoarrow extension type metadata for WKB-encoded geographies.
                // ARROW:extension:* keys follow the Apache Arrow extension type standard
                // and are automatically filtered from Spark client responses.
                metadata.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    GeoArrowWkbType::NAME.to_string(),
                );
                let mut ext = json!({"edges": "spherical"});
                if let Some(crs) = srid_to_crs(*srid)? {
                    ext["crs"] = serde_json::Value::String(crs);
                }
                metadata.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), ext.to_string());
                data_type
            }
            spec::DataType::Variant => {
                metadata.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    VariantType::NAME.to_string(),
                );
                data_type
            }
            x => x,
        };
        Ok(
            adt::Field::new(name, self.resolve_data_type(data_type)?, *nullable)
                .with_metadata(metadata),
        )
    }

    pub fn resolve_fields(&self, fields: &spec::Fields) -> CommonResult<adt::Fields> {
        let fields = fields
            .into_iter()
            .map(|f| self.resolve_field(f))
            .collect::<CommonResult<Vec<_>>>()?;
        Ok(adt::Fields::from(fields))
    }

    pub fn resolve_schema(&self, schema: spec::Schema) -> CommonResult<adt::Schema> {
        let fields = self.resolve_fields(&schema.fields)?;
        Ok(adt::Schema::new(fields))
    }

    pub fn resolve_time_unit(time_unit: &spec::TimeUnit) -> CommonResult<adt::TimeUnit> {
        match time_unit {
            spec::TimeUnit::Second => Ok(adt::TimeUnit::Second),
            spec::TimeUnit::Millisecond => Ok(adt::TimeUnit::Millisecond),
            spec::TimeUnit::Microsecond => Ok(adt::TimeUnit::Microsecond),
            spec::TimeUnit::Nanosecond => Ok(adt::TimeUnit::Nanosecond),
        }
    }

    pub fn resolve_union_mode(union_mode: &spec::UnionMode) -> adt::UnionMode {
        match union_mode {
            spec::UnionMode::Sparse => adt::UnionMode::Sparse,
            spec::UnionMode::Dense => adt::UnionMode::Dense,
        }
    }

    pub fn resolve_timezone(
        &self,
        timestamp_type: &spec::TimestampType,
    ) -> CommonResult<Option<Arc<str>>> {
        match timestamp_type {
            spec::TimestampType::Configured => match self.default_timestamp_ltz {
                true => Ok(Some(Arc::clone(&self.session_timezone))),
                false => Ok(None),
            },
            spec::TimestampType::WithLocalTimeZone => Ok(Some(Arc::clone(&self.session_timezone))),
            spec::TimestampType::WithoutTimeZone => Ok(None),
        }
    }
}
