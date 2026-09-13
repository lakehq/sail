use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use datafusion::arrow::datatypes as adt;
use parquet_variant_compute::VariantType;
use sail_common::geoarrow::extension::GeoArrowWkbType;
use sail_common::spec;
use sail_common::spec::{
    SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME,
    SAIL_ORIGINAL_FIELD_NAME_METADATA_KEY,
};
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};
use serde_json::json;

use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

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
fn srid_to_crs(srid: i32) -> PlanResult<Option<String>> {
    match srid {
        4326 => Ok(Some("OGC:CRS84".to_string())),
        3857 => Ok(Some("EPSG:3857".to_string())),
        0 => Ok(Some("SRID:0".to_string())),
        -1 => Ok(None),
        _ => Err(PlanError::invalid(format!(
            "unsupported SRID: {srid}. Supported values: 0, 3857, 4326, or ANY (-1)"
        ))),
    }
}

/// Validate SRID for Geometry type against Spark 4.1's CartesianSpatialReferenceSystemMapper.
/// Valid SRIDs: 0 (unspecified), 3857 (Web Mercator), 4326 (WGS84), -1 (mixed).
fn validate_geometry_srid(srid: i32) -> PlanResult<()> {
    if !matches!(srid, 0 | 3857 | 4326 | -1) {
        return Err(PlanError::invalid(format!(
            "SRID {srid} is not valid for GEOMETRY. Valid values: 0, 3857, 4326, or ANY"
        )));
    }
    Ok(())
}

/// Validate SRID for Geography type against Spark 4.1's GeographicSpatialReferenceSystemMapper.
/// Valid SRIDs: 4326 (WGS84), -1 (mixed).
fn validate_geography_srid(srid: i32) -> PlanResult<()> {
    if !matches!(srid, 4326 | -1) {
        return Err(PlanError::invalid(format!(
            "SRID {srid} is not valid for GEOGRAPHY. Valid values: 4326 or ANY"
        )));
    }
    Ok(())
}

impl PlanResolver<'_> {
    fn arrow_binary_type(&self, state: &mut PlanResolverState) -> adt::DataType {
        if self.config.arrow_use_large_var_types && state.config().arrow_allow_large_var_types {
            adt::DataType::LargeBinary
        } else {
            adt::DataType::Binary
        }
    }

    fn arrow_string_type(&self, state: &mut PlanResolverState) -> adt::DataType {
        if self.config.arrow_use_large_var_types && state.config().arrow_allow_large_var_types {
            adt::DataType::LargeUtf8
        } else {
            adt::DataType::Utf8
        }
    }

    pub fn resolve_data_type_for_plan(
        &self,
        data_type: &spec::DataType,
    ) -> PlanResult<adt::DataType> {
        let mut state = PlanResolverState::new();
        self.resolve_data_type(data_type, &mut state)
    }

    /// References:
    ///   org.apache.spark.sql.util.ArrowUtils#toArrowType
    ///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
    pub(super) fn resolve_data_type(
        &self,
        data_type: &spec::DataType,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::DataType> {
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
                Ok(adt::DataType::List(Arc::new(
                    self.resolve_field(&field, state)?,
                )))
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
                    Arc::new(self.resolve_field(&field, state)?),
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
                    self.resolve_field(&field, state)?,
                )))
            }
            DataType::Struct { fields } => {
                let mut fields = self.resolve_fields(fields, state)?;
                deduplicate_field_names(&mut fields);
                Ok(adt::DataType::Struct(fields))
            }
            DataType::Union {
                union_fields,
                union_mode,
            } => {
                let union_fields = union_fields
                    .iter()
                    .map(|(i, field)| Ok((*i, Arc::new(self.resolve_field(field, state)?))))
                    .collect::<PlanResult<_>>()?;
                Ok(adt::DataType::Union(
                    union_fields,
                    Self::resolve_union_mode(union_mode),
                ))
            }
            DataType::Dictionary {
                key_type,
                value_type,
            } => Ok(adt::DataType::Dictionary(
                Box::new(self.resolve_data_type(key_type, state)?),
                Box::new(self.resolve_data_type(value_type, state)?),
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
                        adt::DataType::Struct(self.resolve_fields(&fields, state)?),
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
                Ok(self.arrow_string_type(state))
            }
            DataType::ConfiguredBinary => Ok(self.arrow_binary_type(state)),
            DataType::Variant => {
                // Variant layout using Binary for PySpark compatibility.
                // parquet-variant uses BinaryView internally but we convert to Binary
                let fields = adt::Fields::from(vec![
                    adt::Field::new(VARIANT_VALUE_FIELD_NAME, adt::DataType::Binary, false),
                    variant_metadata_field(adt::DataType::Binary, false),
                ]);
                Ok(adt::DataType::Struct(fields))
            }
            DataType::UserDefined { .. } => Err(PlanError::unsupported(
                "user defined data type should only exist in a field",
            )),
        }
    }

    pub(super) fn resolve_field(
        &self,
        field: &spec::Field,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Field> {
        let spec::Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let mut metadata: HashMap<String, String> = metadata.iter().cloned().collect();
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
                        PlanError::internal(format!("failed to serialize UDT metadata: {e}"))
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
            adt::Field::new(name, self.resolve_data_type(data_type, state)?, *nullable)
                .with_metadata(metadata),
        )
    }

    pub(super) fn resolve_fields(
        &self,
        fields: &spec::Fields,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Fields> {
        let fields = fields
            .into_iter()
            .map(|f| self.resolve_field(f, state))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(adt::Fields::from(fields))
    }

    pub(super) fn resolve_schema(
        &self,
        schema: spec::Schema,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Schema> {
        let fields = self.resolve_fields(&schema.fields, state)?;
        Ok(adt::Schema::new(fields))
    }

    pub fn resolve_time_unit(time_unit: &spec::TimeUnit) -> PlanResult<adt::TimeUnit> {
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
    ) -> PlanResult<Option<Arc<str>>> {
        match timestamp_type {
            spec::TimestampType::Configured => match self.config.default_timestamp_type {
                DefaultTimestampType::TimestampLtz => {
                    Ok(Some(Arc::clone(&self.config.session_timezone)))
                }
                DefaultTimestampType::TimestampNtz => Ok(None),
            },
            spec::TimestampType::WithLocalTimeZone => {
                Ok(Some(Arc::clone(&self.config.session_timezone)))
            }
            spec::TimestampType::WithoutTimeZone => Ok(None),
        }
    }
}

/// Deduplicate duplicate field names within an Arrow struct in place.
///
/// Arrow struct fields are addressed by name, so duplicate names in a Spark struct
/// (which Spark allows) must be made unique for the Arrow representation. Duplicates
/// receive a numeric suffix (`name_0`, `name_1`, ...) in order of appearance, matching
/// Spark's `deduplicateFieldNames` in `ArrowUtils.scala`. Unique names are left untouched.
///
/// Only the direct children are renamed here; nested structs are already deduplicated
/// because struct resolution recurses through this function.
///
/// The original name is stored in the field metadata under
/// [`SAIL_ORIGINAL_FIELD_NAME_METADATA_KEY`] so the logical Spark schema reported
/// back to the client keeps the user-visible name.
///
/// Reference: https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
fn deduplicate_field_names(fields: &mut adt::Fields) {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for field in fields.iter() {
        *counts.entry(field.name().as_str()).or_default() += 1;
    }
    if counts.values().all(|&c| c == 1) {
        return;
    }
    let mut next: HashMap<String, usize> = HashMap::new();
    let renamed = fields
        .iter()
        .map(|field| {
            if counts[field.name().as_str()] > 1 {
                let i = next.entry(field.name().clone()).or_default();
                let new_name = format!("{}_{i}", field.name());
                *i += 1;
                let original_name = field.name().clone();
                let mut metadata = field.metadata().clone();
                metadata.insert(
                    SAIL_ORIGINAL_FIELD_NAME_METADATA_KEY.to_string(),
                    original_name,
                );
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_name(new_name)
                        .with_metadata(metadata),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    *fields = adt::Fields::from(renamed);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
    use sail_catalog::provider::CatalogProvider;
    use sail_catalog_memory::MemoryCatalogProvider;
    use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
    use sail_common_datafusion::session::plan::PlanService;

    use super::*;
    use crate::catalog::SparkCatalogObjectDisplay;
    use crate::config::PlanConfig;
    use crate::formatter::SparkPlanFormatter;
    use crate::resolver::PlanResolver;

    fn f(name: &str) -> adt::Field {
        adt::Field::new(name, adt::DataType::Int32, true)
    }

    #[test]
    fn test_deduplicate_field_names() {
        // No duplicates: unchanged.
        let mut fields = adt::Fields::from(vec![f("a"), f("b")]);
        deduplicate_field_names(&mut fields);
        let names: Vec<_> = fields.iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["a", "b"]);

        // Duplicates get deterministic numeric suffixes in order of appearance.
        let mut fields = adt::Fields::from(vec![f("x"), f("a"), f("x"), f("x")]);
        deduplicate_field_names(&mut fields);
        let names: Vec<_> = fields.iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["x_0", "a", "x_1", "x_2"]);

        // Renamed fields carry their original name so it can be restored when the
        // schema is reported back to the client; unchanged fields carry no such key.
        let original = |f: &adt::Field| {
            f.metadata()
                .get(SAIL_ORIGINAL_FIELD_NAME_METADATA_KEY)
                .cloned()
        };
        assert_eq!(original(&fields[0]).as_deref(), Some("x"));
        assert_eq!(original(&fields[1]), None);
        assert_eq!(original(&fields[2]).as_deref(), Some("x"));
        assert_eq!(original(&fields[3]).as_deref(), Some("x"));
    }

    fn create_session() -> PlanResult<SessionContext> {
        let mut state = SessionStateBuilder::new().build();
        let catalog_manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "sail".to_string(),
                Arc::new(MemoryCatalogProvider::new(
                    "sail".to_string(),
                    vec![Arc::from("default")].try_into()?,
                    None,
                )) as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "sail".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })?;
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        state.config_mut().set_extension(Arc::new(catalog_manager));
        state.config_mut().set_extension(Arc::new(plan_service));
        Ok(SessionContext::new_with_state(state))
    }

    fn spec_field(name: &str, data_type: spec::DataType) -> spec::Field {
        spec::Field {
            name: name.to_string(),
            data_type,
            nullable: true,
            metadata: vec![],
        }
    }

    /// End-to-end resolution of the nested struct schema used by the PySpark
    /// `test_createDataFrame_duplicate_field_names` / `test_toPandas_duplicate_field_names`
    /// cases: a struct with duplicated nested field names must resolve to an Arrow
    /// struct with unique names.
    #[test]
    fn test_resolve_struct_with_duplicate_nested_field_names() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        // struct<x: string, x: int, y: int, y: string>
        let data_type = spec::DataType::Struct {
            fields: spec::Fields::from(vec![
                spec_field("x", spec::DataType::Utf8),
                spec_field("x", spec::DataType::Int32),
                spec_field("y", spec::DataType::Int32),
                spec_field("y", spec::DataType::Utf8),
            ]),
        };

        let adt::DataType::Struct(fields) = resolver.resolve_data_type(&data_type, &mut state)?
        else {
            return Err(PlanError::internal("expected struct"));
        };
        let names: Vec<_> = fields.iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["x_0", "x_1", "y_0", "y_1"]);
        Ok(())
    }

    /// A struct without duplicates must be left untouched.
    #[test]
    fn test_resolve_struct_without_duplicates_unchanged() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let data_type = spec::DataType::Struct {
            fields: spec::Fields::from(vec![
                spec_field("a", spec::DataType::Int32),
                spec_field("b", spec::DataType::Utf8),
            ]),
        };

        let adt::DataType::Struct(fields) = resolver.resolve_data_type(&data_type, &mut state)?
        else {
            return Err(PlanError::internal("expected struct"));
        };
        let names: Vec<_> = fields.iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["a", "b"]);
        Ok(())
    }
}
