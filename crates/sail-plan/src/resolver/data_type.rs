use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes as adt;
use sail_common::spec;

use crate::config::TimestampType;
use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
                timezone_info,
            } => Ok(adt::DataType::Timestamp(
                Self::resolve_time_unit(time_unit)?,
                Self::resolve_timezone(
                    timezone_info,
                    self.config.system_timezone.as_str(),
                    &self.config.timestamp_type,
                )?,
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
            } => {
                // TODO: Currently `start_field` and `end_field` is lost in translation.
                //  This does not impact computation accuracy,
                //  This may affect the display string in the `data_type_to_simple_string` function.
                Ok(adt::DataType::Interval(Self::resolve_interval_unit(
                    interval_unit,
                )))
            }
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
                    name: "item".to_string(),
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
                    name: "item".to_string(),
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
                    name: "item".to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::LargeList(Arc::new(
                    self.resolve_field(&field, state)?,
                )))
            }
            DataType::Struct { fields } => {
                Ok(adt::DataType::Struct(self.resolve_fields(fields, state)?))
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
                        name: "key".to_string(),
                        data_type: *key_type.clone(),
                        nullable: false,
                        metadata: vec![],
                    },
                    spec::Field {
                        name: "value".to_string(),
                        data_type: *value_type.clone(),
                        nullable: *value_type_nullable,
                        metadata: vec![],
                    },
                ]);
                Ok(adt::DataType::Map(
                    Arc::new(adt::Field::new(
                        "entries",
                        adt::DataType::Struct(self.resolve_fields(&fields, state)?),
                        false,
                    )),
                    *keys_sorted,
                ))
            }
            DataType::ConfiguredUtf8 { utf8_type: _ } => {
                // FIXME: Currently `length` and `utf8_type` is lost in translation.
                //  This impacts accuracy if `spec::ConfiguredUtf8Type` is `VarChar` or `Char`.
                Ok(self.arrow_string_type(state))
            }
            DataType::ConfiguredBinary => Ok(self.arrow_binary_type(state)),
            DataType::UserDefined { .. } => Err(PlanError::unsupported(
                "user defined data type should only exist in a field",
            )),
        }
    }

    pub fn unresolve_data_type(data_type: &adt::DataType) -> PlanResult<spec::DataType> {
        // TODO: unresolve_data_type is no longer needed, remove it
        use spec::DataType;

        match data_type {
            adt::DataType::Null => Ok(DataType::Null),
            adt::DataType::Boolean => Ok(DataType::Boolean),
            adt::DataType::Int8 => Ok(DataType::Int8),
            adt::DataType::Int16 => Ok(DataType::Int16),
            adt::DataType::Int32 => Ok(DataType::Int32),
            adt::DataType::Int64 => Ok(DataType::Int64),
            adt::DataType::UInt8 => Ok(DataType::UInt8),
            adt::DataType::UInt16 => Ok(DataType::UInt16),
            adt::DataType::UInt32 => Ok(DataType::UInt32),
            adt::DataType::UInt64 => Ok(DataType::UInt64),
            adt::DataType::Float16 => Ok(DataType::Float16),
            adt::DataType::Float32 => Ok(DataType::Float32),
            adt::DataType::Float64 => Ok(DataType::Float64),
            adt::DataType::Timestamp(time_unit, timezone) => Ok(DataType::Timestamp {
                time_unit: Self::unresolve_time_unit(time_unit)?,
                timezone_info: Self::unresolve_timezone(timezone)?,
            }),
            adt::DataType::Date32 => Ok(DataType::Date32),
            adt::DataType::Date64 => Ok(DataType::Date64),
            adt::DataType::Time32(time_unit) => Ok(DataType::Time32 {
                time_unit: Self::unresolve_time_unit(time_unit)?,
            }),
            adt::DataType::Time64(time_unit) => Ok(DataType::Time64 {
                time_unit: Self::unresolve_time_unit(time_unit)?,
            }),
            adt::DataType::Duration(time_unit) => Ok(DataType::Duration {
                time_unit: Self::unresolve_time_unit(time_unit)?,
            }),
            adt::DataType::Interval(interval_unit) => {
                // TODO: Currently `start_field` and `end_field` are lost in translation.
                //  This does not impact computation accuracy,
                //  This may affect the display string in the `data_type_to_simple_string` function.
                Ok(DataType::Interval {
                    interval_unit: Self::unresolve_interval_unit(interval_unit),
                    start_field: None,
                    end_field: None,
                })
            }
            adt::DataType::Binary => Ok(DataType::Binary),
            adt::DataType::FixedSizeBinary(i32) => Ok(DataType::FixedSizeBinary { size: *i32 }),
            adt::DataType::LargeBinary => Ok(DataType::LargeBinary),
            adt::DataType::BinaryView => Ok(DataType::BinaryView),
            adt::DataType::Utf8 => Ok(DataType::Utf8),
            adt::DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            adt::DataType::Utf8View => Ok(DataType::Utf8View),
            adt::DataType::List(field) => {
                let field = Self::unresolve_field(field)?;
                Ok(DataType::List {
                    data_type: Box::new(field.data_type),
                    nullable: field.nullable,
                })
            }
            adt::DataType::FixedSizeList(field, i32) => {
                let field = Self::unresolve_field(field)?;
                Ok(DataType::FixedSizeList {
                    data_type: Box::new(field.data_type),
                    nullable: field.nullable,
                    length: *i32,
                })
            }
            adt::DataType::LargeList(field) => {
                let field = Self::unresolve_field(field)?;
                Ok(DataType::LargeList {
                    data_type: Box::new(field.data_type),
                    nullable: field.nullable,
                })
            }
            adt::DataType::Struct(fields) => Ok(DataType::Struct {
                fields: Self::unresolve_fields(fields)?,
            }),
            adt::DataType::Union(union_fields, union_mode) => {
                let union_fields = union_fields
                    .iter()
                    .map(|(i, field)| Ok((i, Arc::new(Self::unresolve_field(field)?))))
                    .collect::<PlanResult<_>>()?;
                Ok(DataType::Union {
                    union_fields,
                    union_mode: Self::unresolve_union_mode(union_mode),
                })
            }
            adt::DataType::Dictionary(key_type, value_type) => Ok(DataType::Dictionary {
                key_type: Box::new(Self::unresolve_data_type(key_type)?),
                value_type: Box::new(Self::unresolve_data_type(value_type)?),
            }),
            adt::DataType::Decimal128(u8, i8) => Ok(DataType::Decimal128 {
                precision: *u8,
                scale: *i8,
            }),
            adt::DataType::Decimal256(u8, i8) => Ok(DataType::Decimal256 {
                precision: *u8,
                scale: *i8,
            }),
            adt::DataType::Map(field, keys_are_sorted) => {
                let field: spec::Field = Self::unresolve_field(field)?;
                let fields = match field.data_type {
                    DataType::Struct { fields } => fields,
                    _ => return Err(PlanError::invalid("invalid map data type")),
                };
                if fields.len() != 2 {
                    return Err(PlanError::invalid(
                        "map data type must have key and value fields",
                    ));
                }
                Ok(DataType::Map {
                    key_type: Box::new(fields[0].data_type.clone()),
                    value_type: Box::new(fields[1].data_type.clone()),
                    value_type_nullable: fields[1].nullable,
                    keys_sorted: *keys_are_sorted,
                })
            }
            adt::DataType::ListView(_) => {
                // Not yet fully supported by Arrow
                Err(PlanError::unsupported("unresolve_data_type ListView"))
            }
            adt::DataType::LargeListView(_) => {
                // Not yet fully supported by Arrow
                Err(PlanError::unsupported("unresolve_data_type LargeListView"))
            }
            adt::DataType::RunEndEncoded(_, _) => {
                Err(PlanError::unsupported("unresolve_data_type RunEndEncoded"))
            }
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
        let mut metadata: HashMap<_, _> = metadata
            .iter()
            .map(|(k, v)| (format!("metadata.{}", k), v.to_string()))
            .collect();
        let data_type = match data_type {
            spec::DataType::UserDefined {
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => {
                if let Some(jvm_class) = jvm_class {
                    metadata.insert("udt.jvm_class".to_string(), jvm_class.to_string());
                }
                if let Some(python_class) = python_class {
                    metadata.insert("udt.python_class".to_string(), python_class.to_string());
                }
                if let Some(serialized_python_class) = serialized_python_class {
                    metadata.insert(
                        "udt.serialized_python_class".to_string(),
                        serialized_python_class.to_string(),
                    );
                }
                sql_type
            }
            x => x,
        };
        Ok(
            adt::Field::new(name, self.resolve_data_type(data_type, state)?, *nullable)
                .with_metadata(metadata),
        )
    }

    pub fn unresolve_field(field: &adt::Field) -> PlanResult<spec::Field> {
        let name = field.name();
        let is_udt = field.metadata().keys().any(|k| k.starts_with("udt."));
        let data_type = if is_udt {
            spec::DataType::UserDefined {
                jvm_class: field.metadata().get("udt.jvm_class").cloned(),
                python_class: field.metadata().get("udt.python_class").cloned(),
                serialized_python_class: field
                    .metadata()
                    .get("udt.serialized_python_class")
                    .cloned(),
                sql_type: Box::new(Self::unresolve_data_type(field.data_type())?),
            }
        } else {
            Self::unresolve_data_type(field.data_type())?
        };
        let metadata = field
            .metadata()
            .iter()
            .filter(|(k, _)| k.starts_with("metadata."))
            .map(|(k, v)| (k["metadata.".len()..].to_string(), v.clone()))
            .collect::<HashMap<_, _>>();
        Ok(spec::Field {
            name: name.clone(),
            data_type,
            nullable: field.is_nullable(),
            metadata: metadata.into_iter().collect(),
        })
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

    pub fn unresolve_fields(fields: &adt::Fields) -> PlanResult<spec::Fields> {
        let fields = fields
            .iter()
            .map(|f| Self::unresolve_field(f))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(spec::Fields::from(fields))
    }

    pub(super) fn resolve_schema(
        &self,
        schema: spec::Schema,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Schema> {
        let fields = self.resolve_fields(&schema.fields, state)?;
        Ok(adt::Schema::new(fields))
    }

    pub fn unresolve_schema(schema: adt::SchemaRef) -> PlanResult<spec::Schema> {
        let fields = Self::unresolve_fields(schema.fields())?;
        Ok(spec::Schema { fields })
    }

    pub fn resolve_optional_time_unit(
        time_unit: &Option<spec::TimeUnit>,
    ) -> PlanResult<adt::TimeUnit> {
        match time_unit {
            None => Ok(adt::TimeUnit::Microsecond),
            Some(unit) => Self::resolve_time_unit(unit),
        }
    }

    pub fn resolve_time_unit(time_unit: &spec::TimeUnit) -> PlanResult<adt::TimeUnit> {
        match time_unit {
            spec::TimeUnit::Second => Ok(adt::TimeUnit::Second),
            spec::TimeUnit::Millisecond => Ok(adt::TimeUnit::Millisecond),
            spec::TimeUnit::Microsecond => Ok(adt::TimeUnit::Microsecond),
            spec::TimeUnit::Nanosecond => Ok(adt::TimeUnit::Nanosecond),
        }
    }

    pub fn unresolve_optional_time_unit(
        time_unit: &Option<adt::TimeUnit>,
    ) -> PlanResult<spec::TimeUnit> {
        match time_unit {
            None => Ok(spec::TimeUnit::Microsecond),
            Some(unit) => Self::unresolve_time_unit(unit),
        }
    }

    pub fn unresolve_time_unit(time_unit: &adt::TimeUnit) -> PlanResult<spec::TimeUnit> {
        match time_unit {
            adt::TimeUnit::Second => Ok(spec::TimeUnit::Second),
            adt::TimeUnit::Millisecond => Ok(spec::TimeUnit::Millisecond),
            adt::TimeUnit::Microsecond => Ok(spec::TimeUnit::Microsecond),
            adt::TimeUnit::Nanosecond => Ok(spec::TimeUnit::Nanosecond),
        }
    }

    pub fn resolve_interval_unit(interval_unit: &spec::IntervalUnit) -> adt::IntervalUnit {
        match interval_unit {
            spec::IntervalUnit::YearMonth => adt::IntervalUnit::YearMonth,
            spec::IntervalUnit::DayTime => adt::IntervalUnit::DayTime,
            spec::IntervalUnit::MonthDayNano => adt::IntervalUnit::MonthDayNano,
        }
    }

    pub fn unresolve_interval_unit(interval_unit: &adt::IntervalUnit) -> spec::IntervalUnit {
        match interval_unit {
            adt::IntervalUnit::YearMonth => spec::IntervalUnit::YearMonth,
            adt::IntervalUnit::DayTime => spec::IntervalUnit::DayTime,
            adt::IntervalUnit::MonthDayNano => spec::IntervalUnit::MonthDayNano,
        }
    }

    pub fn resolve_union_mode(union_mode: &spec::UnionMode) -> adt::UnionMode {
        match union_mode {
            spec::UnionMode::Sparse => adt::UnionMode::Sparse,
            spec::UnionMode::Dense => adt::UnionMode::Dense,
        }
    }

    pub fn unresolve_union_mode(union_mode: &adt::UnionMode) -> spec::UnionMode {
        match union_mode {
            adt::UnionMode::Sparse => spec::UnionMode::Sparse,
            adt::UnionMode::Dense => spec::UnionMode::Dense,
        }
    }

    pub fn resolve_timezone(
        timezone: &spec::TimeZoneInfo,
        config_system_timezone: &str,
        config_timestamp_type: &TimestampType,
    ) -> PlanResult<Option<Arc<str>>> {
        let system_timezone = Some(config_system_timezone.into());
        let resolved_timezone = match timezone {
            spec::TimeZoneInfo::SQLConfigured => match config_timestamp_type {
                // FIXME:
                //  This should be:
                //    1. TimestampType::TimestampLtz => config_session_timezone
                //         - With the timestamp parsed in `sail-sql-analyzer` adjusted accordingly.
                //  The reason we are not doing this is because the tests fail if we do.
                //  The Spark client has inconsistent behavior when applying timezones, where
                //  sometimes the local client timezone is used, while other times the session
                //  timezone is used.
                //  There is also a known bug in Spark that has been unresolved for several years:
                //    - https://github.com/apache/spark/pull/28946
                //  A temporary workaround that leads to the most tests passing is the following:
                //    1. Set timezone to None when parsing timestamps in `sail-sql-analyzer`.
                //    2. When receiving a timestamp from the client (TimeZoneInfo::LocalTimeZone):
                //      - Adjust the timestamp (PlanResolver::local_datetime_to_utc_datetime),
                //      - Set timezone to None (get_adjusted_timezone in `literal.rs`).
                //  More time needs to be spent looking through the Spark codebase to replicate the
                //  exact behavior of the Spark server.
                TimestampType::TimestampLtz => None,
                TimestampType::TimestampNtz => None,
            },
            spec::TimeZoneInfo::LocalTimeZone => system_timezone,
            spec::TimeZoneInfo::NoTimeZone => None,
            spec::TimeZoneInfo::TimeZone { timezone } => match timezone {
                None => None,
                Some(timezone) => {
                    if timezone.is_empty() {
                        None
                    } else {
                        Some(Arc::clone(timezone))
                    }
                }
            },
        };
        Ok(resolved_timezone)
    }

    pub fn unresolve_timezone(timezone: &Option<Arc<str>>) -> PlanResult<spec::TimeZoneInfo> {
        match timezone {
            None => Ok(spec::TimeZoneInfo::NoTimeZone),
            Some(timezone) => Ok(spec::TimeZoneInfo::TimeZone {
                timezone: Some(Arc::clone(timezone)),
            }),
        }
    }
}
