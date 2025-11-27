use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes as adt;
use sail_common::spec;
use sail_common::spec::{
    SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME,
};

use crate::config::DefaultTimestampType;
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
            .map(|(k, v)| (format!("metadata.{k}"), v.to_string()))
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

    pub fn resolve_interval_unit(interval_unit: &spec::IntervalUnit) -> adt::IntervalUnit {
        match interval_unit {
            spec::IntervalUnit::YearMonth => adt::IntervalUnit::YearMonth,
            spec::IntervalUnit::DayTime => adt::IntervalUnit::DayTime,
            spec::IntervalUnit::MonthDayNano => adt::IntervalUnit::MonthDayNano,
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
