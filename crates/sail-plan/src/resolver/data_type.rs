use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes as adt;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    fn arrow_binary_type(&self) -> adt::DataType {
        if self.config.arrow_use_large_var_types {
            adt::DataType::LargeBinary
        } else {
            adt::DataType::Binary
        }
    }

    fn arrow_string_type(&self) -> adt::DataType {
        if self.config.arrow_use_large_var_types {
            adt::DataType::LargeUtf8
        } else {
            adt::DataType::Utf8
        }
    }

    /// References:
    ///   org.apache.spark.sql.util.ArrowUtils#toArrowType
    ///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
    pub fn resolve_data_type(&self, data_type: &spec::DataType) -> PlanResult<adt::DataType> {
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
            DataType::Timestamp(time_unit, timezone) => Ok(adt::DataType::Timestamp(
                Self::resolve_time_unit(time_unit)?,
                self.resolve_timezone(timezone)?,
            )),
            DataType::Date32 => Ok(adt::DataType::Date32),
            DataType::Date64 => Ok(adt::DataType::Date64),
            DataType::Time32(time_unit) => {
                Ok(adt::DataType::Time32(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Time64(time_unit) => {
                Ok(adt::DataType::Time64(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Duration(time_unit) => {
                Ok(adt::DataType::Duration(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Interval(interval_unit, _start_field, _end_field) => {
                // TODO: Currently `start_field` and `end_field` is lost in translation.
                //  This does not impact computation accuracy,
                //  This may affect the display string in the `data_type_to_simple_string` function.
                Ok(adt::DataType::Interval(Self::resolve_interval_unit(
                    interval_unit,
                )))
            }
            DataType::Binary => Ok(adt::DataType::Binary),
            DataType::FixedSizeBinary(i32) => Ok(adt::DataType::FixedSizeBinary(*i32)),
            DataType::LargeBinary => Ok(adt::DataType::LargeBinary),
            DataType::BinaryView => Ok(adt::DataType::BinaryView),
            DataType::Utf8 => Ok(adt::DataType::Utf8),
            DataType::LargeUtf8 => Ok(adt::DataType::LargeUtf8),
            DataType::Utf8View => Ok(adt::DataType::Utf8View),
            DataType::List(field) => Ok(adt::DataType::List(Arc::new(self.resolve_field(field)?))),
            DataType::FixedSizeList(field, i32) => Ok(adt::DataType::FixedSizeList(
                Arc::new(self.resolve_field(field)?),
                *i32,
            )),
            DataType::LargeList(field) => Ok(adt::DataType::LargeList(Arc::new(
                self.resolve_field(field)?,
            ))),
            DataType::Struct(fields) => Ok(adt::DataType::Struct(self.resolve_fields(fields)?)),
            DataType::Union(union_fields, union_mode) => {
                let union_fields = union_fields
                    .iter()
                    .map(|(i, field)| Ok((*i, Arc::new(self.resolve_field(field)?))))
                    .collect::<PlanResult<_>>()?;
                Ok(adt::DataType::Union(
                    union_fields,
                    Self::resolve_union_mode(union_mode),
                ))
            }
            DataType::Dictionary(key_type, value_type) => Ok(adt::DataType::Dictionary(
                Box::new(self.resolve_data_type(key_type)?),
                Box::new(self.resolve_data_type(value_type)?),
            )),
            DataType::Decimal128(u8, i8) => Ok(adt::DataType::Decimal128(*u8, *i8)),
            DataType::Decimal256(u8, i8) => Ok(adt::DataType::Decimal256(*u8, *i8)),
            DataType::Map(field, keys_are_sorted) => Ok(adt::DataType::Map(
                Arc::new(self.resolve_field(field)?),
                *keys_are_sorted,
            )),
            DataType::ConfiguredUtf8(_length, _utf8_type) => {
                // FIXME: Currently `length` and `utf8_type` is lost in translation.
                //  This impacts accuracy if `spec::ConfiguredUtf8Type` is `VarChar` or `Char`.
                Ok(self.arrow_string_type())
            }
            DataType::ConfiguredBinary => Ok(self.arrow_binary_type()),
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
            adt::DataType::Timestamp(time_unit, timezone) => Ok(DataType::Timestamp(
                Self::unresolve_time_unit(time_unit)?,
                timezone.clone(),
            )),
            adt::DataType::Date32 => Ok(DataType::Date32),
            adt::DataType::Date64 => Ok(DataType::Date64),
            adt::DataType::Time32(time_unit) => {
                Ok(DataType::Time32(Self::unresolve_time_unit(time_unit)?))
            }
            adt::DataType::Time64(time_unit) => {
                Ok(DataType::Time64(Self::unresolve_time_unit(time_unit)?))
            }
            adt::DataType::Duration(time_unit) => {
                Ok(DataType::Duration(Self::unresolve_time_unit(time_unit)?))
            }
            adt::DataType::Interval(interval_unit) => {
                // TODO: Currently `start_field` and `end_field` is lost in translation.
                //  This does not impact computation accuracy,
                //  This may affect the display string in the `data_type_to_simple_string` function.
                Ok(DataType::Interval(
                    Self::unresolve_interval_unit(interval_unit),
                    None,
                    None,
                ))
            }
            adt::DataType::Binary => Ok(DataType::Binary),
            adt::DataType::FixedSizeBinary(i32) => Ok(DataType::FixedSizeBinary(*i32)),
            adt::DataType::LargeBinary => Ok(DataType::LargeBinary),
            adt::DataType::BinaryView => Ok(DataType::BinaryView),
            adt::DataType::Utf8 => Ok(DataType::Utf8),
            adt::DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            adt::DataType::Utf8View => Ok(DataType::Utf8View),
            adt::DataType::List(field) => {
                Ok(DataType::List(Arc::new(Self::unresolve_field(field)?)))
            }
            adt::DataType::FixedSizeList(field, i32) => Ok(DataType::FixedSizeList(
                Arc::new(Self::unresolve_field(field)?),
                *i32,
            )),
            adt::DataType::LargeList(field) => {
                Ok(DataType::LargeList(Arc::new(Self::unresolve_field(field)?)))
            }
            adt::DataType::Struct(fields) => Ok(DataType::Struct(Self::unresolve_fields(fields)?)),
            adt::DataType::Union(union_fields, union_mode) => {
                let union_fields = union_fields
                    .iter()
                    .map(|(i, field)| Ok((i, Arc::new(Self::unresolve_field(field)?))))
                    .collect::<PlanResult<_>>()?;
                Ok(DataType::Union(
                    union_fields,
                    Self::unresolve_union_mode(union_mode),
                ))
            }
            adt::DataType::Dictionary(key_type, value_type) => Ok(DataType::Dictionary(
                Box::new(Self::unresolve_data_type(key_type)?),
                Box::new(Self::unresolve_data_type(value_type)?),
            )),
            adt::DataType::Decimal128(u8, i8) => Ok(DataType::Decimal128(*u8, *i8)),
            adt::DataType::Decimal256(u8, i8) => Ok(DataType::Decimal256(*u8, *i8)),
            adt::DataType::Map(field, keys_are_sorted) => Ok(DataType::Map(
                Arc::new(Self::unresolve_field(field)?),
                *keys_are_sorted,
            )),
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

    pub fn resolve_field(&self, field: &spec::Field) -> PlanResult<adt::Field> {
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
            adt::Field::new(name, self.resolve_data_type(data_type)?, *nullable)
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

    pub fn resolve_fields(&self, fields: &spec::Fields) -> PlanResult<adt::Fields> {
        let fields = fields
            .into_iter()
            .map(|f| self.resolve_field(f))
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

    pub fn resolve_schema(&self, schema: spec::Schema) -> PlanResult<adt::Schema> {
        let fields = self.resolve_fields(&schema.fields)?;
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

    pub fn resolve_timezone(&self, timezone: &Option<Arc<str>>) -> PlanResult<Option<Arc<str>>> {
        match timezone {
            None => Ok(Some(Arc::<str>::from(self.config.time_zone.as_str()))),
            Some(timezone) => {
                if timezone.is_empty() || timezone.as_ref().to_lowercase().trim() == "ltz" {
                    Ok(Some(Arc::<str>::from(self.config.time_zone.as_str())))
                } else {
                    Ok(Some(Arc::clone(timezone)))
                }
            }
        }
    }
}
