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
                Self::resolve_optional_time_unit(time_unit)?,
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
            DataType::Interval(interval_unit) => Ok(adt::DataType::Interval(
                Self::resolve_interval_unit(interval_unit),
            )),
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
            DataType::UserDefined { .. } => Err(PlanError::unsupported(
                "user defined data type should only exist in a field",
            )),
        }
    }

    pub fn unresolve_data_type(data_type: &adt::DataType) -> PlanResult<spec::DataType> {
        use spec::DataType;

        match data_type {
            adt::DataType::Null => Ok(DataType::Null),
            adt::DataType::Boolean => Ok(DataType::Boolean),
            adt::DataType::UInt8 | adt::DataType::Int8 => Ok(DataType::Byte),
            adt::DataType::UInt16 | adt::DataType::Int16 => Ok(DataType::Short),
            adt::DataType::UInt32 | adt::DataType::Int32 => Ok(DataType::Integer),
            adt::DataType::UInt64 | adt::DataType::Int64 => Ok(DataType::Long),
            adt::DataType::Float16 => Err(PlanError::unsupported("float16")),
            adt::DataType::Float32 => Ok(DataType::Float),
            adt::DataType::Float64 => Ok(DataType::Double),
            // TODO: support timestamp precision in data type spec
            adt::DataType::Timestamp(_, None) => Ok(DataType::TimestampNtz),
            adt::DataType::Timestamp(time_unit, Some(timezone)) => Ok(DataType::Timestamp(
                Some(Self::unresolve_time_unit(Some(time_unit))?),
                Some(timezone),
            )),
            adt::DataType::Date32 => Ok(DataType::Date),
            adt::DataType::Date64 => Err(PlanError::unsupported("date64")),
            adt::DataType::Time32(_) => Err(PlanError::unsupported("time32")),
            adt::DataType::Time64(_) => Err(PlanError::unsupported("time64")),
            adt::DataType::Duration(adt::TimeUnit::Microsecond) => Ok(DataType::DayTimeInterval {
                start_field: Some(spec::DayTimeIntervalField::Day),
                end_field: Some(spec::DayTimeIntervalField::Second),
            }),
            adt::DataType::Duration(_) => Err(PlanError::unsupported("duration")),
            adt::DataType::Interval(adt::IntervalUnit::YearMonth) => {
                Ok(DataType::YearMonthInterval {
                    start_field: Some(spec::YearMonthIntervalField::Year),
                    end_field: Some(spec::YearMonthIntervalField::Month),
                })
            }
            adt::DataType::Interval(adt::IntervalUnit::MonthDayNano) => {
                Ok(DataType::CalendarInterval)
            }
            adt::DataType::Interval(adt::IntervalUnit::DayTime) => {
                Ok(DataType::Interval(spec::IntervalUnit::DayTime))
            }
            adt::DataType::Binary
            | adt::DataType::FixedSizeBinary(_)
            | adt::DataType::LargeBinary => Ok(DataType::Binary),
            adt::DataType::Utf8 | adt::DataType::LargeUtf8 => Ok(DataType::String),
            adt::DataType::List(field)
            | adt::DataType::FixedSizeList(field, _)
            | adt::DataType::LargeList(field) => {
                let field = Self::unresolve_field(field.as_ref().clone())?;
                Ok(DataType::Array {
                    element_type: Box::new(field.data_type),
                    contains_null: field.nullable,
                })
            }
            adt::DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| Self::unresolve_field(field.as_ref().clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::Struct {
                    fields: spec::Fields(fields),
                })
            }
            adt::DataType::Union(_, _) => Err(PlanError::unsupported("union")),
            adt::DataType::Dictionary(_, _) => Err(PlanError::unsupported("dictionary")),
            adt::DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128 { precision, scale })
            }
            adt::DataType::Decimal256(precision, scale) => {
                Ok(DataType::Decimal256 { precision, scale })
            }
            adt::DataType::Map(field, _sorted) => {
                let field: spec::Field = Self::unresolve_field(field.as_ref().clone())?;
                let fields = match field.data_type {
                    DataType::Struct { fields } => fields,
                    _ => return Err(PlanError::invalid("invalid map data type")),
                };
                if fields.0.len() != 2 {
                    return Err(PlanError::invalid(
                        "map data type must have key and value fields",
                    ));
                }
                Ok(DataType::Map {
                    key_type: Box::new(fields.0[0].data_type.clone()),
                    value_type: Box::new(fields.0[1].data_type.clone()),
                    value_contains_null: fields.0[1].nullable,
                })
            }
            adt::DataType::RunEndEncoded(_, _) => Err(PlanError::unsupported("run end encoded")),
            adt::DataType::BinaryView => Err(PlanError::unsupported("BinaryView")),
            adt::DataType::Utf8View => Err(PlanError::unsupported("Utf8View")),
            adt::DataType::ListView(_) => Err(PlanError::unsupported("ListView")),
            adt::DataType::LargeListView(_) => Err(PlanError::unsupported("LargeListView")),
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
            .into_iter()
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
