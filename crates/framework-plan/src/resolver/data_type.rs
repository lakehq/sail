use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use arrow::datatypes as adt;
use framework_common::spec;
use std::collections::HashMap;
use std::sync::Arc;

impl PlanResolver<'_> {
    /// References:
    ///   org.apache.spark.sql.util.ArrowUtils#toArrowType
    ///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
    pub fn resolve_data_type(&self, data_type: spec::DataType) -> PlanResult<adt::DataType> {
        use spec::DataType;

        match data_type {
            DataType::Null => Ok(adt::DataType::Null),
            DataType::Boolean => Ok(adt::DataType::Boolean),
            DataType::Byte => Ok(adt::DataType::Int8),
            DataType::Short => Ok(adt::DataType::Int16),
            DataType::Integer => Ok(adt::DataType::Int32),
            DataType::Long => Ok(adt::DataType::Int64),
            DataType::Float => Ok(adt::DataType::Float32),
            DataType::Double => Ok(adt::DataType::Float64),
            DataType::Binary => Ok(adt::DataType::Binary),
            DataType::String => Ok(adt::DataType::Utf8),
            DataType::Array {
                element_type,
                contains_null,
            } => {
                let field = spec::Field {
                    name: "element".to_string(),
                    data_type: *element_type,
                    nullable: contains_null,
                    metadata: HashMap::new(),
                };
                Ok(adt::DataType::List(Arc::new(self.resolve_field(field)?)))
            }
            DataType::Struct { fields } => Ok(adt::DataType::Struct(self.resolve_fields(fields)?)),
            DataType::Decimal { scale, precision } => {
                Ok(adt::DataType::Decimal128(precision, scale))
            }
            DataType::Char { .. } => Ok(adt::DataType::Utf8),
            DataType::VarChar { .. } => Ok(adt::DataType::Utf8),
            DataType::Date => Ok(adt::DataType::Date32),
            DataType::Timestamp => {
                let timezone: Arc<str> = Arc::from(self.config.time_zone.as_str());
                Ok(adt::DataType::Timestamp(
                    adt::TimeUnit::Microsecond,
                    Some(timezone),
                ))
            }
            DataType::TimestampNtz => {
                Ok(adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None))
            }
            DataType::CalendarInterval => {
                Ok(adt::DataType::Interval(adt::IntervalUnit::MonthDayNano))
            }
            DataType::YearMonthInterval { .. } => {
                Ok(adt::DataType::Interval(adt::IntervalUnit::YearMonth))
            }
            DataType::DayTimeInterval { .. } => {
                // Arrow `IntervalUnit::DayTime` only has millisecond precision
                Ok(adt::DataType::Duration(adt::TimeUnit::Microsecond))
            }
            DataType::Map {
                key_type,
                value_type,
                value_contains_null,
            } => {
                let fields = vec![
                    spec::Field {
                        name: "key".to_string(),
                        data_type: *key_type,
                        nullable: false,
                        metadata: HashMap::new(),
                    },
                    spec::Field {
                        name: "value".to_string(),
                        data_type: *value_type,
                        nullable: value_contains_null,
                        metadata: HashMap::new(),
                    },
                ];
                Ok(adt::DataType::Map(
                    Arc::new(adt::Field::new(
                        "entries",
                        adt::DataType::Struct(self.resolve_fields(fields.into())?),
                        false,
                    )),
                    false,
                ))
            }
            DataType::UserDefined { .. } => Err(PlanError::unsupported(
                "user defined data type should only exist in a field",
            )),
        }
    }

    pub fn unresolve_data_type(data_type: adt::DataType) -> PlanResult<spec::DataType> {
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
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None) => {
                Ok(DataType::TimestampNtz)
            }
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, Some(_)) => {
                Ok(DataType::Timestamp)
            }
            adt::DataType::Timestamp(_, _) => Err(PlanError::unsupported("timestamp")),
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
                Err(PlanError::unsupported("interval unit: day-time"))
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
                Ok(DataType::Decimal { precision, scale })
            }
            adt::DataType::Decimal256(_, _) => Err(PlanError::unsupported("decimal256")),
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

    pub fn resolve_field(&self, field: spec::Field) -> PlanResult<adt::Field> {
        let spec::Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let mut metadata: HashMap<_, _> = metadata
            .into_iter()
            .map(|(k, v)| (format!("metadata.{}", k), v))
            .collect();
        let data_type = match data_type {
            spec::DataType::UserDefined {
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => {
                if let Some(jvm_class) = jvm_class {
                    metadata.insert("udt.jvm_class".to_string(), jvm_class);
                }
                if let Some(python_class) = python_class {
                    metadata.insert("udt.python_class".to_string(), python_class);
                }
                if let Some(serialized_python_class) = serialized_python_class {
                    metadata.insert(
                        "udt.serialized_python_class".to_string(),
                        serialized_python_class,
                    );
                }
                *sql_type
            }
            x => x,
        };
        Ok(
            adt::Field::new(name, self.resolve_data_type(data_type)?, nullable)
                .with_metadata(metadata),
        )
    }

    pub fn unresolve_field(field: adt::Field) -> PlanResult<spec::Field> {
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
                sql_type: Box::new(Self::unresolve_data_type(field.data_type().clone())?),
            }
        } else {
            Self::unresolve_data_type(field.data_type().clone())?
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
            metadata,
        })
    }

    pub fn resolve_fields(&self, fields: spec::Fields) -> PlanResult<adt::Fields> {
        let fields = fields
            .0
            .into_iter()
            .map(|f| self.resolve_field(f))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(adt::Fields::from(fields))
    }

    pub fn unresolve_fields(fields: adt::Fields) -> PlanResult<spec::Fields> {
        let fields = fields
            .iter()
            .map(|f| Ok(Self::unresolve_field(f.as_ref().clone())?))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(spec::Fields(fields))
    }

    pub fn resolve_schema(&self, schema: spec::Schema) -> PlanResult<adt::Schema> {
        let fields = schema
            .fields
            .0
            .into_iter()
            .map(|f| self.resolve_field(f))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(adt::Schema::new(fields))
    }

    pub fn unresolve_schema(schema: adt::Schema) -> PlanResult<spec::Schema> {
        let fields = schema
            .fields()
            .iter()
            .map(|f| Self::unresolve_field(f.as_ref().clone()))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(spec::Schema {
            fields: spec::Fields(fields),
        })
    }
}