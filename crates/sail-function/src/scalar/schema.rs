use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion_common::{DataFusionError, Result};
use sail_common::spec::{
    self, SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME,
    SAIL_MAP_VALUE_FIELD_NAME,
};
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser as sail_parser;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SchemaFormat {
    Csv,
    Json,
    Xml,
}

impl SchemaFormat {
    fn function_name(self) -> &'static str {
        match self {
            Self::Csv => "from_csv",
            Self::Json => "from_json",
            Self::Xml => "from_xml",
        }
    }

    fn supports_extended_types(self) -> bool {
        !matches!(self, Self::Csv)
    }
}

pub(super) fn parse_schema_data_type(schema: &str, format: SchemaFormat) -> Result<DataType> {
    let schema = schema.trim();
    let ast = sail_parser::parse_data_type(schema)
        .or_else(|_| sail_parser::parse_data_type(&format!("STRUCT<{schema}>")))
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let data_type = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    spec_to_arrow_data_type(&data_type, format)
}

pub(super) fn spec_to_arrow_data_type(
    data_type: &spec::DataType,
    format: SchemaFormat,
) -> Result<DataType> {
    use spec::DataType as SpecDataType;

    fn to_time_unit(unit: &spec::TimeUnit) -> TimeUnit {
        match unit {
            spec::TimeUnit::Second => TimeUnit::Second,
            spec::TimeUnit::Millisecond => TimeUnit::Millisecond,
            spec::TimeUnit::Microsecond => TimeUnit::Microsecond,
            spec::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    let convert = |data_type| spec_to_arrow_data_type(data_type, format);
    match data_type {
        SpecDataType::Null => Ok(DataType::Null),
        SpecDataType::Boolean => Ok(DataType::Boolean),
        SpecDataType::Int8 => Ok(DataType::Int8),
        SpecDataType::Int16 => Ok(DataType::Int16),
        SpecDataType::Int32 => Ok(DataType::Int32),
        SpecDataType::Int64 => Ok(DataType::Int64),
        SpecDataType::UInt8 => Ok(DataType::UInt8),
        SpecDataType::UInt16 => Ok(DataType::UInt16),
        SpecDataType::UInt32 => Ok(DataType::UInt32),
        SpecDataType::UInt64 => Ok(DataType::UInt64),
        SpecDataType::Float16 => Ok(DataType::Float16),
        SpecDataType::Float32 => Ok(DataType::Float32),
        SpecDataType::Float64 => Ok(DataType::Float64),
        SpecDataType::Binary | SpecDataType::ConfiguredBinary => Ok(DataType::Binary),
        SpecDataType::FixedSizeBinary { size } => Ok(DataType::FixedSizeBinary(*size)),
        SpecDataType::LargeBinary => Ok(DataType::LargeBinary),
        SpecDataType::BinaryView => Ok(DataType::BinaryView),
        SpecDataType::Utf8 | SpecDataType::ConfiguredUtf8 { .. } => Ok(DataType::Utf8),
        SpecDataType::LargeUtf8 => Ok(DataType::LargeUtf8),
        SpecDataType::Utf8View => Ok(DataType::Utf8View),
        SpecDataType::Date32 => Ok(DataType::Date32),
        SpecDataType::Date64 => Ok(DataType::Date64),
        SpecDataType::Timestamp {
            time_unit,
            timestamp_type,
        } => Ok(DataType::Timestamp(
            to_time_unit(time_unit),
            match timestamp_type {
                spec::TimestampType::Configured | spec::TimestampType::WithLocalTimeZone => {
                    Some(Arc::from("UTC"))
                }
                spec::TimestampType::WithoutTimeZone => None,
            },
        )),
        SpecDataType::Time32 { time_unit } => Ok(DataType::Time32(to_time_unit(time_unit))),
        SpecDataType::Time64 { time_unit } => Ok(DataType::Time64(to_time_unit(time_unit))),
        SpecDataType::Duration { time_unit } => Ok(DataType::Duration(to_time_unit(time_unit))),
        SpecDataType::Interval { interval_unit, .. } => match interval_unit {
            spec::IntervalUnit::YearMonth => Ok(DataType::Interval(IntervalUnit::YearMonth)),
            spec::IntervalUnit::DayTime if format == SchemaFormat::Csv => {
                Ok(DataType::Interval(IntervalUnit::DayTime))
            }
            spec::IntervalUnit::DayTime => Ok(DataType::Duration(TimeUnit::Microsecond)),
            spec::IntervalUnit::MonthDayNano => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        },
        SpecDataType::Decimal128 { precision, scale } => {
            Ok(DataType::Decimal128(*precision, *scale))
        }
        SpecDataType::Decimal256 { precision, scale } => {
            Ok(DataType::Decimal256(*precision, *scale))
        }
        SpecDataType::List {
            data_type,
            nullable,
        } => Ok(DataType::List(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            convert(data_type)?,
            *nullable,
        )))),
        SpecDataType::FixedSizeList {
            data_type,
            nullable,
            length,
        } => Ok(DataType::FixedSizeList(
            Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                convert(data_type)?,
                *nullable,
            )),
            *length,
        )),
        SpecDataType::LargeList {
            data_type,
            nullable,
        } => Ok(DataType::LargeList(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            convert(data_type)?,
            *nullable,
        )))),
        SpecDataType::Struct { fields } => Ok(DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|field| {
                    Ok(Arc::new(Field::new(
                        field.name.clone(),
                        convert(&field.data_type)?,
                        field.nullable,
                    )))
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        SpecDataType::Map {
            key_type,
            value_type,
            value_type_nullable,
            keys_sorted,
        } => {
            let fields = Fields::from(vec![
                Arc::new(Field::new(
                    SAIL_MAP_KEY_FIELD_NAME,
                    convert(key_type)?,
                    false,
                )),
                Arc::new(Field::new(
                    SAIL_MAP_VALUE_FIELD_NAME,
                    convert(value_type)?,
                    *value_type_nullable,
                )),
            ]);
            Ok(DataType::Map(
                Arc::new(Field::new(
                    SAIL_MAP_FIELD_NAME,
                    DataType::Struct(fields),
                    false,
                )),
                *keys_sorted,
            ))
        }
        SpecDataType::Geometry { .. } | SpecDataType::Geography { .. }
            if format.supports_extended_types() =>
        {
            Ok(DataType::Binary)
        }
        SpecDataType::Variant if format.supports_extended_types() => {
            Ok(DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("metadata", DataType::Binary, false)),
                Arc::new(Field::new("value", DataType::Binary, false)),
            ])))
        }
        SpecDataType::UserDefined { sql_type, .. } if format.supports_extended_types() => {
            convert(sql_type)
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported data type in {} schema: {other:?}",
            format.function_name()
        ))),
    }
}
