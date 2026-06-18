use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::*;
use datafusion_common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use sail_common::spec::{
    self, SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME,
    SAIL_MAP_VALUE_FIELD_NAME,
};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser as sail_parser;
use xee_xpath::Documents;

use crate::functions_utils::make_scalar_function;
use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

#[cfg(test)]
const DEFAULT_SESSION_TIMEZONE: &str = "UTC";

/// UDF implementation of `from_xml`, similar to Spark's `XmlToStructs`
/// Parses an XML string column into a struct column using a user-provided schema

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromXml {
    session_timezone: Arc<str>,
    signature: Signature,
}

impl SparkFromXml {
    pub const FROM_XML_NAME: &'static str = "from_xml";

    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseMode {
    Permissive,
    FailFast,
}

#[derive(Debug)]
struct SparkFromXmlOptions {
    null_value: Option<String>,
    attribute_prefix: String,
    value_tag: String,
    timestamp_ltz_format: String,
    timestamp_ntz_format: String,
    date_format: String,
    mode: ParseMode,
}

impl SparkFromXmlOptions {
    const NULL_VALUE_OPTION: &'static str = "nullValue";
    const ATTRIBUTE_PREFIX_OPTION: &'static str = "attributePrefix";
    const ATTRIBUTE_PREFIX_DEFAULT: &'static str = "_";
    const VALUE_TAG_OPTION: &'static str = "valueTag";
    const VALUE_TAG_DEFAULT: &'static str = "_VALUE";
    const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    const TIMESTAMP_NTZ_FORMAT_OPTION: &'static str = "timestampNTZFormat";
    const DATE_FORMAT_OPTION: &'static str = "dateFormat";
    const MODE_OPTION: &'static str = "mode";
    const TIMESTAMP_LTZ_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f";
    const TIMESTAMP_NTZ_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f";
    const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

    fn from_map(map: &MapArray) -> Result<Self> {
        let null_value = find_key_value(map, Self::NULL_VALUE_OPTION);
        let attribute_prefix = find_key_value(map, Self::ATTRIBUTE_PREFIX_OPTION)
            .unwrap_or_else(|| Self::ATTRIBUTE_PREFIX_DEFAULT.to_string());
        let value_tag = find_key_value(map, Self::VALUE_TAG_OPTION)
            .unwrap_or_else(|| Self::VALUE_TAG_DEFAULT.to_string());
        let timestamp_ltz_format = find_key_value(map, Self::TIMESTAMP_FORMAT_OPTION)
            .as_deref()
            .map(spark_datetime_format_to_chrono_strftime)
            .transpose()?
            .unwrap_or_else(|| Self::TIMESTAMP_LTZ_FORMAT_DEFAULT.to_string());
        let timestamp_ntz_format = find_key_value(map, Self::TIMESTAMP_NTZ_FORMAT_OPTION)
            .as_deref()
            .map(spark_datetime_format_to_chrono_strftime)
            .transpose()?
            .unwrap_or_else(|| Self::TIMESTAMP_NTZ_FORMAT_DEFAULT.to_string());
        let date_format = find_key_value(map, Self::DATE_FORMAT_OPTION)
            .as_deref()
            .map(spark_datetime_format_to_chrono_strftime)
            .transpose()?
            .unwrap_or_else(|| Self::DATE_FORMAT_DEFAULT.to_string());
        let mode = match find_key_value(map, Self::MODE_OPTION)
            .as_deref()
            .map(|s| s.to_ascii_uppercase())
            .as_deref()
        {
            Some("FAILFAST") => ParseMode::FailFast,
            _ => ParseMode::Permissive,
        };
        Ok(Self {
            null_value,
            attribute_prefix,
            value_tag,
            timestamp_ltz_format,
            timestamp_ntz_format,
            date_format,
            mode,
        })
    }
}

impl Default for SparkFromXmlOptions {
    fn default() -> Self {
        Self {
            null_value: None,
            attribute_prefix: Self::ATTRIBUTE_PREFIX_DEFAULT.to_string(),
            value_tag: Self::VALUE_TAG_DEFAULT.to_string(),
            timestamp_ltz_format: Self::TIMESTAMP_LTZ_FORMAT_DEFAULT.to_string(),
            timestamp_ntz_format: Self::TIMESTAMP_NTZ_FORMAT_DEFAULT.to_string(),
            date_format: Self::DATE_FORMAT_DEFAULT.to_string(),
            mode: ParseMode::Permissive,
        }
    }
}

impl ScalarUDFImpl for SparkFromXml {
    fn name(&self) -> &str {
        Self::FROM_XML_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let schema_str = match args.scalar_arguments.get(1) {
            Some(Some(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
            )) => s.as_str(),
            _ => {
                return plan_err!(
                    "`{}` requires the schema argument to be a string literal",
                    Self::FROM_XML_NAME
                );
            }
        };
        let dt = parse_xml_schema(schema_str, &self.session_timezone)?;
        Ok(Arc::new(Field::new(self.name(), dt, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Null
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8, DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8] => Ok(vec![DataType::Utf8, arg_types[1].clone()]),
            [DataType::Null
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8, DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8, DataType::Map(_, _)] => Ok(vec![
                DataType::Utf8,
                arg_types[1].clone(),
                arg_types[2].clone(),
            ]),
            _ => plan_err!(
                "`{}` requires 2 or 3 arguments: xml STRING, schema STRING, options MAP (optional), got {:?}",
                Self::FROM_XML_NAME,
                arg_types
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let session_timezone = self.session_timezone.to_string();
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(
            move |inner_args| spark_from_xml_inner(inner_args, session_timezone.as_str()),
            vec![],
        )(&args)
    }
}

fn spark_from_xml_inner(args: &[ArrayRef], session_timezone: &str) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`{}` requires 2 or 3 arguments, got {}",
            SparkFromXml::FROM_XML_NAME,
            args.len()
        );
    }

    let xml_array = args[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("from_xml: expected StringArray for arg 0".to_string())
        })?;

    let schema_array = args[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("from_xml: expected StringArray for arg 1".to_string())
        })?;

    if schema_array.is_empty() || schema_array.is_null(0) {
        return exec_err!(
            "`{}` requires a non-empty schema string",
            SparkFromXml::FROM_XML_NAME
        );
    }
    let schema_str = schema_array.value(0);

    let options = if let Some(opts) = args.get(2) {
        let map = opts.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            DataFusionError::Internal("from_xml: expected MapArray for arg 2".to_string())
        })?;
        SparkFromXmlOptions::from_map(map)?
    } else {
        SparkFromXmlOptions::default()
    };

    let schema_dt = parse_xml_schema(schema_str, session_timezone)?;
    let DataType::Struct(fields) = &schema_dt else {
        return exec_err!(
            "`{}` schema must resolve to a STRUCT type, got {:?}",
            SparkFromXml::FROM_XML_NAME,
            schema_dt
        );
    };

    let mut builder = create_struct_builder(fields, xml_array.len(), session_timezone)?;

    for i in 0..xml_array.len() {
        if xml_array.is_null(i) {
            append_null_struct(&mut builder)?;
            continue;
        }

        let xml_str = xml_array.value(i);
        match parse_xml_into_builder(xml_str, &mut builder, &options, session_timezone) {
            Ok(()) => {}
            Err(e) => {
                if options.mode == ParseMode::FailFast {
                    return Err(e);
                }
                append_null_children_struct(&mut builder)?;
            }
        }
    }

    finish_struct_builder(builder)
}

fn parse_xml_schema(schema: &str, session_timezone: &str) -> Result<DataType> {
    let schema = schema.trim();
    let ast_result = sail_parser::parse_data_type(schema)
        .or_else(|_| sail_parser::parse_data_type(&format!("STRUCT<{schema}>")));
    let ast = ast_result
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    spec_to_arrow_data_type(&spec_dt, session_timezone)
}

fn spec_to_arrow_data_type(dt: &spec::DataType, session_timezone: &str) -> Result<DataType> {
    use spec::DataType as SDT;

    fn to_time_unit(unit: &spec::TimeUnit) -> TimeUnit {
        match unit {
            spec::TimeUnit::Second => TimeUnit::Second,
            spec::TimeUnit::Millisecond => TimeUnit::Millisecond,
            spec::TimeUnit::Microsecond => TimeUnit::Microsecond,
            spec::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    match dt {
        SDT::Null => Ok(DataType::Null),
        SDT::Boolean => Ok(DataType::Boolean),
        SDT::Int8 => Ok(DataType::Int8),
        SDT::Int16 => Ok(DataType::Int16),
        SDT::Int32 => Ok(DataType::Int32),
        SDT::Int64 => Ok(DataType::Int64),
        SDT::UInt8 => Ok(DataType::UInt8),
        SDT::UInt16 => Ok(DataType::UInt16),
        SDT::UInt32 => Ok(DataType::UInt32),
        SDT::UInt64 => Ok(DataType::UInt64),
        SDT::Float16 => Ok(DataType::Float16),
        SDT::Float32 => Ok(DataType::Float32),
        SDT::Float64 => Ok(DataType::Float64),
        SDT::Binary | SDT::ConfiguredBinary => Ok(DataType::Binary),
        SDT::FixedSizeBinary { size } => Ok(DataType::FixedSizeBinary(*size)),
        SDT::LargeBinary => Ok(DataType::LargeBinary),
        SDT::BinaryView => Ok(DataType::BinaryView),
        SDT::Utf8 | SDT::ConfiguredUtf8 { .. } => Ok(DataType::Utf8),
        SDT::LargeUtf8 => Ok(DataType::LargeUtf8),
        SDT::Utf8View => Ok(DataType::Utf8View),
        SDT::Date32 => Ok(DataType::Date32),
        SDT::Date64 => Ok(DataType::Date64),
        SDT::Timestamp {
            time_unit,
            timestamp_type,
        } => Ok(DataType::Timestamp(
            to_time_unit(time_unit),
            match timestamp_type {
                spec::TimestampType::Configured | spec::TimestampType::WithLocalTimeZone => {
                    Some(Arc::from(session_timezone))
                }
                spec::TimestampType::WithoutTimeZone => None,
            },
        )),
        SDT::Time32 { time_unit: u } => Ok(DataType::Time32(to_time_unit(u))),
        SDT::Time64 { time_unit: u } => Ok(DataType::Time64(to_time_unit(u))),
        SDT::Duration { time_unit: u } => Ok(DataType::Duration(to_time_unit(u))),
        SDT::Interval { interval_unit, .. } => match interval_unit {
            spec::IntervalUnit::YearMonth => Ok(DataType::Interval(IntervalUnit::YearMonth)),
            spec::IntervalUnit::DayTime => Ok(DataType::Duration(TimeUnit::Microsecond)),
            spec::IntervalUnit::MonthDayNano => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        },
        SDT::Decimal128 { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        SDT::Decimal256 { precision, scale } => Ok(DataType::Decimal256(*precision, *scale)),
        SDT::List {
            data_type,
            nullable,
        } => Ok(DataType::List(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            spec_to_arrow_data_type(data_type.as_ref(), session_timezone)?,
            *nullable,
        )))),
        SDT::FixedSizeList {
            data_type,
            nullable,
            length,
        } => Ok(DataType::FixedSizeList(
            Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                spec_to_arrow_data_type(data_type.as_ref(), session_timezone)?,
                *nullable,
            )),
            *length,
        )),
        SDT::LargeList {
            data_type,
            nullable,
        } => Ok(DataType::LargeList(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            spec_to_arrow_data_type(data_type.as_ref(), session_timezone)?,
            *nullable,
        )))),
        SDT::Struct { fields } => {
            let mut out: Vec<Arc<Field>> = Vec::with_capacity(fields.len());
            for f in fields.iter() {
                out.push(Arc::new(Field::new(
                    f.name.clone(),
                    spec_to_arrow_data_type(&f.data_type, session_timezone)?,
                    f.nullable,
                )));
            }
            Ok(DataType::Struct(Fields::from(out)))
        }
        SDT::Map {
            key_type,
            value_type,
            value_type_nullable,
            keys_sorted,
        } => {
            let fields = Fields::from(vec![
                Arc::new(Field::new(
                    SAIL_MAP_KEY_FIELD_NAME,
                    spec_to_arrow_data_type(key_type.as_ref(), session_timezone)?,
                    false,
                )),
                Arc::new(Field::new(
                    SAIL_MAP_VALUE_FIELD_NAME,
                    spec_to_arrow_data_type(value_type.as_ref(), session_timezone)?,
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
        SDT::Geometry { .. } | SDT::Geography { .. } => Ok(DataType::Binary),
        SDT::Variant => Ok(DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("metadata", DataType::Binary, false)),
            Arc::new(Field::new("value", DataType::Binary, false)),
        ]))),
        SDT::UserDefined { sql_type, .. } => {
            spec_to_arrow_data_type(sql_type.as_ref(), session_timezone)
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported data type in from_xml schema: {other:?}"
        ))),
    }
}

enum XmlFieldBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Date32(Date32Builder),
    TimestampMicrosecond {
        builder: TimestampMicrosecondBuilder,
        has_tz: bool,
    },
    TimestampNanosecond {
        builder: TimestampNanosecondBuilder,
        has_tz: bool,
    },
    List {
        field: FieldRef,
        offsets: Vec<i32>,
        values: Box<XmlFieldBuilder>,
        nulls: Vec<bool>,
    },
    Struct {
        fields: Fields,
        children: Vec<XmlFieldBuilder>,
        nulls: Vec<bool>,
    },
    Unsupported {
        data_type: DataType,
        count: usize,
    },
}

struct TopLevelStructBuilder {
    fields: Fields,
    children: Vec<XmlFieldBuilder>,
    nulls: Vec<bool>,
}

#[expect(clippy::only_used_in_recursion)]
fn create_field_builder(
    data_type: &DataType,
    capacity: usize,
    session_timezone: &str,
) -> Result<XmlFieldBuilder> {
    match data_type {
        DataType::Boolean => Ok(XmlFieldBuilder::Boolean(BooleanBuilder::with_capacity(
            capacity,
        ))),
        DataType::Int8 => Ok(XmlFieldBuilder::Int8(Int8Builder::with_capacity(capacity))),
        DataType::Int16 => Ok(XmlFieldBuilder::Int16(Int16Builder::with_capacity(
            capacity,
        ))),
        DataType::Int32 => Ok(XmlFieldBuilder::Int32(Int32Builder::with_capacity(
            capacity,
        ))),
        DataType::Int64 => Ok(XmlFieldBuilder::Int64(Int64Builder::with_capacity(
            capacity,
        ))),
        DataType::Float32 => Ok(XmlFieldBuilder::Float32(Float32Builder::with_capacity(
            capacity,
        ))),
        DataType::Float64 => Ok(XmlFieldBuilder::Float64(Float64Builder::with_capacity(
            capacity,
        ))),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(XmlFieldBuilder::String(
            StringBuilder::with_capacity(capacity, capacity * 16),
        )),
        DataType::Date32 => Ok(XmlFieldBuilder::Date32(Date32Builder::with_capacity(
            capacity,
        ))),
        DataType::Timestamp(unit, tz) => match unit {
            TimeUnit::Microsecond => Ok(XmlFieldBuilder::TimestampMicrosecond {
                builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_timezone_opt(tz.clone()),
                has_tz: tz.is_some(),
            }),
            TimeUnit::Nanosecond => Ok(XmlFieldBuilder::TimestampNanosecond {
                builder: TimestampNanosecondBuilder::with_capacity(capacity)
                    .with_timezone_opt(tz.clone()),
                has_tz: tz.is_some(),
            }),
            other => Ok(XmlFieldBuilder::Unsupported {
                data_type: DataType::Timestamp(*other, tz.clone()),
                count: 0,
            }),
        },
        DataType::List(item_field) => {
            let values = create_field_builder(item_field.data_type(), capacity, session_timezone)?;
            let mut offsets = Vec::with_capacity(capacity + 1);
            offsets.push(0_i32);
            Ok(XmlFieldBuilder::List {
                field: item_field.clone(),
                offsets,
                values: Box::new(values),
                nulls: Vec::with_capacity(capacity),
            })
        }
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|f| create_field_builder(f.data_type(), capacity, session_timezone))
                .collect::<Result<Vec<_>>>()?;
            Ok(XmlFieldBuilder::Struct {
                fields: fields.clone(),
                children,
                nulls: Vec::with_capacity(capacity),
            })
        }
        other => Ok(XmlFieldBuilder::Unsupported {
            data_type: other.clone(),
            count: 0,
        }),
    }
}

fn create_struct_builder(
    fields: &Fields,
    capacity: usize,
    session_timezone: &str,
) -> Result<TopLevelStructBuilder> {
    let children = fields
        .iter()
        .map(|f| create_field_builder(f.data_type(), capacity, session_timezone))
        .collect::<Result<Vec<_>>>()?;
    Ok(TopLevelStructBuilder {
        fields: fields.clone(),
        children,
        nulls: Vec::with_capacity(capacity),
    })
}

fn append_null_struct(builder: &mut TopLevelStructBuilder) -> Result<()> {
    builder.nulls.push(false);
    for child in builder.children.iter_mut() {
        append_null_to_field(child)?;
    }
    Ok(())
}

fn append_null_children_struct(builder: &mut TopLevelStructBuilder) -> Result<()> {
    builder.nulls.push(true);
    for child in builder.children.iter_mut() {
        append_null_to_field(child)?;
    }
    Ok(())
}

fn append_null_to_field(builder: &mut XmlFieldBuilder) -> Result<()> {
    match builder {
        XmlFieldBuilder::Boolean(b) => b.append_null(),
        XmlFieldBuilder::Int8(b) => b.append_null(),
        XmlFieldBuilder::Int16(b) => b.append_null(),
        XmlFieldBuilder::Int32(b) => b.append_null(),
        XmlFieldBuilder::Int64(b) => b.append_null(),
        XmlFieldBuilder::Float32(b) => b.append_null(),
        XmlFieldBuilder::Float64(b) => b.append_null(),
        XmlFieldBuilder::String(b) => b.append_null(),
        XmlFieldBuilder::Date32(b) => b.append_null(),
        XmlFieldBuilder::TimestampMicrosecond { builder: b, .. } => b.append_null(),
        XmlFieldBuilder::TimestampNanosecond { builder: b, .. } => b.append_null(),
        XmlFieldBuilder::List { offsets, nulls, .. } => {
            nulls.push(false);
            let curr = offsets.last().copied().unwrap_or(0);
            offsets.push(curr);
        }
        XmlFieldBuilder::Struct {
            children, nulls, ..
        } => {
            nulls.push(false);
            for child in children.iter_mut() {
                append_null_to_field(child)?;
            }
        }
        XmlFieldBuilder::Unsupported { count, .. } => *count += 1,
    }
    Ok(())
}

fn parse_xml_into_builder(
    xml: &str,
    builder: &mut TopLevelStructBuilder,
    options: &SparkFromXmlOptions,
    session_timezone: &str,
) -> Result<()> {
    let mut documents = Documents::new();
    let handle = documents
        .add_string_without_uri(xml)
        .map_err(|e| DataFusionError::Execution(format!("Invalid XML: {e}")))?;

    let doc_node = documents
        .document_node(handle)
        .ok_or_else(|| DataFusionError::Execution("XML document has no root node".to_string()))?;

    let xot = documents.xot();
    let root = xot
        .document_element(doc_node)
        .map_err(|e| DataFusionError::Execution(format!("No root element: {e}")))?;

    builder.nulls.push(true);

    for (field, child_builder) in builder.fields.iter().zip(builder.children.iter_mut()) {
        append_xml_field(
            xot,
            root,
            field.name(),
            child_builder,
            options,
            session_timezone,
        )?;
    }

    Ok(())
}

fn append_xml_field(
    xot: &xot::Xot,
    parent: xot::Node,
    field_name: &str,
    builder: &mut XmlFieldBuilder,
    options: &SparkFromXmlOptions,
    session_timezone: &str,
) -> Result<()> {
    if !options.attribute_prefix.is_empty()
        && field_name.starts_with(options.attribute_prefix.as_str())
    {
        let attr_name = &field_name[options.attribute_prefix.len()..];
        let text = find_attribute(xot, parent, attr_name);
        return append_text(text.as_deref(), builder, options, session_timezone);
    }

    if field_name == options.value_tag {
        let text = element_text_content(xot, parent);
        return append_text(text.as_deref(), builder, options, session_timezone);
    }

    let matches: Vec<xot::Node> = child_elements_named(xot, parent, field_name);

    match builder {
        XmlFieldBuilder::List {
            field,
            offsets,
            values,
            nulls,
        } => {
            if matches.is_empty() {
                nulls.push(false);
                let curr = offsets.last().copied().unwrap_or(0);
                offsets.push(curr);
            } else {
                nulls.push(true);
                let start = offsets.last().copied().unwrap_or(0);
                for &node in &matches {
                    append_element_to_field(xot, node, values, options, session_timezone)?;
                }
                offsets.push(start + matches.len() as i32);
                let _ = field;
            }
        }
        XmlFieldBuilder::Struct {
            fields,
            children,
            nulls,
        } => {
            if matches.is_empty() {
                nulls.push(false);
                for child in children.iter_mut() {
                    append_null_to_field(child)?;
                }
            } else {
                nulls.push(true);
                let node = matches[0];
                for (f, child) in fields.clone().iter().zip(children.iter_mut()) {
                    append_xml_field(xot, node, f.name(), child, options, session_timezone)?;
                }
            }
        }
        _ => {
            if matches.is_empty() {
                append_null_to_field(builder)?;
            } else {
                let text = element_text_content(xot, matches[0]);
                append_text(text.as_deref(), builder, options, session_timezone)?;
            }
        }
    }

    Ok(())
}

fn append_element_to_field(
    xot: &xot::Xot,
    node: xot::Node,
    builder: &mut XmlFieldBuilder,
    options: &SparkFromXmlOptions,
    session_timezone: &str,
) -> Result<()> {
    match builder {
        XmlFieldBuilder::Struct {
            fields,
            children,
            nulls,
        } => {
            nulls.push(true);
            for (f, child) in fields.clone().iter().zip(children.iter_mut()) {
                append_xml_field(xot, node, f.name(), child, options, session_timezone)?;
            }
        }
        _ => {
            let text = element_text_content(xot, node);
            append_text(text.as_deref(), builder, options, session_timezone)?;
        }
    }
    Ok(())
}

fn append_text(
    text: Option<&str>,
    builder: &mut XmlFieldBuilder,
    options: &SparkFromXmlOptions,
    session_timezone: &str,
) -> Result<()> {
    let raw = match text {
        None => return append_null_to_field(builder),
        Some(s) => s,
    };

    if let Some(nv) = &options.null_value {
        if raw == nv.as_str() {
            return append_null_to_field(builder);
        }
    }

    match builder {
        XmlFieldBuilder::Boolean(b) => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" => b.append_value(true),
            "false" | "0" => b.append_value(false),
            _ => b.append_null(),
        },
        XmlFieldBuilder::Int8(b) => match raw.parse::<i8>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::Int16(b) => match raw.parse::<i16>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::Int32(b) => match raw.parse::<i32>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::Int64(b) => match raw.parse::<i64>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::Float32(b) => match raw.parse::<f32>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::Float64(b) => match raw.parse::<f64>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },

        XmlFieldBuilder::String(b) => b.append_value(raw),
        XmlFieldBuilder::Date32(b) => match NaiveDate::parse_from_str(raw, &options.date_format) {
            Ok(d) => b.append_value(Date32Type::from_naive_date(d)),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::TimestampMicrosecond { builder: b, has_tz } => {
            let fmt = if *has_tz {
                &options.timestamp_ltz_format
            } else {
                &options.timestamp_ntz_format
            };
            match parse_naive_datetime(raw, fmt) {
                Some(naive) => {
                    let micros = to_utc_micros(naive, *has_tz, session_timezone)?;
                    b.append_value(micros);
                }
                None => b.append_null(),
            }
        }
        XmlFieldBuilder::TimestampNanosecond { builder: b, has_tz } => {
            let fmt = if *has_tz {
                &options.timestamp_ltz_format
            } else {
                &options.timestamp_ntz_format
            };
            match parse_naive_datetime(raw, fmt) {
                Some(naive) => {
                    let micros = to_utc_micros(naive, *has_tz, session_timezone)?;
                    b.append_value(micros * 1_000);
                }
                None => b.append_null(),
            }
        }
        XmlFieldBuilder::List { .. } | XmlFieldBuilder::Struct { .. } => {
            append_null_to_field(builder)?;
        }
        XmlFieldBuilder::Unsupported { count, .. } => *count += 1,
    }
    Ok(())
}

fn parse_naive_datetime(raw: &str, fmt: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw, fmt).ok().or_else(|| {
        NaiveDate::parse_from_str(raw, fmt)
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
    })
}

fn to_utc_micros(naive: NaiveDateTime, has_tz: bool, session_timezone: &str) -> Result<i64> {
    if has_tz {
        let tz: Tz = session_timezone
            .parse()
            .map_err(|e| DataFusionError::Execution(format!("Invalid timezone: {e}")))?;
        let localized = localize_with_fallback(&tz, &naive)?;
        Ok(localized.timestamp_micros())
    } else {
        Ok(naive.and_utc().timestamp_micros())
    }
}

fn child_elements_named(xot: &xot::Xot, parent: xot::Node, name: &str) -> Vec<xot::Node> {
    xot.children(parent)
        .filter(|&child| {
            if let xot::Value::Element(el) = xot.value(child) {
                xot.local_name_str(el.name()) == name
            } else {
                false
            }
        })
        .collect()
}

fn element_text_content(xot: &xot::Xot, node: xot::Node) -> Option<String> {
    let text: String = xot
        .children(node)
        .filter_map(|child| {
            if let xot::Value::Text(t) = xot.value(child) {
                Some(t.get().to_string())
            } else {
                None
            }
        })
        .collect();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn find_attribute(xot: &xot::Xot, node: xot::Node, attr_name: &str) -> Option<String> {
    for (name_id, value) in xot.attributes(node).iter() {
        if xot.local_name_str(name_id) == attr_name {
            return Some(value.clone());
        }
    }
    None
}

fn find_key_value(map: &MapArray, key: &str) -> Option<String> {
    if map.is_empty() {
        return None;
    }
    let entries = map.value(0);
    let keys = entries
        .column_by_name(SAIL_MAP_KEY_FIELD_NAME)?
        .as_any()
        .downcast_ref::<StringArray>()?;
    let values = entries
        .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)?
        .as_any()
        .downcast_ref::<StringArray>()?;
    for i in 0..keys.len() {
        if !keys.is_null(i) && keys.value(i).eq_ignore_ascii_case(key) {
            return if values.is_null(i) {
                None
            } else {
                Some(values.value(i).to_string())
            };
        }
    }
    None
}

fn finish_field_builder(builder: XmlFieldBuilder) -> Result<ArrayRef> {
    match builder {
        XmlFieldBuilder::Boolean(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Int8(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Int16(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Int32(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Int64(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Float32(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Float64(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::String(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::Date32(mut b) => Ok(Arc::new(b.finish())),
        XmlFieldBuilder::TimestampMicrosecond { mut builder, .. } => Ok(Arc::new(builder.finish())),
        XmlFieldBuilder::TimestampNanosecond { mut builder, .. } => Ok(Arc::new(builder.finish())),
        XmlFieldBuilder::List {
            field,
            offsets,
            values,
            nulls,
        } => {
            let values_array = finish_field_builder(*values)?;
            Ok(Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values_array,
                Some(NullBuffer::from(nulls)),
            )))
        }
        XmlFieldBuilder::Struct {
            fields,
            children,
            nulls,
        } => {
            let arrays = children
                .into_iter()
                .map(finish_field_builder)
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                fields,
                arrays,
                Some(NullBuffer::from(nulls)),
            )))
        }
        XmlFieldBuilder::Unsupported { data_type, count } => Ok(new_null_array(&data_type, count)),
    }
}

fn finish_struct_builder(builder: TopLevelStructBuilder) -> Result<ArrayRef> {
    let arrays = builder
        .children
        .into_iter()
        .map(finish_field_builder)
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::new(
        builder.fields,
        arrays,
        Some(NullBuffer::from(builder.nulls)),
    )))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn run(xml: &str, schema: &str) -> Result<ArrayRef> {
        let xml_arr = Arc::new(StringArray::from(vec![Some(xml)])) as ArrayRef;
        let schema_arr = Arc::new(StringArray::from(vec![schema])) as ArrayRef;
        spark_from_xml_inner(&[xml_arr, schema_arr], DEFAULT_SESSION_TIMEZONE)
    }

    fn col(result: &ArrayRef, name: &str) -> ArrayRef {
        result
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column_by_name(name)
            .unwrap()
            .clone()
    }

    #[test]
    fn test_primitives() -> Result<()> {
        let r = run("<p><a>1</a><b>0.8</b></p>", "a INT, b DOUBLE")?;
        assert_eq!(
            col(&r, "a")
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(
            (col(&r, "b")
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                - 0.8)
                .abs()
                < 1e-9
        );
        Ok(())
    }

    #[test]
    fn test_missing_tag_is_null() -> Result<()> {
        let r = run("<p><b>1</b></p>", "a INT, b INT")?;
        assert!(col(&r, "a").is_null(0));
        assert_eq!(
            col(&r, "b")
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        Ok(())
    }

    #[test]
    fn test_empty_tag_string_is_empty_string() -> Result<()> {
        let r = run("<p><a></a></p>", "a STRING")?;
        assert_eq!(
            col(&r, "a")
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            ""
        );
        Ok(())
    }

    #[test]
    fn test_empty_tag_int_is_null() -> Result<()> {
        let r = run("<p><a></a></p>", "a INT")?;
        assert!(col(&r, "a").is_null(0));
        Ok(())
    }

    #[test]
    fn test_null_input_is_null_struct() -> Result<()> {
        let xml_arr = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
        let schema_arr = Arc::new(StringArray::from(vec!["a INT"])) as ArrayRef;
        let r = spark_from_xml_inner(&[xml_arr, schema_arr], DEFAULT_SESSION_TIMEZONE)?;
        assert!(r.is_null(0));
        Ok(())
    }

    #[test]
    fn test_root_tag_ignored() -> Result<()> {
        for xml in &[
            "<ROW><a>1</a></ROW>",
            "<record><a>1</a></record>",
            "<p><a>1</a></p>",
        ] {
            let r = run(xml, "a INT")?;
            assert_eq!(
                col(&r, "a")
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                1
            );
        }
        Ok(())
    }

    #[test]
    fn test_xml_entities_unescaped() -> Result<()> {
        let r = run("<p><s>a &lt; b</s></p>", "s STRING")?;
        assert_eq!(
            col(&r, "s")
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a < b"
        );
        Ok(())
    }

    #[test]
    fn test_attribute_prefix() -> Result<()> {
        let r = run(
            r#"<p id="99"><name>Alice</name></p>"#,
            "_id STRING, name STRING",
        )?;
        assert_eq!(
            col(&r, "_id")
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "99"
        );
        assert_eq!(
            col(&r, "name")
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Alice"
        );
        Ok(())
    }

    #[test]
    fn test_repeated_tags_array() -> Result<()> {
        let r = run("<p><a>1</a><a>2</a><a>3</a></p>", "a ARRAY<INT>")?;
        let list = col(&r, "a")
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let ints = list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 3);
        assert_eq!(ints.value(0), 1);
        assert_eq!(ints.value(1), 2);
        assert_eq!(ints.value(2), 3);
        Ok(())
    }

    #[test]
    fn test_nested_struct() -> Result<()> {
        let r = run(
            "<p><student><name>Bob</name><rank>1</rank></student></p>",
            "student STRUCT<name: STRING, rank: INT>",
        )?;
        let nested = col(&r, "student")
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        let name = nested.column_by_name("name").unwrap();
        assert_eq!(
            name.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );
        Ok(())
    }

    #[test]
    fn test_boolean() -> Result<()> {
        let r = run("<p><flag>true</flag></p>", "flag BOOLEAN")?;
        assert!(col(&r, "flag")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        let r = run("<p><flag>false</flag></p>", "flag BOOLEAN")?;
        assert!(!col(&r, "flag")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        Ok(())
    }
}
