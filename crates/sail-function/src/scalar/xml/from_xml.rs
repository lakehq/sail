use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::*;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use sail_common_datafusion::utils::datetime::{
    SparkTimeZone, localize_with_fallback, parse_spark_timezone,
};
use xee_xpath::Documents;

use crate::functions_utils::make_scalar_function;
use crate::scalar::datetime::format::{DateTimeFormat, ParsedDateTime};
use crate::scalar::options::{find_option, reject_null_options};
use crate::scalar::schema::{SchemaFormat, parse_schema_data_type};

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
    timestamp_ltz_format: DateTimeFormat,
    timestamp_ntz_format: DateTimeFormat,
    date_format: DateTimeFormat,
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
    const TIMESTAMP_LTZ_FORMAT_DEFAULT: &'static str = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    const TIMESTAMP_NTZ_FORMAT_DEFAULT: &'static str = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    const DATE_FORMAT_DEFAULT: &'static str = "yyyy-MM-dd";

    fn from_map(map: &MapArray) -> Result<Self> {
        reject_null_options(map, SparkFromXml::FROM_XML_NAME)?;
        let null_value = find_option(map, Self::NULL_VALUE_OPTION).map(str::to_owned);
        let attribute_prefix = find_option(map, Self::ATTRIBUTE_PREFIX_OPTION)
            .unwrap_or(Self::ATTRIBUTE_PREFIX_DEFAULT)
            .to_owned();
        let value_tag = find_option(map, Self::VALUE_TAG_OPTION)
            .unwrap_or(Self::VALUE_TAG_DEFAULT)
            .to_owned();
        if value_tag.is_empty() {
            return plan_err!("`valueTag` must not be empty");
        }
        if value_tag == attribute_prefix {
            return plan_err!("`valueTag` and `attributePrefix` must not be equal");
        }
        let timestamp_ltz_format = find_option(map, Self::TIMESTAMP_FORMAT_OPTION)
            .map(DateTimeFormat::for_parsing)
            .transpose()?
            .unwrap_or_else(|| {
                #[expect(clippy::expect_used)]
                DateTimeFormat::for_parsing(Self::TIMESTAMP_LTZ_FORMAT_DEFAULT)
                    .expect("default timestamp LTZ format should be valid")
            });
        let timestamp_ntz_format = find_option(map, Self::TIMESTAMP_NTZ_FORMAT_OPTION)
            .map(DateTimeFormat::for_parsing)
            .transpose()?
            .unwrap_or_else(|| {
                #[expect(clippy::expect_used)]
                DateTimeFormat::for_parsing(Self::TIMESTAMP_NTZ_FORMAT_DEFAULT)
                    .expect("default timestamp NTZ format should be valid")
            });
        let date_format = find_option(map, Self::DATE_FORMAT_OPTION)
            .map(DateTimeFormat::for_parsing)
            .transpose()?
            .unwrap_or_else(|| {
                #[expect(clippy::expect_used)]
                DateTimeFormat::for_parsing(Self::DATE_FORMAT_DEFAULT)
                    .expect("default date format should be valid")
            });
        let mode = match find_option(map, Self::MODE_OPTION) {
            None => ParseMode::Permissive,
            Some(mode) if mode.eq_ignore_ascii_case("PERMISSIVE") => ParseMode::Permissive,
            Some(mode) if mode.eq_ignore_ascii_case("FAILFAST") => ParseMode::FailFast,
            Some(other) => {
                return plan_err!("`mode` must be PERMISSIVE or FAILFAST, got '{other}'");
            }
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
            timestamp_ltz_format: {
                #[expect(clippy::expect_used)]
                DateTimeFormat::for_parsing(Self::TIMESTAMP_LTZ_FORMAT_DEFAULT)
                    .expect("default timestamp LTZ format should be valid")
            },
            timestamp_ntz_format: {
                #[expect(clippy::expect_used)]
                DateTimeFormat::for_parsing(Self::TIMESTAMP_NTZ_FORMAT_DEFAULT)
                    .expect("default timestamp NTZ format should be valid")
            },
            date_format: {
                #[expect(clippy::expect_used)]
                DateTimeFormat::for_parsing(Self::DATE_FORMAT_DEFAULT)
                    .expect("default date format should be valid")
            },
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
        let dt = parse_xml_schema(schema_str)?;
        Ok(Arc::new(Field::new(self.name(), dt, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [
                DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            ] => Ok(vec![DataType::Utf8, DataType::Utf8]),
            [
                DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Map(_, _),
            ] => Ok(vec![DataType::Utf8, DataType::Utf8, arg_types[2].clone()]),
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

    let schema_dt = parse_xml_schema(schema_str)?;
    let DataType::Struct(fields) = &schema_dt else {
        return exec_err!(
            "`{}` schema must resolve to a STRUCT type, got {:?}",
            SparkFromXml::FROM_XML_NAME,
            schema_dt
        );
    };

    let mut builder = create_struct_builder(fields, xml_array.len())?;
    let session_timezone = parse_spark_timezone(session_timezone)?;

    for i in 0..xml_array.len() {
        if xml_array.is_null(i) {
            append_null_struct(&mut builder)?;
            continue;
        }

        let xml_str = xml_array.value(i);
        match parse_xml_into_builder(xml_str, &mut builder, &options, &session_timezone) {
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

fn parse_xml_schema(schema: &str) -> Result<DataType> {
    parse_schema_data_type(schema, SchemaFormat::Xml)
}

enum XmlFieldBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal128 {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
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

fn create_field_builder(data_type: &DataType, capacity: usize) -> Result<XmlFieldBuilder> {
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
        DataType::Decimal128(precision, scale) => Ok(XmlFieldBuilder::Decimal128 {
            builder: Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| DataFusionError::Internal(format!("Decimal128 builder error: {e}")))?,
            precision: *precision,
            scale: *scale,
        }),
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
            let values = create_field_builder(item_field.data_type(), capacity)?;
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
                .map(|f| create_field_builder(f.data_type(), capacity))
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

fn create_struct_builder(fields: &Fields, capacity: usize) -> Result<TopLevelStructBuilder> {
    let children = fields
        .iter()
        .map(|f| create_field_builder(f.data_type(), capacity))
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
        XmlFieldBuilder::Decimal128 { builder: b, .. } => b.append_null(),
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
    session_timezone: &SparkTimeZone,
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
    session_timezone: &SparkTimeZone,
) -> Result<()> {
    if field_name == options.value_tag {
        let text = element_text_content(xot, parent);
        return append_text(text.as_deref(), builder, options, session_timezone);
    }

    let attr_name = field_name.strip_prefix(options.attribute_prefix.as_str());
    if let Some(attr_name) = attr_name {
        if let Some(val) = find_attribute(xot, parent, attr_name) {
            return append_text(Some(val.as_str()), builder, options, session_timezone);
        } else if !options.attribute_prefix.is_empty() {
            return append_text(None, builder, options, session_timezone);
        }
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
                for (f, child) in fields.iter().zip(children.iter_mut()) {
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
    session_timezone: &SparkTimeZone,
) -> Result<()> {
    match builder {
        XmlFieldBuilder::Struct {
            fields,
            children,
            nulls,
        } => {
            nulls.push(true);
            for (f, child) in fields.iter().zip(children.iter_mut()) {
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
    session_timezone: &SparkTimeZone,
) -> Result<()> {
    let raw = match text {
        None => return append_null_to_field(builder),
        Some(s) => s,
    };

    if let Some(nv) = &options.null_value
        && raw == nv.as_str()
    {
        return append_null_to_field(builder);
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
        XmlFieldBuilder::Decimal128 {
            builder: b,
            precision,
            scale,
        } => match parse_decimal128(raw, *precision, *scale) {
            Some(v) => b.append_value(v),
            None => b.append_null(),
        },
        XmlFieldBuilder::Float64(b) => match raw.parse::<f64>() {
            Ok(v) => b.append_value(v),
            Err(_) => b.append_null(),
        },

        XmlFieldBuilder::String(b) => b.append_value(raw),
        XmlFieldBuilder::Date32(b) => match options.date_format.parse_date_value(raw) {
            Ok(d) => b.append_value(Date32Type::from_naive_date(d)),
            Err(_) => b.append_null(),
        },
        XmlFieldBuilder::TimestampMicrosecond { builder: b, has_tz } => {
            let fmt = if *has_tz {
                &options.timestamp_ltz_format
            } else {
                &options.timestamp_ntz_format
            };
            match fmt.parse_datetime_value(raw) {
                Ok(parsed) => {
                    let micros = parsed_timestamp_to_micros(parsed, *has_tz, session_timezone)?;
                    b.append_value(micros);
                }
                Err(_) => b.append_null(),
            }
        }
        XmlFieldBuilder::TimestampNanosecond { builder: b, has_tz } => {
            let fmt = if *has_tz {
                &options.timestamp_ltz_format
            } else {
                &options.timestamp_ntz_format
            };
            match fmt.parse_datetime_value(raw) {
                Ok(parsed) => {
                    let micros = parsed_timestamp_to_micros(parsed, *has_tz, session_timezone)?;
                    b.append_value(micros * 1_000);
                }
                Err(_) => b.append_null(),
            }
        }
        XmlFieldBuilder::List { .. } | XmlFieldBuilder::Struct { .. } => {
            append_null_to_field(builder)?;
        }
        XmlFieldBuilder::Unsupported { count, .. } => *count += 1,
    }
    Ok(())
}

fn parsed_timestamp_to_micros(
    parsed: ParsedDateTime,
    has_timezone: bool,
    session_timezone: &SparkTimeZone,
) -> Result<i64> {
    if !has_timezone {
        return Ok(parsed.datetime.and_utc().timestamp_micros());
    }

    if let Some(offset) = parsed.offset {
        return parsed
            .datetime
            .and_local_timezone(offset)
            .single()
            .map(|datetime| datetime.to_utc().timestamp_micros())
            .ok_or_else(|| DataFusionError::Execution("cannot apply parsed offset".to_string()));
    }

    let timezone = parsed
        .timezone
        .as_deref()
        .map(parse_spark_timezone)
        .transpose()?
        .unwrap_or(*session_timezone);
    Ok(localize_with_fallback(&timezone, &parsed.datetime)?.timestamp_micros())
}

fn parse_decimal128(raw: &str, precision: u8, scale: i8) -> Option<i128> {
    let raw = raw.trim();
    let is_negative = raw.starts_with('-');
    let raw = if is_negative { &raw[1..] } else { raw };

    let (int_part, frac_part) = if let Some(pos) = raw.find('.') {
        (&raw[..pos], &raw[pos + 1..])
    } else {
        (raw, "")
    };

    let int_val: i128 = int_part.parse().ok()?;

    let scale = scale as usize;
    let frac_val: i128 = if frac_part.len() >= scale {
        frac_part[..scale].parse().ok()?
    } else {
        let padded = format!("{:0<width$}", frac_part, width = scale);
        padded.parse().ok()?
    };

    let scale_factor: i128 = 10_i128.checked_pow(scale as u32)?;
    let value = int_val.checked_mul(scale_factor)?.checked_add(frac_val)?;
    let value = if is_negative {
        value.checked_neg()?
    } else {
        value
    };
    let max_value = 10_i128.checked_pow(precision as u32)?;
    if value.abs() >= max_value {
        return None; // overflow
    }
    Some(value)
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
    Some(text)
}

fn find_attribute(xot: &xot::Xot, node: xot::Node, attr_name: &str) -> Option<String> {
    for (name_id, value) in xot.attributes(node).iter() {
        if xot.local_name_str(name_id) == attr_name {
            return Some(value.clone());
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
        XmlFieldBuilder::Decimal128 { mut builder, .. } => Ok(Arc::new(builder.finish())),
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
        assert!(
            col(&r, "flag")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        let r = run("<p><flag>false</flag></p>", "flag BOOLEAN")?;
        assert!(
            !col(&r, "flag")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        Ok(())
    }
}
