use std::fmt::Write as _;
use std::sync::Arc;

use chrono::prelude::*;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

/// Spark-compatible `to_xml` UDF. Serializes a StructArray into XML strings.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToXml {
    session_timezone: Arc<str>,
    signature: Signature,
}

#[derive(Debug)]
struct SparkToXmlOptions {
    row_tag: String,
    array_element_name: String,
    attribute_prefix: String,
    value_tag: String,
    /// If Some(s), nulls are emitted as <field>s</field>. If None, they are omitted.
    null_value: Option<String>,
    timestamp_ltz_format: Option<String>,
    timestamp_ntz_format: String,
    date_format: String,
    session_timezone: String,
}

impl SparkToXmlOptions {
    pub const ROW_TAG_OPTION: &'static str = "rowTag";
    pub const ROW_TAG_DEFAULT: &'static str = "ROW";
    pub const ARRAY_ELEMENT_NAME_OPTION: &'static str = "arrayElementName";
    pub const ARRAY_ELEMENT_NAME_DEFAULT: &'static str = "item";
    pub const ATTRIBUTE_PREFIX_OPTION: &'static str = "attributePrefix";
    pub const ATTRIBUTE_PREFIX_DEFAULT: &'static str = "_";
    pub const VALUE_TAG_OPTION: &'static str = "valueTag";
    pub const VALUE_TAG_DEFAULT: &'static str = "_VALUE";
    pub const NULL_VALUE_OPTION: &'static str = "nullValue";
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    pub const TIMESTAMP_NTZ_FORMAT_OPTION: &'static str = "timestampNTZFormat";
    pub const DATE_FORMAT_OPTION: &'static str = "dateFormat";

    // Standard Spark strftime defaults
    pub const TIMESTAMP_LTZ_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f%:z";
    pub const TIMESTAMP_NTZ_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f";
    pub const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

    fn from_map(map: &MapArray, session_timezone: &str) -> Result<Self> {
        let row_tag = find_key_value(map, Self::ROW_TAG_OPTION)
            .unwrap_or_else(|| Self::ROW_TAG_DEFAULT.to_string());
        if row_tag.is_empty() {
            return plan_err!("`rowTag` option must not be empty");
        }
        if row_tag.starts_with('<') || row_tag.ends_with('>') {
            return plan_err!("`rowTag` must not include angle brackets");
        }

        let attribute_prefix = find_key_value(map, Self::ATTRIBUTE_PREFIX_OPTION)
            .unwrap_or_else(|| Self::ATTRIBUTE_PREFIX_DEFAULT.to_string());
        if attribute_prefix.is_empty() {
            return plan_err!("`attributePrefix` must not be empty when writing XML");
        }

        let value_tag = find_key_value(map, Self::VALUE_TAG_OPTION)
            .unwrap_or_else(|| Self::VALUE_TAG_DEFAULT.to_string());
        if value_tag.is_empty() {
            return plan_err!("`valueTag` must not be empty");
        }
        if value_tag == attribute_prefix {
            return plan_err!("`valueTag` and `attributePrefix` must not be equal");
        }

        let null_value = find_key_value(map, Self::NULL_VALUE_OPTION);

        let timestamp_ltz_format = find_key_value(map, Self::TIMESTAMP_FORMAT_OPTION)
            .as_deref()
            .map(spark_datetime_format_to_chrono_strftime)
            .transpose()?;

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

        Ok(Self {
            row_tag,
            array_element_name: find_key_value(map, Self::ARRAY_ELEMENT_NAME_OPTION)
                .unwrap_or_else(|| Self::ARRAY_ELEMENT_NAME_DEFAULT.to_string()),
            attribute_prefix,
            value_tag,
            null_value,
            timestamp_ltz_format,
            timestamp_ntz_format,
            date_format,
            session_timezone: session_timezone.to_string(),
        })
    }

    #[inline]
    fn is_attribute(&self, name: &str) -> bool {
        name.starts_with(self.attribute_prefix.as_str()) && name != self.value_tag
    }

    #[inline]
    fn strip_prefix<'a>(&self, name: &'a str) -> &'a str {
        name.strip_prefix(self.attribute_prefix.as_str())
            .unwrap_or(name)
    }
}

impl Default for SparkToXmlOptions {
    fn default() -> Self {
        Self {
            row_tag: Self::ROW_TAG_DEFAULT.to_string(),
            array_element_name: Self::ARRAY_ELEMENT_NAME_DEFAULT.to_string(),
            attribute_prefix: Self::ATTRIBUTE_PREFIX_DEFAULT.to_string(),
            value_tag: Self::VALUE_TAG_DEFAULT.to_string(),
            null_value: None,
            timestamp_ltz_format: None,
            timestamp_ntz_format: Self::TIMESTAMP_NTZ_FORMAT_DEFAULT.to_string(),
            date_format: Self::DATE_FORMAT_DEFAULT.to_string(),
            session_timezone: "UTC".to_string(),
        }
    }
}

impl SparkToXml {
    pub const TO_XML_NAME: &'static str = "to_xml";

    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    fn column_name(args: &[datafusion_expr::Expr]) -> String {
        let Some(input) = args.first() else {
            return format!("{}()", Self::TO_XML_NAME);
        };
        format!("{}({})", Self::TO_XML_NAME, input.schema_name())
    }
}

impl Default for SparkToXml {
    fn default() -> Self {
        Self::new(Arc::from("UTC"))
    }
}

impl ScalarUDFImpl for SparkToXml {
    fn name(&self) -> &str {
        Self::TO_XML_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn schema_name(&self, args: &[datafusion_expr::Expr]) -> Result<String> {
        Ok(Self::column_name(args))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            1 => match &arg_types[0] {
                DataType::Struct(_) => Ok(arg_types.to_vec()),
                other => plan_err!("`to_xml` first argument must be a struct, got {other}"),
            },
            2 => {
                match &arg_types[0] {
                    DataType::Struct(_) => {}
                    other => {
                        return plan_err!("`to_xml` first argument must be a struct, got {other}");
                    }
                }
                match &arg_types[1] {
                    DataType::Map(_, _) | DataType::Null => {}
                    other => {
                        return plan_err!(
                            "`to_xml` second argument must be a map of options, got {other}"
                        );
                    }
                }
                Ok(arg_types.to_vec())
            }
            n => plan_err!("`to_xml` takes 1 or 2 arguments, got {n}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let session_timezone = self.session_timezone.as_ref();

        let options = if args.len() == 2 {
            match &args[1] {
                ColumnarValue::Scalar(s) if s.is_null() => SparkToXmlOptions {
                    session_timezone: session_timezone.to_string(),
                    ..SparkToXmlOptions::default()
                },
                ColumnarValue::Array(arr) if matches!(arr.data_type(), DataType::Null) => {
                    SparkToXmlOptions {
                        session_timezone: session_timezone.to_string(),
                        ..SparkToXmlOptions::default()
                    }
                }
                _ => {
                    let map_array = to_map_array(&args[1])?;
                    SparkToXmlOptions::from_map(&map_array, session_timezone)?
                }
            }
        } else {
            SparkToXmlOptions {
                session_timezone: session_timezone.to_string(),
                ..SparkToXmlOptions::default()
            }
        };

        let struct_array = to_struct_array(&args[0])?;
        let result = spark_to_xml_inner(&struct_array, &options)?;
        Ok(ColumnarValue::Array(result))
    }
}

fn spark_to_xml_inner(array: &StructArray, options: &SparkToXmlOptions) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    // One reused buffer across rows: `clear()` keeps its capacity, so the row
    // strings are not re-allocated per row and are copied into the Arrow buffer
    // exactly once.
    let mut buf = String::with_capacity(256);

    for row in 0..array.len() {
        if array.is_null(row) {
            builder.append_null();
        } else {
            buf.clear();
            write_struct(
                &mut buf,
                array,
                row,
                array.fields(),
                &options.row_tag,
                0,
                options,
            )?;
            // Every writer prefixes its own leading newline; the root element has none.
            builder.append_value(buf.strip_prefix('\n').unwrap_or(&buf));
        }
    }

    Ok(Arc::new(builder.finish()))
}

const INDENT: &str = "    ";

/// Appends `depth` levels of indentation without allocating (unlike
/// `INDENT.repeat(depth)`, which builds a fresh `String` each call).
#[inline]
fn push_indent(buf: &mut String, depth: usize) {
    for _ in 0..depth {
        buf.push_str(INDENT);
    }
}

/// Serializes a struct as `<tag …attrs…>…children…</tag>`, matching Spark's
/// `StaxXmlGenerator`. Uses a leading-newline convention: every element/array/map
/// child prefixes its own `\n` plus indentation, so no trailing newline is ever
/// appended. Attributes go on the open tag; the `valueTag` field becomes inline
/// text right after `>`; remaining fields become child elements.
fn write_struct(
    buf: &mut String,
    array: &StructArray,
    row: usize,
    fields: &Fields,
    tag: &str,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    buf.push('\n');
    push_indent(buf, depth);
    buf.push('<');
    buf.push_str(tag);

    // First pass: attributes go on the open tag.
    for (col_idx, field) in fields.iter().enumerate() {
        if !options.is_attribute(field.name()) {
            continue;
        }
        let col = array.column(col_idx);
        if col.is_null(row) {
            if let Some(ref nv) = options.null_value {
                let attr_name = options.strip_prefix(field.name());
                write!(buf, r#" {}="{}""#, attr_name, escape_attr(nv))?;
            }
        } else {
            let attr_name = options.strip_prefix(field.name());
            let value = format_field_to_xml(col, row, field.data_type(), options)?;
            write!(buf, r#" {}="{}""#, attr_name, escape_attr(&value))?;
        }
    }

    // Second pass: the valueTag field becomes inline text, the rest child elements.
    let mut value_text: Option<String> = None;
    let mut children = String::new();
    for (col_idx, field) in fields.iter().enumerate() {
        if options.is_attribute(field.name()) {
            continue;
        }
        let col = array.column(col_idx);
        if field.name() == options.value_tag.as_str() {
            if col.is_null(row) {
                if let Some(ref nv) = options.null_value {
                    value_text = Some(escape_text(nv));
                }
            } else {
                value_text = Some(escape_text(&format_field_to_xml(
                    col,
                    row,
                    field.data_type(),
                    options,
                )?));
            }
        } else {
            write_field(&mut children, col, row, field, depth + 1, options)?;
        }
    }

    let has_text = value_text.is_some();
    let has_children = !children.is_empty();
    if !has_text && !has_children {
        buf.push_str("/>");
    } else {
        buf.push('>');
        if let Some(text) = value_text {
            buf.push_str(&text);
        }
        buf.push_str(&children);
        if has_children {
            buf.push('\n');
            push_indent(buf, depth);
        }
        buf.push_str("</");
        buf.push_str(tag);
        buf.push('>');
    }

    Ok(())
}

fn write_field(
    buf: &mut String,
    col: &ArrayRef,
    row: usize,
    field: &Field,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    let name = field.name().as_str();

    if col.is_null(row) {
        if let Some(ref nv) = options.null_value {
            buf.push('\n');
            push_indent(buf, depth);
            push_element(buf, name, &escape_text(nv));
        }
        return Ok(());
    }

    match field.data_type() {
        DataType::Struct(child_fields) => {
            let child = col.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "to_xml: expected StructArray for field '{name}'"
                ))
            })?;
            write_struct(buf, child, row, child_fields, name, depth, options)?;
        }
        DataType::List(item_field) | DataType::LargeList(item_field) => {
            write_array(buf, col, row, name, item_field, depth, options)?;
        }
        DataType::Map(_, _) => {
            write_map(buf, col, row, name, depth, options)?;
        }
        _ => {
            let text = format_field_to_xml(col, row, field.data_type(), options)?;
            buf.push('\n');
            push_indent(buf, depth);
            push_element(buf, name, &escape_text(&text));
        }
    }

    Ok(())
}

/// Serializes an array field. Spark repeats the field tag once per element for
/// primitives (`<f>1</f><f>2</f>`) and structs, with no `item` wrapper. Only a
/// nested array wraps its inner elements, which then use `arrayElementName`.
fn write_array(
    buf: &mut String,
    col: &ArrayRef,
    row: usize,
    field_name: &str,
    item_field: &Field,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    let (offsets_start, offsets_end, values): (usize, usize, ArrayRef) =
        if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
            (
                list.offsets()[row] as usize,
                list.offsets()[row + 1] as usize,
                list.values().clone(),
            )
        } else if let Some(list) = col.as_any().downcast_ref::<LargeListArray>() {
            (
                list.offsets()[row] as usize,
                list.offsets()[row + 1] as usize,
                list.values().clone(),
            )
        } else {
            return exec_err!("to_xml: expected ListArray for field '{field_name}'");
        };

    for item_idx in offsets_start..offsets_end {
        match item_field.data_type() {
            DataType::Struct(child_fields) => {
                let child = values
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "to_xml: expected StructArray inside list '{field_name}'"
                        ))
                    })?;
                write_struct(
                    buf,
                    child,
                    item_idx,
                    child_fields,
                    field_name,
                    depth,
                    options,
                )?;
            }
            DataType::List(inner_field) | DataType::LargeList(inner_field) => {
                buf.push('\n');
                push_indent(buf, depth);
                buf.push('<');
                buf.push_str(field_name);
                buf.push('>');
                write_array(
                    buf,
                    &values,
                    item_idx,
                    options.array_element_name.as_str(),
                    inner_field,
                    depth + 1,
                    options,
                )?;
                buf.push('\n');
                push_indent(buf, depth);
                buf.push_str("</");
                buf.push_str(field_name);
                buf.push('>');
            }
            _ => {
                if values.is_null(item_idx) {
                    if let Some(ref nv) = options.null_value {
                        buf.push('\n');
                        push_indent(buf, depth);
                        push_element(buf, field_name, &escape_text(nv));
                    }
                } else {
                    let text =
                        format_field_to_xml(&values, item_idx, item_field.data_type(), options)?;
                    buf.push('\n');
                    push_indent(buf, depth);
                    push_element(buf, field_name, &escape_text(&text));
                }
            }
        }
    }

    Ok(())
}

fn write_map(
    buf: &mut String,
    col: &ArrayRef,
    row: usize,
    field_name: &str,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    let map_array = col.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "to_xml: expected MapArray for field '{field_name}'"
        ))
    })?;

    let entries = map_array.value(row);
    let num_entries = entries.len();
    let keys = entries
        .column_by_name(SAIL_MAP_KEY_FIELD_NAME)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "to_xml: map field '{field_name}' missing key column"
            ))
        })?;
    let values = entries
        .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "to_xml: map field '{field_name}' missing value column"
            ))
        })?;

    let mut inner = String::new();
    for i in 0..num_entries {
        if keys.is_null(i) {
            return exec_err!(
                "to_xml: map field '{field_name}' has a null key, \
                 which cannot be used as an XML tag name"
            );
        }
        let key_scalar = ScalarValue::try_from_array(keys, i)?;
        let key = match key_scalar {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => s,
            other => {
                return exec_err!(
                    "to_xml: map keys must be strings to be valid XML tag names, got {other:?}"
                );
            }
        };
        if key.is_empty() {
            return exec_err!(
                "to_xml: map field '{field_name}' has an empty key, \
                 which is not a valid XML tag name"
            );
        }
        let first = key.chars().next().unwrap_or('\0');
        if first.is_ascii_digit() || first == '-' || first == '.' {
            return exec_err!(
                "to_xml: map key '{key}' starts with '{first}', \
                 which is not a valid XML name start character"
            );
        }
        write_map_entry(&mut inner, values, i, &key, depth + 1, options)?;
    }

    buf.push('\n');
    push_indent(buf, depth);
    buf.push('<');
    buf.push_str(field_name);
    if inner.is_empty() {
        buf.push_str("/>");
    } else {
        buf.push('>');
        buf.push_str(&inner);
        buf.push('\n');
        push_indent(buf, depth);
        buf.push_str("</");
        buf.push_str(field_name);
        buf.push('>');
    }

    Ok(())
}

/// Renders a single map value under tag `key`, prefixing its own newline plus
/// indentation. A list value repeats `key` once per element (like a top-level
/// primitive array).
fn write_map_entry(
    buf: &mut String,
    values: &ArrayRef,
    i: usize,
    key: &str,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    if values.is_null(i) {
        if let Some(ref nv) = options.null_value {
            buf.push('\n');
            push_indent(buf, depth);
            push_element(buf, key, &escape_text(nv));
        }
        return Ok(());
    }

    match values.data_type() {
        DataType::Struct(child_fields) => {
            let child = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "to_xml: expected StructArray for map value '{key}'"
                    ))
                })?;
            write_struct(buf, child, i, child_fields, key, depth, options)?;
        }
        DataType::List(item_field) | DataType::LargeList(item_field) => {
            let (offsets_start, offsets_end, array_values): (usize, usize, ArrayRef) =
                if let Some(list) = values.as_any().downcast_ref::<ListArray>() {
                    (
                        list.offsets()[i] as usize,
                        list.offsets()[i + 1] as usize,
                        list.values().clone(),
                    )
                } else if let Some(list) = values.as_any().downcast_ref::<LargeListArray>() {
                    (
                        list.offsets()[i] as usize,
                        list.offsets()[i + 1] as usize,
                        list.values().clone(),
                    )
                } else {
                    return exec_err!("to_xml: expected ListArray for map value '{key}'");
                };
            for elem_idx in offsets_start..offsets_end {
                if array_values.is_null(elem_idx) {
                    if let Some(ref nv) = options.null_value {
                        buf.push('\n');
                        push_indent(buf, depth);
                        push_element(buf, key, &escape_text(nv));
                    }
                } else if let DataType::Struct(child_fields) = item_field.data_type() {
                    let child = array_values
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "to_xml: expected StructArray in map array value '{key}'"
                            ))
                        })?;
                    write_struct(buf, child, elem_idx, child_fields, key, depth, options)?;
                } else {
                    let text = format_field_to_xml(
                        &array_values,
                        elem_idx,
                        item_field.data_type(),
                        options,
                    )?;
                    buf.push('\n');
                    push_indent(buf, depth);
                    push_element(buf, key, &escape_text(&text));
                }
            }
        }
        val_dt => {
            let text = format_field_to_xml(values, i, val_dt, options)?;
            buf.push('\n');
            push_indent(buf, depth);
            push_element(buf, key, &escape_text(&text));
        }
    }

    Ok(())
}

fn format_field_to_xml(
    array: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
    options: &SparkToXmlOptions,
) -> Result<String> {
    match data_type {
        DataType::Timestamp(time_unit, tz_opt) => {
            format_timestamp_field(array, row_idx, time_unit, tz_opt, options)
        }
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row_idx);
            // 719_163 is the epoch offset for chrono NaiveDate
            let naive =
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163).ok_or_else(|| {
                    DataFusionError::Execution(format!("Date32 value out of range: {days}"))
                })?;
            Ok(naive.format(&options.date_format).to_string())
        }
        DataType::Date64 => {
            let millis = array.as_primitive::<Date64Type>().value(row_idx);
            let naive = DateTime::from_timestamp(millis.div_euclid(1_000), 0)
                .map(|dt| dt.date_naive())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Date64 value out of range: {millis}"))
                })?;
            Ok(naive.format(&options.date_format).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let raw = array.as_primitive::<Decimal128Type>().value(row_idx);
            Ok(format_decimal128(raw, *scale as u32))
        }
        _ => {
            let scalar = ScalarValue::try_from_array(array, row_idx)?;
            scalar_to_display_string(&scalar)
        }
    }
}

fn format_decimal128(raw: i128, scale: u32) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    let negative = raw < 0;
    let raw_abs = raw.unsigned_abs();
    let divisor = 10u128.pow(scale.min(38));
    let integer_part = raw_abs / divisor;
    let fractional_part = raw_abs % divisor;
    let sign = if negative { "-" } else { "" };
    format!(
        "{sign}{integer_part}.{fractional_part:0>width$}",
        width = scale as usize
    )
}

/// Formats a `f64` the way Java's `Double.toString` (and therefore Spark) does:
/// plain decimal for `1e-3 <= |x| < 1e7`, scientific `d.dddEexp` otherwise,
/// always with at least one fractional digit.
fn format_double(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if v == 0.0 {
        return "0.0".to_string();
    }
    // Rust's `{:e}` yields the shortest round-tripping mantissa in normalized
    // form `d[.ddd]e<exp>`, which carries exactly the significant digits and the
    // base-10 exponent Java needs.
    java_float_repr(v < 0.0, &format!("{:e}", v.abs()))
}

/// Same as [`format_double`] but for `f32` (Spark `FLOAT`). Formatting the `f32`
/// directly (not via `f64`) keeps the shortest `f32` round-tripping digits.
fn format_single(v: f32) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if v == 0.0 {
        return "0.0".to_string();
    }
    java_float_repr(v < 0.0, &format!("{:e}", v.abs()))
}

/// Reformats Rust's normalized `{:e}` output (e.g. `"1.2345678e7"`) into Java
/// `Double.toString` style. `neg` carries the sign separately.
fn java_float_repr(neg: bool, rust_sci: &str) -> String {
    let Some((mantissa, exp_str)) = rust_sci.split_once('e') else {
        return rust_sci.to_string();
    };
    let Ok(sci_exp) = exp_str.parse::<i32>() else {
        return rust_sci.to_string();
    };
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let sign = if neg { "-" } else { "" };

    if (-3..7).contains(&sci_exp) {
        // Plain decimal notation.
        let body = if sci_exp >= 0 {
            let int_len = sci_exp as usize + 1;
            if int_len >= digits.len() {
                format!("{}{}.0", digits, "0".repeat(int_len - digits.len()))
            } else {
                format!("{}.{}", &digits[..int_len], &digits[int_len..])
            }
        } else {
            format!("0.{}{}", "0".repeat((-sci_exp - 1) as usize), digits)
        };
        format!("{sign}{body}")
    } else {
        // Scientific notation: one digit before the point, at least one after.
        let mantissa = if digits.len() == 1 {
            format!("{}.0", digits)
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        format!("{sign}{mantissa}E{sci_exp}")
    }
}

fn scalar_to_display_string(scalar: &ScalarValue) -> Result<String> {
    match scalar {
        ScalarValue::Boolean(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int8(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int16(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int32(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int64(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Ok(v.to_string()),
        ScalarValue::Float32(Some(v)) => Ok(format_single(*v)),
        ScalarValue::Float64(Some(v)) => Ok(format_double(*v)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Ok(v.clone()),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v)) => Ok(format_binary(v)),
        sv if sv.is_null() => exec_err!("to_xml: scalar_to_display_string called on null: {sv:?}"),
        _ => exec_err!("to_xml: unsupported scalar type for XML serialization: {scalar:?}"),
    }
}

/// Spark renders binary as space-separated uppercase hex bytes in brackets,
/// e.g. `X'48656C6C6F'` becomes `[48 65 6C 6C 6F]` and empty binary becomes `[]`.
fn format_binary(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len() * 3 + 2);
    output.push('[');
    for (i, byte) in value.iter().enumerate() {
        if i > 0 {
            output.push(' ');
        }
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output.push(']');
    output
}

fn format_timestamp_field(
    array: &ArrayRef,
    row_idx: usize,
    time_unit: &TimeUnit,
    tz_opt: &Option<Arc<str>>,
    options: &SparkToXmlOptions,
) -> Result<String> {
    let micros = match time_unit {
        TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row_idx) * 1_000_000,
        TimeUnit::Millisecond => {
            array
                .as_primitive::<TimestampMillisecondType>()
                .value(row_idx)
                * 1_000
        }
        TimeUnit::Microsecond => array
            .as_primitive::<TimestampMicrosecondType>()
            .value(row_idx),
        TimeUnit::Nanosecond => array
            .as_primitive::<TimestampNanosecondType>()
            .value(row_idx)
            .div_euclid(1_000),
    };

    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    let is_default_format = options.timestamp_ltz_format.is_none();

    if tz_opt.is_some() {
        let tz: Tz = options.session_timezone.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Invalid session timezone '{}': {e}",
                options.session_timezone
            ))
        })?;
        let utc_dt = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
            DataFusionError::Execution(format!("Timestamp out of range: {micros}"))
        })?;
        let local_dt = utc_dt.with_timezone(&tz);
        if is_default_format {
            // Default Spark: ISO 8601 with Z suffix for UTC
            Ok(local_dt
                .format(SparkToXmlOptions::TIMESTAMP_LTZ_FORMAT_DEFAULT)
                .to_string()
                .replace("+00:00", "Z"))
        } else {
            let fmt = options.timestamp_ltz_format.as_deref().ok_or_else(|| {
                DataFusionError::Internal(
                    "timestamp_ltz_format missing despite non-default path".to_string(),
                )
            })?;

            Ok(local_dt.format(fmt).to_string())
        }
    } else {
        let naive = DateTime::from_timestamp(secs, nanos)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Timestamp out of range: {micros}"))
            })?;
        Ok(naive.format(&options.timestamp_ntz_format).to_string())
    }
}

/// Spark's `StaxXmlGenerator` escapes only `&` and `<` in element text; `>` is
/// left literal.
fn escape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            _ => out.push(c),
        }
    }
    out
}

/// In attribute values Spark escapes `&`, `<` and `"`; `>` and `'` are left
/// literal.
fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

fn push_element(buf: &mut String, name: &str, content: &str) {
    buf.push('<');
    buf.push_str(name);
    buf.push('>');
    buf.push_str(content);
    buf.push_str("</");
    buf.push_str(name);
    buf.push('>');
}

fn to_struct_array(col: &ColumnarValue) -> Result<StructArray> {
    let array = match col {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };
    array
        .as_any()
        .downcast_ref::<StructArray>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal("to_xml: expected StructArray as first argument".to_string())
        })
}

fn to_map_array(col: &ColumnarValue) -> Result<MapArray> {
    let array = match col {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };
    array
        .as_any()
        .downcast_ref::<MapArray>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Plan("to_xml: second argument must be a map literal".to_string())
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_text() {
        // Spark escapes only `&` and `<`; `>` stays literal.
        assert_eq!(escape_text("a & b < c > d"), "a &amp; b &lt; c > d");
        assert_eq!(escape_text("clean"), "clean");
    }

    #[test]
    fn test_escape_attr() {
        // Spark escapes `&`, `<` and `"` in attributes; `>` and `'` stay literal.
        assert_eq!(escape_attr(r#"say "hi""#), "say &quot;hi&quot;");
        assert_eq!(escape_attr("it's > that"), "it's > that");
    }

    #[test]
    fn test_format_binary() {
        assert_eq!(
            format_binary(&[0x48, 0x65, 0x6C, 0x6C, 0x6F]),
            "[48 65 6C 6C 6F]"
        );
        assert_eq!(format_binary(&[]), "[]");
        assert_eq!(format_binary(&[0x00, 0xFF]), "[00 FF]");
    }

    #[test]
    fn test_format_decimal128() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(format_decimal128(25, 1), "2.5");
        assert_eq!(format_decimal128(-99, 2), "-0.99");
        assert_eq!(format_decimal128(100, 2), "1.00");
        assert_eq!(format_decimal128(0, 2), "0.00");
        assert_eq!(format_decimal128(999999, 2), "9999.99");
        assert_eq!(format_decimal128(42, 0), "42");
        Ok(())
    }

    #[test]
    fn test_format_double() {
        // Values pinned against Java `Double.toString` (Spark JVM ground truth).
        assert_eq!(format_double(f64::NAN), "NaN");
        assert_eq!(format_double(f64::INFINITY), "Infinity");
        assert_eq!(format_double(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(format_double(0.0), "0.0");
        assert_eq!(format_double(-0.0), "0.0");
        assert_eq!(format_double(1.0), "1.0");
        assert_eq!(format_double(100.0), "100.0");
        assert_eq!(format_double(2.71), "2.71");
        assert_eq!(format_double(-123.456), "-123.456");
        assert_eq!(format_double(0.001), "0.001");
        assert_eq!(format_double(0.0009), "9.0E-4");
        assert_eq!(format_double(0.0001), "1.0E-4");
        assert_eq!(format_double(9999999.0), "9999999.0");
        assert_eq!(format_double(10000000.0), "1.0E7");
        assert_eq!(format_double(12345678.0), "1.2345678E7");
        assert_eq!(format_double(1e20), "1.0E20");
        assert_eq!(format_double(1e-7), "1.0E-7");
        assert_eq!(format_double(1e308), "1.0E308");
        assert_eq!(format_double(123456789012345.0), "1.23456789012345E14");
    }

    #[test]
    fn test_format_single() {
        assert_eq!(format_single(f32::NAN), "NaN");
        assert_eq!(format_single(f32::INFINITY), "Infinity");
        assert_eq!(format_single(1.0), "1.0");
        assert_eq!(format_single(0.1), "0.1");
        assert_eq!(format_single(123.456), "123.456");
        assert_eq!(format_single(10000000.0), "1.0E7");
    }
}
