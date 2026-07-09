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
    declaration: String,
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
    pub const DECLARATION_OPTION: &'static str = "declaration";
    pub const DECLARATION_DEFAULT: &'static str =
        r#"version="1.0" encoding="UTF-8" standalone="yes""#;
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

        let declaration = find_key_value(map, Self::DECLARATION_OPTION)
            .unwrap_or_else(|| Self::DECLARATION_DEFAULT.to_string());
        if declaration.starts_with('<') || declaration.ends_with('>') {
            return plan_err!("`declaration` must not include angle brackets");
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
            declaration,
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
            declaration: Self::DECLARATION_DEFAULT.to_string(),
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
    let mut output: Vec<Option<String>> = Vec::with_capacity(array.len());

    for row in 0..array.len() {
        if array.is_null(row) {
            output.push(None);
        } else {
            output.push(Some(write_row(array, row, array.fields(), options)?));
        }
    }

    Ok(Arc::new(StringArray::from(output)))
}

const INDENT: &str = "    ";

fn write_row(
    array: &StructArray,
    row: usize,
    fields: &Fields,
    options: &SparkToXmlOptions,
) -> Result<String> {
    let mut buf = String::with_capacity(256);
    if !options.declaration.is_empty() {
        buf.push_str("<?xml ");
        buf.push_str(&options.declaration);
        buf.push_str("?>\n");
    }
    write_struct(&mut buf, array, row, fields, &options.row_tag, 0, options)?;
    Ok(buf)
}

/// Matches Spark's two-pass logic: attributes are written to the open tag,
/// then child elements follow.
fn write_struct(
    buf: &mut String,
    array: &StructArray,
    row: usize,
    fields: &Fields,
    tag: &str,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    let pad = INDENT.repeat(depth);

    let (attr_cols, elem_cols): (Vec<_>, Vec<_>) = fields
        .iter()
        .enumerate()
        .partition(|(_, f)| options.is_attribute(f.name()));

    buf.push_str(&pad);
    buf.push('<');
    buf.push_str(tag);

    for (col_idx, field) in &attr_cols {
        let col = array.column(*col_idx);
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

    if elem_cols.is_empty() {
        buf.push_str("/>\n");
    } else {
        let content_start = buf.len();
        buf.push_str(">\n");
        for (col_idx, field) in &elem_cols {
            let col = array.column(*col_idx);
            write_field(buf, col, row, field, depth + 1, options)?;
        }
        let content_end = buf.len();
        if content_end == content_start + 2 {
            buf.truncate(content_start);
            buf.push_str("/>\n");
        } else {
            buf.push_str(&pad);
            buf.push_str("</");
            buf.push_str(tag);
            buf.push_str(">\n");
        }
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
    let pad = INDENT.repeat(depth);
    let name = field.name().as_str();

    // value_tag handles text content injected directly into the parent
    if name == options.value_tag {
        if col.is_null(row) {
            if let Some(ref nv) = options.null_value {
                buf.push_str(&escape_text(nv));
            }
        } else {
            buf.push_str(&escape_text(&format_field_to_xml(
                col,
                row,
                field.data_type(),
                options,
            )?));
        }
        return Ok(());
    }

    if col.is_null(row) {
        if let Some(ref nv) = options.null_value {
            buf.push_str(&pad);
            push_element(buf, name, &escape_text(nv));
            buf.push('\n');
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
            buf.push_str(&pad);
            push_element(buf, name, &escape_text(&text));
            buf.push('\n');
        }
    }

    Ok(())
}

fn write_array(
    buf: &mut String,
    col: &ArrayRef,
    row: usize,
    field_name: &str,
    item_field: &Field,
    depth: usize,
    options: &SparkToXmlOptions,
) -> Result<()> {
    let pad = INDENT.repeat(depth);

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
                write_array(
                    buf,
                    &values,
                    item_idx,
                    options.array_element_name.as_str(),
                    inner_field,
                    depth,
                    options,
                )?;
            }
            _ => {
                if values.is_null(item_idx) {
                    if let Some(ref nv) = options.null_value {
                        buf.push_str(&pad);
                        buf.push('<');
                        buf.push_str(field_name);
                        buf.push_str(">\n");
                        buf.push_str(&INDENT.repeat(depth + 1));
                        push_element(buf, &options.array_element_name, &escape_text(nv));
                        buf.push('\n');
                        buf.push_str(&pad);
                        buf.push_str("</");
                        buf.push_str(field_name);
                        buf.push_str(">\n");
                    }
                } else {
                    let text =
                        format_field_to_xml(&values, item_idx, item_field.data_type(), options)?;
                    buf.push_str(&pad);
                    buf.push('<');
                    buf.push_str(field_name);
                    buf.push_str(">\n");
                    buf.push_str(&INDENT.repeat(depth + 1));
                    push_element(buf, &options.array_element_name, &escape_text(&text));
                    buf.push('\n');
                    buf.push_str(&pad);
                    buf.push_str("</");
                    buf.push_str(field_name);
                    buf.push_str(">\n");
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

    let pad = INDENT.repeat(depth);

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
        if values.is_null(i) {
            if let Some(ref nv) = options.null_value {
                inner.push_str(&INDENT.repeat(depth + 1));
                push_element(&mut inner, &key, &escape_text(nv));
                inner.push('\n');
            }
        } else {
            let val_dt = values.data_type();
            match val_dt {
                DataType::Struct(child_fields) => {
                    let child = values
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "to_xml: expected StructArray for map value in '{field_name}'"
                            ))
                        })?;
                    write_struct(&mut inner, child, i, child_fields, &key, depth + 1, options)?;
                }
                DataType::List(item_field) | DataType::LargeList(item_field) => {
                    let (offsets_start, offsets_end, array_values): (usize, usize, ArrayRef) =
                        if let Some(list) = values.as_any().downcast_ref::<ListArray>() {
                            (
                                list.offsets()[i] as usize,
                                list.offsets()[i + 1] as usize,
                                list.values().clone(),
                            )
                        } else if let Some(list) = values.as_any().downcast_ref::<LargeListArray>()
                        {
                            (
                                list.offsets()[i] as usize,
                                list.offsets()[i + 1] as usize,
                                list.values().clone(),
                            )
                        } else {
                            return exec_err!(
                                "to_xml: expected ListArray for map value in field '{field_name}'"
                            );
                        };
                    for elem_idx in offsets_start..offsets_end {
                        if array_values.is_null(elem_idx) {
                            if let Some(ref nv) = options.null_value {
                                inner.push_str(&INDENT.repeat(depth + 1));
                                push_element(&mut inner, &key, &escape_text(nv));
                                inner.push('\n');
                            }
                        } else {
                            match item_field.data_type() {
                                DataType::Struct(child_fields) => {
                                    let child = array_values
                                        .as_any()
                                        .downcast_ref::<StructArray>()
                                        .ok_or_else(|| DataFusionError::Internal(format!(
                                            "to_xml: expected StructArray in map array value for '{field_name}'"
                                        )))?;
                                    write_struct(
                                        &mut inner,
                                        child,
                                        elem_idx,
                                        child_fields,
                                        &key,
                                        depth + 1,
                                        options,
                                    )?;
                                }
                                _ => {
                                    let text = format_field_to_xml(
                                        &array_values,
                                        elem_idx,
                                        item_field.data_type(),
                                        options,
                                    )?;
                                    inner.push_str(&INDENT.repeat(depth + 1));
                                    push_element(&mut inner, &key, &escape_text(&text));
                                    inner.push('\n');
                                }
                            }
                        }
                    }
                }
                _ => {
                    let text = format_field_to_xml(values, i, val_dt, options)?;
                    inner.push_str(&INDENT.repeat(depth + 1));
                    push_element(&mut inner, &key, &escape_text(&text));
                    inner.push('\n');
                }
            }
        }
    }

    if inner.is_empty() {
        buf.push_str(&pad);
        buf.push('<');
        buf.push_str(field_name);
        buf.push_str("/>\n");
    } else {
        buf.push_str(&pad);
        buf.push('<');
        buf.push_str(field_name);
        buf.push_str(">\n");
        buf.push_str(&inner);
        buf.push_str(&pad);
        buf.push_str("</");
        buf.push_str(field_name);
        buf.push_str(">\n");
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

fn format_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        format!("{v}")
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
        ScalarValue::Float32(Some(v)) => Ok(format_float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Ok(format_float(*v)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Ok(v.clone()),
        sv if sv.is_null() => exec_err!("to_xml: scalar_to_display_string called on null: {sv:?}"),
        _ => exec_err!("to_xml: unsupported scalar type for XML serialization: {scalar:?}"),
    }
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

fn escape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(c),
        }
    }
    out
}

fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
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
        assert_eq!(escape_text("a & b < c > d"), "a &amp; b &lt; c &gt; d");
        assert_eq!(escape_text("clean"), "clean");
    }

    #[test]
    fn test_escape_attr() {
        assert_eq!(escape_attr(r#"say "hi""#), "say &quot;hi&quot;");
        assert_eq!(escape_attr("it's"), "it&apos;s");
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
    fn test_format_float() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(format_float(f64::NAN), "NaN");
        assert_eq!(format_float(f64::INFINITY), "Infinity");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(format_float(3.44), "3.44");
        assert_eq!(format_float(0.0), "0");
        Ok(())
    }
}
