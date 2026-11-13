use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, StringArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, exec_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;
use quick_xml::Reader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesStart, Event};

use crate::scalar::datetime::format::DateTimeFormat;
use crate::scalar::datetime::timestamp_parser::parse_timestamp_string;
use crate::utils::schema_inference::timestamp::is_timestamp_string;
use crate::utils::schema_inference::{
    InferredType, TypeMerger, merge_decimals, type_coercion, validation,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfXml {
    signature: Signature,
    case_sensitive: bool,
    timestamp_ntz: bool,
}

impl SparkSchemaOfXml {
    pub fn new(case_sensitive: bool, timestamp_ntz: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            case_sensitive,
            timestamp_ntz,
        }
    }

    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    pub fn timestamp_ntz(&self) -> bool {
        self.timestamp_ntz
    }
}

impl ScalarUDFImpl for SparkSchemaOfXml {
    fn name(&self) -> &str {
        "schema_of_xml"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        type_coercion::coerce_types(self.name(), arg_types, false)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validation::validate_literal_args(self.name(), &args.args)?;
        make_scalar_function(
            |args| schema_of_xml(args, self.case_sensitive, self.timestamp_ntz),
            vec![Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args.args)
    }
}

fn schema_of_xml(args: &[ArrayRef], case_sensitive: bool, timestamp_ntz: bool) -> Result<ArrayRef> {
    validation::validate_args_len("schema_of_xml", args)?;
    let rows = downcast_arg!(&args[0], StringArray);
    if rows.is_empty() || rows.is_null(0) {
        return plan_err!("For function `schema_of_xml`, the xml must not be null");
    }
    let options = XmlInferenceOptions::new(args.get(1), case_sensitive, timestamp_ntz)?;
    let inferred = match options.infer_xml(rows.value(0)) {
        Ok(inferred) => inferred,
        Err(error) if options.fail_fast => {
            return exec_err!("Malformed XML in schema_of_xml: {error}");
        }
        Err(_) => {
            InferredType::Struct(vec![(options.corrupt_record.clone(), InferredType::String)])
        }
    };
    Ok(Arc::new(StringArray::from(vec![
        inferred.canonicalized_ddl(),
    ])))
}

struct XmlInferenceOptions {
    case_sensitive: bool,
    timestamp_ntz: bool,
    exclude_attribute: bool,
    ignore_namespace: bool,
    ignore_spaces: bool,
    infer_schema: bool,
    prefers_decimal: bool,
    attribute_prefix: String,
    value_tag: String,
    corrupt_record: String,
    fail_fast: bool,
    date_format: DateTimeFormat,
    timestamp_format: Option<DateTimeFormat>,
    timestamp_ntz_format: Option<DateTimeFormat>,
}

type XmlResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl XmlInferenceOptions {
    fn new(arg: Option<&ArrayRef>, case_sensitive: bool, timestamp_ntz: bool) -> Result<Self> {
        let mut options = BTreeMap::new();
        if let Some(arg) = arg {
            let map = downcast_arg!(arg, MapArray);
            if map.is_empty() || map.is_null(0) {
                return plan_err!("schema_of_xml options must not be null");
            }
            let entries = map.value(0);
            let keys = downcast_arg!(entries.column(0), StringArray);
            let values = downcast_arg!(entries.column(1), StringArray);
            for (key, value) in keys.iter().zip(values.iter()) {
                let Some(key) = key else {
                    return plan_err!("schema_of_xml options must not contain null keys");
                };
                options.insert(
                    key.to_ascii_lowercase(),
                    value.unwrap_or("null").to_string(),
                );
            }
        }
        let boolean = |key: &str, default: bool| -> Result<bool> {
            match options.get(key).map(|v| v.to_ascii_lowercase()).as_deref() {
                None => Ok(default),
                Some("true") => Ok(true),
                Some("false") => Ok(false),
                _ => plan_err!("Invalid boolean value for option `{key}` in `schema_of_xml`"),
            }
        };
        let format = |key: &str| {
            options
                .get(key)
                .map(|s| DateTimeFormat::for_parsing(s))
                .transpose()
        };
        let attribute_prefix = options
            .get("attributeprefix")
            .cloned()
            .unwrap_or_else(|| "_".into());
        let value_tag = options
            .get("valuetag")
            .cloned()
            .unwrap_or_else(|| "_VALUE".into());
        if value_tag.is_empty() || value_tag == attribute_prefix {
            return plan_err!(
                "schema_of_xml valueTag must be non-empty and different from attributePrefix"
            );
        }
        let mode = options
            .get("mode")
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();
        if mode == "DROPMALFORMED" {
            return plan_err!("schema_of_xml does not support DROPMALFORMED mode");
        }
        for key in ["rowvalidationxsdpath", "locale"] {
            if options
                .get(key)
                .is_some_and(|v| key != "locale" || !v.eq_ignore_ascii_case("en-US"))
            {
                return Err(DataFusionError::NotImplemented(format!(
                    "schema_of_xml does not support option `{key}`"
                )));
            }
        }
        Ok(Self {
            case_sensitive,
            timestamp_ntz,
            exclude_attribute: boolean("excludeattribute", false)?,
            ignore_namespace: boolean("ignorenamespace", false)?,
            ignore_spaces: boolean("ignoresurroundingspaces", true)?,
            infer_schema: boolean("inferschema", true)?,
            prefers_decimal: boolean("prefersdecimal", false)?,
            attribute_prefix,
            value_tag,
            corrupt_record: options
                .get("columnnameofcorruptrecord")
                .cloned()
                .unwrap_or_else(|| "_corrupt_record".into()),
            fail_fast: mode == "FAILFAST",
            date_format: DateTimeFormat::for_parsing(
                options
                    .get("dateformat")
                    .map_or("yyyy-MM-dd", String::as_str),
            )?,
            timestamp_format: format("timestampformat")?,
            timestamp_ntz_format: format("timestampntzformat")?,
        })
    }

    fn infer_value(&self, value: &str) -> InferredType {
        use InferredType::*;
        if !self.infer_schema {
            return String;
        }
        let value = if self.ignore_spaces {
            value.trim_matches(|c| c <= '\u{20}')
        } else {
            value
        };
        if value.is_empty() {
            return Null;
        }
        if value.parse::<i64>().is_ok() {
            return Long;
        }
        if self.prefers_decimal
            && let Some(decimal) = infer_decimal(value)
        {
            return decimal;
        }
        let unsigned = value.trim_start_matches(['+', '-']);
        if unsigned.starts_with(|c: char| c.is_ascii_digit() || c == '.')
            && value.parse::<f64>().is_ok()
        {
            return Double;
        }
        if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
            return Boolean;
        }
        if self.date_format.parse_datetime_value(value).is_ok() {
            return Date;
        }
        if self.timestamp_ntz {
            let is_ntz = self.timestamp_ntz_format.as_ref().map_or_else(
                || {
                    is_timestamp_string(value)
                        && parse_timestamp_string(value)
                            .is_some_and(|p| p.timezone.is_none() && !p.just_time)
                },
                |f| {
                    f.parse_datetime_value(value)
                        .is_ok_and(|p| p.offset.is_none() && p.timezone.is_none())
                },
            );
            if is_ntz {
                return TimestampNtz;
            }
        }
        if self.timestamp_format.as_ref().map_or_else(
            || is_timestamp_string(value),
            |f| f.parse_datetime_value(value).is_ok(),
        ) {
            return Timestamp;
        }
        String
    }

    fn name<'a>(&self, name: &'a str) -> &'a str {
        if self.ignore_namespace {
            name.rsplit_once(':').map_or(name, |(_, local)| local)
        } else {
            name
        }
    }

    fn add_field(&self, fields: &mut Vec<(String, InferredType)>, name: String, ty: InferredType) {
        if let Some((_, old)) = fields
            .iter_mut()
            .find(|(n, _)| self.compare_field_names(n, &name).is_eq())
        {
            let previous = std::mem::replace(old, InferredType::Null);
            let element = match previous {
                InferredType::Array(element) => *element,
                other => other,
            };
            *old = InferredType::Array(Box::new(element.merge_with(ty, self)));
        } else {
            fields.push((name, ty));
        }
    }

    fn infer_xml(&self, xml: &str) -> XmlResult<InferredType> {
        // Use lexical names rather than resolving namespaces. Spark accepts undeclared
        // prefixes and treats different prefixes for the same URI as distinct names.
        let mut reader = Reader::from_str(xml);
        reader.config_mut().expand_empty_elements = true;
        reader.config_mut().check_comments = true;
        let mut inferred = None;
        loop {
            match reader.read_event()? {
                Event::Start(element) if inferred.is_none() => {
                    inferred = Some(self.infer_element(&mut reader, element, true)?);
                }
                Event::Text(text)
                    if text
                        .xml_content()?
                        .trim_matches([' ', '\t', '\n', '\r'])
                        .is_empty() => {}
                Event::Decl(_) | Event::DocType(_) | Event::Comment(_) | Event::PI(_) => {}
                Event::Eof => return inferred.ok_or_else(|| "Missing XML root element".into()),
                _ => return Err("Unexpected content outside XML root element".into()),
            }
        }
    }

    fn add_text(&self, fields: &mut Vec<(String, InferredType)>, text: &mut String) {
        if !text.trim_matches([' ', '\t', '\n', '\r']).is_empty() {
            self.add_field(fields, self.value_tag.clone(), self.infer_value(text));
        }
        text.clear();
    }

    fn infer_element(
        &self,
        reader: &mut Reader<&[u8]>,
        element: BytesStart<'_>,
        root: bool,
    ) -> XmlResult<InferredType> {
        let mut attributes = Vec::new();
        for attribute in element.attributes() {
            let attribute = attribute?;
            let name = reader.decoder().decode(attribute.key.as_ref())?;
            let value = attribute.decode_and_unescape_value(reader.decoder())?;
            if !self.exclude_attribute {
                attributes.push((
                    format!("{}{}", self.attribute_prefix, self.name(&name)),
                    self.infer_value(&value),
                ));
            }
        }
        let mut fields = Vec::new();
        let mut has_elements = false;
        let mut text = String::new();
        loop {
            match reader.read_event()? {
                Event::Text(value) => text.push_str(&value.xml_content()?),
                Event::CData(value) => text.push_str(&value.xml_content()?),
                Event::GeneralRef(reference) => {
                    if let Some(character) = reference.resolve_char_ref()? {
                        text.push(character);
                    } else {
                        let name = reference.decode()?;
                        text.push_str(
                            resolve_predefined_entity(&name).ok_or("Unknown XML entity")?,
                        );
                    }
                }
                Event::Start(element) => {
                    self.add_text(&mut fields, &mut text);
                    has_elements = true;
                    let name = element.name();
                    let name = reader.decoder().decode(name.as_ref())?;
                    let name = self.name(&name).to_string();
                    let inferred = self.infer_element(reader, element, false)?;
                    self.add_field(&mut fields, name, inferred);
                }
                Event::End(_) => {
                    self.add_text(&mut fields, &mut text);
                    break;
                }
                Event::Comment(_) | Event::PI(_) => self.add_text(&mut fields, &mut text),
                _ => return Err("Unexpected XML event inside element".into()),
            }
        }
        let primitive_text = fields
            .first()
            .is_none_or(|(_, ty)| !matches!(ty, InferredType::Array(_) | InferredType::Struct(_)));
        let mut inferred = if !root && !has_elements && fields.len() <= 1 && primitive_text {
            fields.pop().map(|(_, ty)| ty).unwrap_or(InferredType::Null)
        } else {
            InferredType::Struct(fields)
        };
        if !attributes.is_empty() {
            let mut fields = match inferred {
                InferredType::Struct(fields) => fields,
                InferredType::Null => vec![],
                ty => vec![(self.value_tag.clone(), ty)],
            };
            // Namespace declarations are ordinary attributes in the lexical reader.
            for (name, ty) in attributes {
                self.add_field(&mut fields, name, ty);
            }
            inferred = InferredType::Struct(fields);
        }
        if let InferredType::Struct(fields) = &mut inferred {
            fields.sort_by(|a, b| a.0.cmp(&b.0));
        }
        Ok(inferred)
    }
}

impl TypeMerger for XmlInferenceOptions {
    fn compare_field_names(&self, left: &str, right: &str) -> Ordering {
        if self.case_sensitive {
            left.cmp(right)
        } else {
            left.to_lowercase().cmp(&right.to_lowercase())
        }
    }

    fn merge_atomic(&self, left: InferredType, right: InferredType) -> InferredType {
        use InferredType::*;
        match (left, right) {
            (Array(element), other) | (other, Array(element)) => {
                Array(Box::new(element.merge_with(other, self)))
            }
            (Struct(mut fields), other) | (other, Struct(mut fields)) => {
                if let Some((_, ty)) = fields.iter_mut().find(|(name, _)| name == &self.value_tag) {
                    *ty = std::mem::replace(ty, Null).merge_with(other, self);
                    Struct(fields)
                } else {
                    String
                }
            }
            (Long, Double) | (Double, Long) | (Double, Decimal(_, _)) | (Decimal(_, _), Double) => {
                Double
            }
            (Long, Decimal(p, s)) | (Decimal(p, s), Long) => merge_decimals(20, 0, p, s),
            (Decimal(p1, s1), Decimal(p2, s2)) => merge_decimals(p1, s1, p2, s2),
            (Timestamp, TimestampNtz)
            | (TimestampNtz, Timestamp)
            | (Date, Timestamp)
            | (Timestamp, Date) => Timestamp,
            (Date, TimestampNtz) | (TimestampNtz, Date) => TimestampNtz,
            _ => String,
        }
    }
}

fn infer_decimal(value: &str) -> Option<InferredType> {
    let value = value.replace(',', "");
    let value = value.as_str();
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (mantissa, exponent) = match value.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (value, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty() && fraction.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{integer}{fraction}");
    let scale = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    let precision = i32::try_from(digits.trim_start_matches('0').len().max(1))
        .ok()?
        .max(scale);
    (scale >= 0 && precision <= 38).then_some(InferredType::Decimal(precision as u8, scale as u8))
}
