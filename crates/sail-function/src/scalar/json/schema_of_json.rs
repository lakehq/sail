use std::any::Any;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{downcast_array, Array, MapArray, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_types_exec_err};

/// Infers the schema of a JSON string and returns it in DDL format.
///
/// Example: schema_of_json('[{"col":0}]') returns 'ARRAY<STRUCT<col: BIGINT>>'
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfJson {
    signature: Signature,
}

impl Default for SparkSchemaOfJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaOfJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn validate_args_are_literal(cols: &[ColumnarValue]) -> Result<()> {
        if let Some(ColumnarValue::Array(_)) = cols.first() {
            return Err(generic_exec_err(
                "schema_of_json",
                "the input `json` should be a foldable string expression, got a column reference",
            ));
        }
        if let Some(ColumnarValue::Array(_)) = cols.get(1) {
            return Err(generic_exec_err(
                "schema_of_json",
                "the input `options` should be a foldable map expression, got a column reference",
            ));
        }
        Ok(())
    }

    fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => Ok(()),
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Map(map_field, _)] => {
                if let DataType::Struct(fields) = map_field.data_type() {
                    let key = &fields[0];
                    let value = &fields[1];
                    if !key.data_type().is_string() || !value.data_type().is_string() {
                        return Err(generic_exec_err(
                            "schema_of_json",
                            &format!(
                                "options map keys/values must both be string type, got key: {}, value: {}",
                                key.data_type(),
                                value.data_type()
                            ),
                        ));
                    }
                    Ok(())
                } else {
                    Err(unsupported_data_types_exec_err(
                        "schema_of_json",
                        "STRING and MAP<STRING, STRING>",
                        arg_types,
                    ))
                }
            }
            [DataType::Null] => Err(generic_exec_err(
                "schema_of_json",
                "the json must not be null",
            )),
            _ => Err(unsupported_data_types_exec_err(
                "schema_of_json",
                "STRING",
                arg_types,
            )),
        }
    }
}

impl ScalarUDFImpl for SparkSchemaOfJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "schema_of_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "schema_of_json",
                (1, 2),
                arg_types.len(),
            ));
        }
        Self::validate_arg_types(arg_types)?;
        let mut coerced = vec![DataType::Utf8];
        if arg_types.len() > 1 {
            coerced.push(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ));
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        Self::validate_args_are_literal(&args)?;

        // Parse options from the optional second argument.
        let opts = if args.len() > 1 {
            if let ColumnarValue::Scalar(ScalarValue::Map(map_arr)) = &args[1] {
                SparkSchemaOfJsonOptions::default().map_to_options(map_arr.as_ref())?
            } else {
                SparkSchemaOfJsonOptions::default()
            }
        } else {
            SparkSchemaOfJsonOptions::default()
        };

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let json_str = match scalar {
                    ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
                        s.as_deref()
                    }
                    ScalarValue::Null => {
                        return Err(generic_exec_err(
                            "schema_of_json",
                            "the json must not be null",
                        ));
                    }
                    _ => {
                        return Err(generic_exec_err(
                            "schema_of_json",
                            "first argument must be a string",
                        ))
                    }
                };

                let json_str = json_str.ok_or_else(|| {
                    generic_exec_err("schema_of_json", "the json must not be null")
                })?;

                let schema = infer_json_schema(json_str, &opts)?;

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(schema))))
            }
            ColumnarValue::Array(_) => Err(generic_exec_err(
                "schema_of_json",
                "the input `json` should be a foldable string expression, got a column reference",
            )),
        }
    }
}

/// Internal typed representation of an inferred JSON type.
///
/// Using a typed enum (instead of DDL strings) avoids ever re-parsing the
/// output: `common_supertype` operates on variants, and field names are
/// stored **raw** in `Struct` so escaping is applied exactly once, at the
/// final `to_ddl()` serialization step.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InferredType {
    Null,
    Boolean,
    BigInt,
    /// Integer wider than i64. Spark uses `DECIMAL(precision, 0)`.
    Decimal {
        precision: u32,
    },
    Double,
    String,
    Timestamp,
    Array(Box<InferredType>),
    Struct(Vec<(String, InferredType)>),
}

impl InferredType {
    /// In Spark, a bare JSON `null` surfaces as STRING.
    fn coerce_bare_null(self) -> Self {
        if matches!(self, InferredType::Null) {
            InferredType::String
        } else {
            self
        }
    }

    fn to_ddl(&self) -> String {
        match self {
            InferredType::Null => "NULL".to_string(),
            InferredType::Boolean => "BOOLEAN".to_string(),
            InferredType::BigInt => "BIGINT".to_string(),
            InferredType::Decimal { precision } => format!("DECIMAL({precision},0)"),
            InferredType::Double => "DOUBLE".to_string(),
            InferredType::String => "STRING".to_string(),
            InferredType::Timestamp => "TIMESTAMP".to_string(),
            InferredType::Array(elem) => format!("ARRAY<{}>", elem.to_ddl()),
            InferredType::Struct(fields) if fields.is_empty() => "STRUCT<>".to_string(),
            InferredType::Struct(fields) => {
                let rendered: Vec<String> = fields
                    .iter()
                    .map(|(name, typ)| format!("{}: {}", escape_field_name(name), typ.to_ddl()))
                    .collect();
                format!("STRUCT<{}>", rendered.join(", "))
            }
        }
    }
}

/// Returns the common supertype of two inferred types, following Spark's
/// JSON schema-inference promotion rules.
fn common_supertype(a: InferredType, b: InferredType) -> InferredType {
    use InferredType::*;

    if a == b {
        return a;
    }
    match (a, b) {
        // `null` loses against any concrete type.
        (Null, other) | (other, Null) => other,

        // Numeric promotion ladder: BigInt < Decimal < Double.
        (BigInt, Double) | (Double, BigInt) => Double,
        (Decimal { .. }, Double) | (Double, Decimal { .. }) => Double,
        (BigInt, Decimal { precision }) | (Decimal { precision }, BigInt) => {
            // Spark widens BIGINT to DECIMAL(20,0) when merging with DECIMAL,
            // to accommodate i64::MAX overflow values like 9223372036854775808.
            Decimal {
                precision: precision.max(20),
            }
        }
        (Decimal { precision: p1 }, Decimal { precision: p2 }) => Decimal {
            precision: p1.max(p2),
        },

        // Timestamp merges with String as STRING.
        (Timestamp, String) | (String, Timestamp) => String,

        // Arrays recurse on element type.
        (Array(a), Array(b)) => Array(Box::new(common_supertype(*a, *b))),

        // Structs: union of fields, recurse on shared names.
        (Struct(fa), Struct(fb)) => Struct(merge_struct_fields(fa, fb)),

        // Any other mix falls back to STRING (matches Spark's behaviour for
        // e.g. `[1, "foo"]` or `[true, 1]`).
        _ => String,
    }
}

/// Merges two lists of struct fields: shared names are promoted via
/// `common_supertype`, unique names are kept. Output is sorted by name to
/// match Spark's deterministic ordering.
fn merge_struct_fields(
    a: Vec<(String, InferredType)>,
    b: Vec<(String, InferredType)>,
) -> Vec<(String, InferredType)> {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<String, InferredType> = a.into_iter().collect();
    for (name, typ) in b {
        map.entry(name)
            .and_modify(|existing| {
                *existing =
                    common_supertype(std::mem::replace(existing, InferredType::Null), typ.clone());
            })
            .or_insert(typ);
    }
    map.into_iter().collect()
}

/// Spark-compatible field-name escaping.
///
/// Unquoted identifiers in Spark DDL must match `[A-Za-z_][A-Za-z0-9_]*`.
/// Anything else gets backtick-quoted; embedded backticks are doubled.
fn escape_field_name(name: &str) -> String {
    let mut chars = name.chars();
    let is_unquoted = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if is_unquoted {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

/// Returns true if `s` looks like a timestamp or date literal that Spark's
/// `inferTimestamp` option would promote to TIMESTAMP.
fn is_timestamp_string(s: &str) -> bool {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
        || NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

/// Infer the Spark SQL DDL schema from a JSON string.
fn infer_json_schema(json: &str, opts: &SparkSchemaOfJsonOptions) -> Result<String> {
    if json.trim().is_empty() {
        return Ok(InferredType::String.to_ddl());
    }
    let mut jiter = if opts.allow_non_numeric_numbers {
        Jiter::new(json.as_bytes()).with_allow_inf_nan()
    } else {
        Jiter::new(json.as_bytes())
    };
    let peek = jiter
        .peek()
        .map_err(|e| generic_exec_err("schema_of_json", &format!("failed to parse JSON: {e}")))?;
    let inferred = infer_type_from_peek(&mut jiter, peek, opts)?;
    Ok(inferred.coerce_bare_null().to_ddl())
}

fn jiter_err(e: impl std::fmt::Display) -> DataFusionError {
    generic_exec_err("schema_of_json", &format!("failed to parse JSON: {e}"))
}

fn infer_type_from_peek(
    jiter: &mut Jiter,
    peek: Peek,
    opts: &SparkSchemaOfJsonOptions,
) -> Result<InferredType> {
    let inferred = match peek {
        Peek::Null => {
            jiter.known_null().map_err(jiter_err)?;
            InferredType::Null
        }
        Peek::True | Peek::False => {
            jiter.known_bool(peek).map_err(jiter_err)?;
            if opts.primitives_as_string {
                InferredType::String
            } else {
                InferredType::Boolean
            }
        }
        Peek::String => {
            let s = jiter.known_str().map_err(jiter_err)?;
            if opts.infer_timestamp && is_timestamp_string(s) {
                InferredType::Timestamp
            } else {
                InferredType::String
            }
        }
        Peek::Minus => {
            let t = infer_number_type(jiter)?;
            if opts.primitives_as_string {
                InferredType::String
            } else {
                t
            }
        }
        Peek::Infinity | Peek::NaN => {
            jiter.known_float(peek).map_err(jiter_err)?;
            if opts.primitives_as_string {
                InferredType::String
            } else {
                InferredType::Double
            }
        }
        Peek::Array => infer_array_type(jiter, opts)?,
        Peek::Object => infer_struct_type(jiter, opts)?,
        _ => {
            let t = infer_number_type(jiter)?;
            if opts.primitives_as_string {
                InferredType::String
            } else {
                t
            }
        }
    };
    Ok(inferred)
}

fn infer_number_type(jiter: &mut Jiter) -> Result<InferredType> {
    let start = jiter.current_index();
    jiter.next_skip().map_err(jiter_err)?;
    let slice = jiter.slice_to_current(start);
    let num_str = std::str::from_utf8(slice)
        .map_err(|e| generic_exec_err("schema_of_json", &format!("invalid number utf8: {e}")))?;

    if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
        return Ok(InferredType::Double);
    }

    let digits = num_str.trim_start_matches('-');
    if digits.len() > 38 {
        // Overflows DECIMAL(38,0) max precision → Spark falls back to DOUBLE.
        return Ok(InferredType::Double);
    }
    // Integer that doesn't fit in BIGINT widens to DECIMAL(digits, 0).
    if digits.len() > 18 && num_str.parse::<i64>().is_err() {
        return Ok(InferredType::Decimal {
            precision: digits.len() as u32,
        });
    }
    Ok(InferredType::BigInt)
}

fn infer_array_type(jiter: &mut Jiter, opts: &SparkSchemaOfJsonOptions) -> Result<InferredType> {
    let first_peek = jiter.known_array().map_err(jiter_err)?;

    let Some(element_peek) = first_peek else {
        return Ok(InferredType::Array(Box::new(InferredType::String)));
    };

    let mut element_type = infer_type_from_peek(jiter, element_peek, opts)?;

    while let Some(peek) = jiter.array_step().map_err(jiter_err)? {
        let next_type = infer_type_from_peek(jiter, peek, opts)?;
        element_type = common_supertype(element_type, next_type);
    }

    Ok(InferredType::Array(Box::new(
        element_type.coerce_bare_null(),
    )))
}

fn infer_struct_type(jiter: &mut Jiter, opts: &SparkSchemaOfJsonOptions) -> Result<InferredType> {
    let first_key = jiter.known_object().map_err(jiter_err)?;

    let Some(mut current_key) = first_key else {
        return Ok(InferredType::Struct(Vec::new()));
    };

    let mut fields: Vec<(String, InferredType)> = Vec::new();

    loop {
        let field_name = current_key.to_string();
        let peek = jiter.peek().map_err(jiter_err)?;
        let field_type = infer_type_from_peek(jiter, peek, opts)?.coerce_bare_null();
        // Spark silently drops fields with empty-string keys.
        if !field_name.is_empty() {
            fields.push((field_name, field_type));
        }

        match jiter.next_key().map_err(jiter_err)? {
            Some(key) => current_key = key,
            None => break,
        }
    }

    fields.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(InferredType::Struct(fields))
}

#[derive(Debug, Default)]
enum ModeOptions {
    #[default]
    Permissive,
    FailFast,
    DropMalformed,
}

impl ModeOptions {
    fn from_str(value: String) -> Result<Self> {
        match value.as_str() {
            "PERMISSIVE" => Ok(ModeOptions::Permissive),
            "FAILFAST" => Ok(ModeOptions::FailFast),
            "DROPMALFORMED" => Ok(ModeOptions::DropMalformed),
            other => Err(generic_exec_err(
                "schema_of_json",
                &format!("invalid mode option: {other}"),
            )),
        }
    }
}

#[derive(Debug, Default)]
struct SparkSchemaOfJsonOptions {
    mode: ModeOptions,
    primitives_as_string: bool,
    infer_timestamp: bool,
    allow_non_numeric_numbers: bool,
}

impl SparkSchemaOfJsonOptions {
    pub fn map_to_options(mut self, map_array: &MapArray) -> Result<Self> {
        if map_array.is_empty() {
            return Ok(self);
        }
        let inner_struct = map_array.value(0);
        let (keys, values) = Self::get_keys_values_from_map(inner_struct)?;
        for (key, value) in keys.iter().zip(values.iter()) {
            let (key, value) = Self::unwrap_or_key_value(key, value)?;
            match key {
                "mode" => self.mode = ModeOptions::from_str(value.to_string())?,
                "primitivesAsString" => self.primitives_as_string = value == "true",
                "inferTimestamp" => self.infer_timestamp = value == "true",
                "allowNonNumericNumbers" => self.allow_non_numeric_numbers = value == "true",
                "allowNumericLeadingZeros" => {
                    // Parsing numbers with leading zeros would require either a
                    // permissive JSON parser or extending jiter. Until that is
                    // wired in, we refuse the option explicitly rather than
                    // accepting it as a silent no-op.
                    return Err(DataFusionError::NotImplemented(format!(
                        "`schema_of_json` option `allowNumericLeadingZeros` is not yet supported (value: {value})"
                    )));
                }
                other => {
                    return Err(generic_exec_err(
                        "schema_of_json",
                        &format!("unsupported option: {other}"),
                    ));
                }
            }
        }
        Ok(self)
    }

    fn get_keys_values_from_map(inner_struct: StructArray) -> Result<(StringArray, StringArray)> {
        let (keys, values) = match inner_struct.data_type() {
            DataType::Struct(fields) => {
                let key_type = fields[0].data_type();
                let value_type = fields[1].data_type();
                if key_type == &DataType::Utf8 && value_type == &DataType::Utf8 {
                    let keys = downcast_array::<StringArray>(inner_struct.column(0));
                    let values = downcast_array::<StringArray>(inner_struct.column(1));
                    (keys, values)
                } else {
                    return Err(generic_exec_err(
                        "schema_of_json",
                        &format!(
                            "options map must be map<string, string>, got key: {key_type:?}, value: {value_type:?}"
                        ),
                    ));
                }
            }
            other => {
                return Err(generic_exec_err(
                    "schema_of_json",
                    &format!("options must be a map with struct entries, got {other:?}"),
                ));
            }
        };
        Ok((keys, values))
    }

    fn unwrap_or_key_value<'a>(
        key: Option<&'a str>,
        value: Option<&'a str>,
    ) -> Result<(&'a str, &'a str)> {
        match (key, value) {
            (Some(k), Some(v)) => Ok((k, v)),
            _ => Err(generic_exec_err(
                "schema_of_json",
                &format!("unexpected options key/value pair: {key:?}: {value:?}"),
            )),
        }
    }
}
