use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err, plan_err};
use datafusion_expr::ScalarFunctionArgs;
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::scalar::math::spark_hex::{printed_text, prints_as_text};

/// Spark's `Unhex(expr, failOnError)`.
///
/// `Unhex` is `ImplicitCastInputTypes` over STRING and decodes the UTF-8 BYTES of its input in
/// pairs, each digit through `java.util.HexFormat.fromHexDigit` (`mathExpressions.scala`), which
/// accepts only ASCII `[0-9A-Fa-f]` and throws for any other byte. With `failOnError = false`
/// (the SQL `unhex`) a throw is a NULL; with `failOnError = true` (what `to_binary(.., 'hex')` is)
/// it is `CONVERSION_INVALID_INPUT`. An odd number of digits is padded on the left, an empty input
/// is an empty binary, and the result is always nullable BINARY.
///
/// `ansi_mode` only decides whether a calendar interval is implicitly cast to STRING (see
/// `coerce_input`).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnHex {
    signature: Signature,
    fail_on_error: bool,
    ansi_mode: bool,
}

impl Default for SparkUnHex {
    fn default() -> Self {
        Self::with_options(false, false)
    }
}

impl SparkUnHex {
    pub fn with_options(fail_on_error: bool, ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            fail_on_error,
            ansi_mode,
        }
    }

    pub fn fail_on_error(&self) -> bool {
        self.fail_on_error
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkUnHex {
    fn name(&self) -> &str {
        "spark_unhex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `Unhex.nullable = true`, unconditional (not narrowed by `failOnError`).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1247>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg_type] = arg_types else {
            return plan_err!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `unhex` requires 1 parameters but the actual number is {}. Please, refer to 'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix.",
                arg_types.len()
            );
        };
        Ok(vec![coerce_input(arg_type, "unhex", self.ansi_mode)?])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // The arity is validated once, in `coerce_types`.
        let [arg] = args.args.as_slice() else {
            return internal_err!("`unhex` expects 1 argument, got {}", args.args.len());
        };
        let (array, is_scalar) = match arg {
            ColumnarValue::Array(array) => (Arc::clone(array), false),
            ColumnarValue::Scalar(value) => (value.to_array()?, true),
        };
        let array = if prints_as_text(array.data_type()) {
            printed_text(&args, array)?
        } else {
            array
        };
        let decoded = unhex_array(&array, self.fail_on_error)?;
        if is_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &decoded, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(decoded))
        }
    }
}

/// The type an input is handed to the kernel as: Spark casts it to STRING first, so what is
/// decoded is its printed text.
///
/// - STRING and BINARY are decoded as they are (a BINARY cast to STRING keeps its bytes);
/// - an integer prints as its digits, so it goes through a cast to STRING;
/// - everything else Spark can print (a fractional number, a decimal with a scale, BOOLEAN, a
///   datetime, an interval, VARIANT) is left as it is and printed when it is evaluated, so that
///   the text is what is decoded (or what an error names);
/// - a calendar interval is only cast to STRING when ANSI is on: the implicit cast covers
///   `AtomicType` only (`TypeCoercion.scala:234`) and a calendar interval is not one;
/// - ARRAY, MAP and STRUCT are rejected, as Spark's implicit cast has no rule for them.
pub(crate) fn coerce_input(
    data_type: &DataType,
    function: &str,
    ansi_mode: bool,
) -> Result<DataType> {
    Ok(match data_type {
        DataType::Interval(IntervalUnit::MonthDayNano) if !ansi_mode => {
            return plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{function}\" due to data type mismatch: The first parameter requires the \"STRING\" type, however the input has the type \"INTERVAL\"."
            );
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => data_type.clone(),
        DataType::BinaryView => DataType::Binary,
        DataType::Dictionary(_, value_type) => coerce_input(value_type, function, ansi_mode)?,
        DataType::Null => DataType::Utf8,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Utf8,
        DataType::Decimal32(_, 0)
        | DataType::Decimal64(_, 0)
        | DataType::Decimal128(_, 0)
        | DataType::Decimal256(_, 0) => DataType::Utf8,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Boolean
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => data_type.clone(),
        data_type if is_marked_variant_storage_type(data_type) => data_type.clone(),
        other => {
            return plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{function}\" due to data type mismatch: The first parameter requires the \"STRING\" type, however the input has the type \"{other}\"."
            );
        }
    })
}

fn unhex_array(array: &ArrayRef, fail_on_error: bool) -> Result<ArrayRef> {
    let len = array.len();
    match array.data_type() {
        DataType::Utf8 => unhex_values(
            array
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(str::as_bytes)),
            len,
            fail_on_error,
        ),
        DataType::LargeUtf8 => unhex_values(
            array
                .as_string::<i64>()
                .iter()
                .map(|v| v.map(str::as_bytes)),
            len,
            fail_on_error,
        ),
        DataType::Utf8View => unhex_values(
            array.as_string_view().iter().map(|v| v.map(str::as_bytes)),
            len,
            fail_on_error,
        ),
        DataType::Binary => unhex_values(array.as_binary::<i32>().iter(), len, fail_on_error),
        DataType::LargeBinary => unhex_values(array.as_binary::<i64>().iter(), len, fail_on_error),
        DataType::FixedSizeBinary(_) => {
            unhex_values(array.as_fixed_size_binary().iter(), len, fail_on_error)
        }
        other => internal_err!("`unhex` cannot decode {other}; `coerce_types` should have cast it"),
    }
}

fn unhex_values<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
    len: usize,
    fail_on_error: bool,
) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(len, 0);
    let mut decoded = Vec::new();
    for value in values {
        let Some(bytes) = value else {
            builder.append_null();
            continue;
        };
        decoded.clear();
        if unhex_bytes(bytes, &mut decoded) {
            builder.append_value(&decoded);
        } else if fail_on_error {
            return Err(invalid_input_err(&String::from_utf8_lossy(bytes)));
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// `DataTypeErrorsBase.toSQLValue(String)`: quoted, with `\\` and `'` escaped.
pub(crate) fn sql_string_value(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn invalid_input_err(value: &str) -> datafusion_common::DataFusionError {
    conversion_invalid_input_err(value, "HEX")
}

/// `QueryExecutionErrors.invalidInputInConversionError` as `ToBinary` raises it: `format` is the
/// format the value did not follow.
pub(crate) fn conversion_invalid_input_err(
    value: &str,
    format: &str,
) -> datafusion_common::DataFusionError {
    let value = sql_string_value(value);
    exec_datafusion_err!(
        "[CONVERSION_INVALID_INPUT] The value {value} ('{format}') cannot be converted to \"BINARY\" because it is malformed. Correct the value as per the syntax, or change its format. Use `try_to_binary` to tolerate malformed input and return NULL instead."
    )
}

// [Credit]: <https://github.com/apache/datafusion-comet/blob/bfd7054c02950219561428463d3926afaf8edbba/native/spark-expr/src/scalar_funcs/unhex.rs>

/// `java.util.HexFormat.fromHexDigit`: ASCII `[0-9A-Fa-f]`, nothing else.
fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// `Hex.unhex(bytes)`: decodes `bytes` into `out` and returns whether every byte was a hex digit.
/// An odd count leaves the first digit on its own, so the result is padded on the left.
fn unhex_bytes(bytes: &[u8], out: &mut Vec<u8>) -> bool {
    out.reserve(bytes.len().div_ceil(2));
    let mut rest = bytes;
    if bytes.len() % 2 == 1 {
        let Some(first) = hex_digit(bytes[0]) else {
            return false;
        };
        out.push(first);
        rest = &bytes[1..];
    }
    let (pairs, _) = rest.as_chunks::<2>();
    for [high, low] in pairs {
        let (Some(high), Some(low)) = (hex_digit(*high), hex_digit(*low)) else {
            return false;
        };
        out.push((high << 4) | low);
    }
    true
}
