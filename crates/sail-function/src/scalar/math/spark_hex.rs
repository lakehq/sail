use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, DecimalType, Field, FieldRef, Float32Type, Float64Type, IntervalUnit,
};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::{Result, ScalarValue, internal_err, plan_err};
use datafusion_expr::ScalarFunctionArgs;
// [Credit]: <https://github.com/apache/datafusion/blob/55.1.0/datafusion/spark/src/function/math/hex.rs>
// The encoding kernel (writes the hex digits straight into the output buffer) is the one from
// `datafusion-spark`'s `SparkHex`; this UDF adds what Spark's `Hex` does around it: the implicit
// casts, ANSI, and the nullability.
use datafusion_spark::function::math::hex::spark_hex as encode_hex;
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::scalar::math::spark_bin::{cast_overflow_err, double_to_i64, float_to_i64};
use crate::scalar::spark_to_string::SparkToUtf8;
use crate::scalar::variant::spark_variant_to_json::variant_to_text;

/// Spark's `hex`.
///
/// `Hex` is `ImplicitCastInputTypes` over `TypeCollection(LongType, BinaryType, StringType)`
/// (`mathExpressions.scala:1189-1215`), so the input is first cast to the first of those it can
/// be cast to:
/// - integral, fractional and decimal values go through a cast to BIGINT (saturating when ANSI is
///   off, `CAST_OVERFLOW` when it is on), then `Hex.hex(Long)` writes the shortest form;
/// - STRING and BINARY are written byte by byte;
/// - everything else Spark can print (BOOLEAN, DATE, TIMESTAMP, intervals...) is cast to STRING
///   and the text is hexed;
/// - an untyped NULL is cast to the first member of the collection (`TypeCoercion.scala:202`, BIGINT),
///   which gives a NULL string here all the same; ARRAY, MAP and STRUCT are rejected.
///
/// `Hex` is `nullIntolerant`, so the result is nullable only when the (cast) input is. The cast
/// from a fractional or decimal type to BIGINT is the one that can add a NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHexCast {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkHexCast {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkHexCast {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkHexCast {
    fn name(&self) -> &str {
        "spark_hex_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [arg] = args.arg_fields else {
            return wrong_num_args(args.arg_fields.len());
        };
        let nullable = arg.is_nullable() || casts_to_bigint_with_null(arg.data_type());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg_type] = arg_types else {
            return wrong_num_args(arg_types.len());
        };
        let coerced = match arg_type {
            // A calendar interval is only implicitly cast to STRING when ANSI is on. With ANSI off the
            // implicit cast only covers `AtomicType` (`TypeCoercion.scala:234`), which a calendar
            // interval is not, so `Hex` rejects it like any other type outside its collection.
            DataType::Interval(IntervalUnit::MonthDayNano) if !self.ansi_mode => {
                return unexpected_input_type(arg_type);
            }
            DataType::Int64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Interval(_)
            | DataType::Duration(_) => arg_type.clone(),
            data_type if is_marked_variant_storage_type(data_type) => data_type.clone(),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DataType::Int64,
            DataType::Float16 => DataType::Float64,
            DataType::Decimal32(p, s) | DataType::Decimal64(p, s) => DataType::Decimal128(*p, *s),
            DataType::BinaryView => DataType::Binary,
            DataType::Null => DataType::Utf8,
            // An Arrow dictionary is one more storage shape of its value type, not a Spark type.
            DataType::Dictionary(_, value_type) => {
                return self.coerce_types(&[value_type.as_ref().clone()]);
            }
            other => return unexpected_input_type(other),
        };
        Ok(vec![coerced])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // The arity is validated once, in `coerce_types`.
        let [arg] = args.args.as_slice() else {
            return internal_err!("`hex` expects 1 argument, got {}", args.args.len());
        };
        let (array, is_scalar) = match arg {
            ColumnarValue::Array(array) => (Arc::clone(array), false),
            ColumnarValue::Scalar(value) => (value.to_array()?, true),
        };
        let hexed = match array.data_type() {
            DataType::Float32 => {
                let values: &Float32Array = array.as_primitive::<Float32Type>();
                encode(&float_array_to_i64(values, self.ansi_mode)?)?
            }
            DataType::Float64 => {
                let values: &Float64Array = array.as_primitive::<Float64Type>();
                encode(&double_array_to_i64(values, self.ansi_mode)?)?
            }
            DataType::Decimal128(precision, scale) => {
                let values = array.as_primitive::<Decimal128Type>();
                encode(&decimal_array_to_i64(
                    values,
                    *precision,
                    *scale,
                    self.ansi_mode,
                )?)?
            }
            data_type if prints_as_text(data_type) => encode(&printed_text(&args, array)?)?,
            _ => encode(&array)?,
        };
        if is_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &hexed, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(hexed))
        }
    }
}

fn unexpected_input_type<T>(data_type: &DataType) -> Result<T> {
    plan_err!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"hex\" due to data type mismatch: The first parameter requires the (\"BIGINT\" or \"BINARY\" or \"STRING\") type, however the input has the type \"{data_type}\"."
    )
}

fn wrong_num_args<T>(actual: usize) -> Result<T> {
    plan_err!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `hex` requires 1 parameters but the actual number is {actual}. Please, refer to 'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix."
    )
}

/// Whether the implicit cast to BIGINT can produce a NULL of its own (`Cast.forceNullable`,
/// `Cast.scala:446`): fractional and decimal sources.
fn casts_to_bigint_with_null(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn encode(array: &ArrayRef) -> Result<ArrayRef> {
    match encode_hex(&[ColumnarValue::Array(Arc::clone(array))])? {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(value) => value.to_array(),
    }
}

fn float_array_to_i64(values: &Float32Array, ansi_mode: bool) -> Result<ArrayRef> {
    let out = values
        .iter()
        .map(|v| v.map(|v| float_to_i64(v, ansi_mode)).transpose())
        .collect::<Result<Int64Array>>()?;
    Ok(Arc::new(out))
}

fn double_array_to_i64(values: &Float64Array, ansi_mode: bool) -> Result<ArrayRef> {
    let out = values
        .iter()
        .map(|v| v.map(|v| double_to_i64(v, ansi_mode)).transpose())
        .collect::<Result<Int64Array>>()?;
    Ok(Arc::new(out))
}

/// Spark's DECIMAL -> BIGINT cast: truncate the fraction towards zero, then keep the low 64
/// bits when ANSI is off (`Decimal.toLong` wraps) or raise `CAST_OVERFLOW` when it is on.
pub(crate) fn decimal_array_to_i64(
    values: &Decimal128Array,
    precision: u8,
    scale: i8,
    ansi_mode: bool,
) -> Result<ArrayRef> {
    let divisor = 10_i128.checked_pow(u32::try_from(scale.max(0)).unwrap_or(0));
    let out = values
        .iter()
        .map(|v| {
            let Some(unscaled) = v else {
                return Ok(None);
            };
            let whole = match divisor {
                Some(divisor) if divisor != 0 => unscaled / divisor,
                _ => unscaled,
            };
            match i64::try_from(whole) {
                Ok(v) => Ok(Some(v)),
                Err(_) if ansi_mode => Err(cast_overflow_err(
                    &format!(
                        "{}BD",
                        Decimal128Type::format_decimal(unscaled, precision, scale)
                    ),
                    &format!("DECIMAL({precision},{scale})"),
                )),
                Err(_) => Ok(Some(whole as i64)),
            }
        })
        .collect::<Result<Int64Array>>()?;
    Ok(Arc::new(out))
}

/// Whether an array of this type has to be cast to STRING before it is hexed or unhexed: Spark
/// prints it, and the printed text is what is processed.
pub(crate) fn prints_as_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
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
            | DataType::Interval(_)
    ) || is_marked_variant_storage_type(data_type)
}

/// The text Spark's CAST to STRING prints, through the same UDF `CAST(x AS STRING)` uses
/// (`SparkToUtf8`, or `variant_to_text` for a VARIANT), so that the interval fields carried
/// in the argument's field metadata are honoured.
pub(crate) fn printed_text(args: &ScalarFunctionArgs, array: ArrayRef) -> Result<ArrayRef> {
    if is_marked_variant_storage_type(array.data_type()) {
        variant_to_text(&array, args.config_options.execution.time_zone.as_deref())
    } else {
        to_text(&SparkToUtf8::new(), DataType::Utf8, args, array)
    }
}

fn to_text(
    udf: &dyn ScalarUDFImpl,
    return_type: DataType,
    args: &ScalarFunctionArgs,
    array: ArrayRef,
) -> Result<ArrayRef> {
    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(array)],
        arg_fields: args.arg_fields[0..1].to_vec(),
        number_rows: args.number_rows,
        return_field: Arc::new(Field::new(udf.name(), return_type, true)),
        config_options: Arc::clone(&args.config_options),
    })?;
    match result {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(value) => value.to_array(),
    }
}
