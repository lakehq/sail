use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType, Field,
    FieldRef,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::math::negative::SparkNegative as DataFusionNegative;
use num::traits::CheckedNeg;

/// `ConfigOptions` snapshots with `execution.enable_ansi_mode` pinned. Negation
/// only reads the ANSI flag, so the two possible snapshots are built once and
/// shared process-wide; each batch clones an `Arc` instead of deep-cloning the
/// session config.
static ANSI_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| make_config(true));
static NON_ANSI_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| make_config(false));

fn make_config(ansi_mode: bool) -> Arc<ConfigOptions> {
    let mut config = ConfigOptions::default();
    config.execution.enable_ansi_mode = ansi_mode;
    Arc::new(config)
}

/// Spark unary minus / `negative(x)` that honors `spark.sql.ansi.enabled`.
///
/// The ANSI flag is captured at planning time (via the constructor) and
/// serialized through the physical codec, so the value the client requested
/// reaches every worker — unlike reading DataFusion's session-level
/// `execution.enable_ansi_mode`, which only reflects the driver's context.
///
/// Name, signature, return type, and the negate kernel are delegated to upstream
/// `datafusion_spark::SparkNegative` (its free `spark_negative` helper is private).
/// We drive the kernel by handing it the matching pinned-ANSI [`ConfigOptions`]
/// snapshot per batch.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNegative {
    inner: DataFusionNegative,
    ansi_mode: bool,
}

impl Default for SparkNegative {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkNegative {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            inner: DataFusionNegative::new(),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

/// Spark's [`UnaryMinus`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L46-L106)
/// applies Scala's 34-significant-digit `BigDecimal` context to expanded decimal
/// values, then reconstructs the original DECIMAL(p, s). This can round a value
/// within range, or round it into an out-of-range value such as `-1e38` for
/// DECIMAL(38, 0); Spark raises the latter in both ANSI modes. Upstream
/// DataFusion only negates Arrow's physical integer, so intercept decimal inputs
/// here before delegating all other types to its vectorized implementation.
fn decimal_overflow<T: DecimalType>(value: T::Native, precision: u8, scale: i8) -> ArrowError {
    ArrowError::ComputeError(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION] The {} cannot be represented as Decimal({precision}, {scale}). SQLSTATE: 22003",
        T::format_decimal(value, precision, scale),
    ))
}

/// `Decimal.unary_-` uses Scala `BigDecimal`, whose DECIMAL128 context keeps 34
/// significant digits. Arrow stores the unscaled integer exactly, therefore we
/// reproduce that context explicitly before validating the declared precision.
fn spark_decimal128_negate(value: i128, precision: u8, scale: i8) -> Result<i128> {
    let negated = value
        .checked_neg()
        .ok_or_else(|| decimal_overflow::<Decimal128Type>(value, precision, scale))?;
    let absolute = negated
        .checked_abs()
        .ok_or_else(|| decimal_overflow::<Decimal128Type>(negated, precision, scale))?;
    let divisor = if absolute < 10_i128.pow(34) {
        1
    } else if absolute < 10_i128.pow(35) {
        10
    } else if absolute < 10_i128.pow(36) {
        100
    } else if absolute < 10_i128.pow(37) {
        1_000
    } else {
        10_000
    };
    let quotient = negated / divisor;
    let remainder = negated % divisor;
    let doubled_remainder = remainder.abs() * 2;
    let rounded =
        if doubled_remainder > divisor || (doubled_remainder == divisor && quotient % 2 != 0) {
            quotient + negated.signum()
        } else {
            quotient
        }
        .checked_mul(divisor)
        .ok_or_else(|| decimal_overflow::<Decimal128Type>(negated, precision, scale))?;
    Decimal128Type::validate_decimal_precision(rounded, precision, scale)
        .map_err(|_| decimal_overflow::<Decimal128Type>(rounded, precision, scale))?;
    Ok(rounded)
}

fn negate_decimal128_array(array: &ArrayRef, precision: u8, scale: i8) -> Result<ColumnarValue> {
    let array = array.as_primitive::<Decimal128Type>();
    let data_type = array.data_type().clone();
    let result: PrimitiveArray<Decimal128Type> = array
        .try_unary(|value| spark_decimal128_negate(value, precision, scale))?
        .with_data_type(data_type);
    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Spark implements `Decimal.abs` as `unary_-` only for negative values
/// (`Decimal.scala:543-551`). Reuse the same DECIMAL128 context as negation so
/// `abs(-999…9)` rounds exactly as Spark does before checking the target precision.
pub(crate) fn spark_decimal128_abs(arg: &ColumnarValue) -> Result<Option<ColumnarValue>> {
    let result = match arg {
        ColumnarValue::Array(array) => {
            let DataType::Decimal128(precision, scale) = array.data_type() else {
                return Ok(None);
            };
            let data_type = array.data_type().clone();
            let result: PrimitiveArray<Decimal128Type> = array
                .as_primitive::<Decimal128Type>()
                .try_unary(|value| {
                    if value < 0 {
                        spark_decimal128_negate(value, *precision, *scale)
                    } else {
                        Ok(value)
                    }
                })?
                .with_data_type(data_type);
            ColumnarValue::Array(Arc::new(result))
        }
        ColumnarValue::Scalar(ScalarValue::Decimal128(value, precision, scale)) => {
            let value = value
                .map(|value| {
                    if value < 0 {
                        spark_decimal128_negate(value, *precision, *scale)
                    } else {
                        Ok(value)
                    }
                })
                .transpose()?;
            ColumnarValue::Scalar(ScalarValue::Decimal128(value, *precision, *scale))
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

fn negate_decimal_array<T>(array: &ArrayRef, precision: u8, scale: i8) -> Result<ColumnarValue>
where
    T: DecimalType,
    T::Native: CheckedNeg,
{
    let array = array.as_primitive::<T>();
    let data_type = array.data_type().clone();
    let result: PrimitiveArray<T> = array
        .try_unary(|value| {
            let negated = value
                .checked_neg()
                .ok_or_else(|| decimal_overflow::<T>(value, precision, scale))?;
            T::validate_decimal_precision(negated, precision, scale)
                .map_err(|_| decimal_overflow::<T>(negated, precision, scale))?;
            Ok::<T::Native, ArrowError>(negated)
        })?
        .with_data_type(data_type);
    Ok(ColumnarValue::Array(Arc::new(result)))
}

macro_rules! negate_decimal_scalar {
    ($value:expr, $precision:expr, $scale:expr, $type:ty, $variant:ident) => {{
        let value = *$value;
        let precision = *$precision;
        let scale = *$scale;
        let negated = value
            .checked_neg()
            .ok_or_else(|| decimal_overflow::<$type>(value, precision, scale))?;
        <$type>::validate_decimal_precision(negated, precision, scale)
            .map_err(|_| decimal_overflow::<$type>(negated, precision, scale))?;
        Ok::<ColumnarValue, ArrowError>(ColumnarValue::Scalar(ScalarValue::$variant(
            Some(negated),
            precision,
            scale,
        )))
    }};
}

fn negate_decimal(arg: &ColumnarValue) -> Result<Option<ColumnarValue>> {
    let result = match arg {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Decimal32(precision, scale) => {
                negate_decimal_array::<Decimal32Type>(array, *precision, *scale)?
            }
            DataType::Decimal64(precision, scale) => {
                negate_decimal_array::<Decimal64Type>(array, *precision, *scale)?
            }
            DataType::Decimal128(precision, scale) => {
                negate_decimal128_array(array, *precision, *scale)?
            }
            DataType::Decimal256(precision, scale) => {
                negate_decimal_array::<Decimal256Type>(array, *precision, *scale)?
            }
            _ => return Ok(None),
        },
        ColumnarValue::Scalar(ScalarValue::Decimal32(Some(value), precision, scale)) => {
            negate_decimal_scalar!(value, precision, scale, Decimal32Type, Decimal32)?
        }
        ColumnarValue::Scalar(ScalarValue::Decimal64(Some(value), precision, scale)) => {
            negate_decimal_scalar!(value, precision, scale, Decimal64Type, Decimal64)?
        }
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), precision, scale)) => {
            let value = spark_decimal128_negate(*value, *precision, *scale)?;
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), *precision, *scale))
        }
        ColumnarValue::Scalar(ScalarValue::Decimal256(Some(value), precision, scale)) => {
            negate_decimal_scalar!(value, precision, scale, Decimal256Type, Decimal256)?
        }
        ColumnarValue::Scalar(value) if value.is_null() => return Ok(None),
        ColumnarValue::Scalar(_) => return Ok(None),
    };
    Ok(Some(result))
}

impl ScalarUDFImpl for SparkNegative {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let _ = arg_types;
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark's `UnaryMinus` is `nullIntolerant`, so it is nullable precisely when its child is
    /// nullable (`arithmetic.scala:50`).
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let argument = args.arg_fields.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "negative expects one argument".to_string(),
            )
        })?;
        Ok(Arc::new(Field::new(
            self.name(),
            argument.data_type().clone(),
            argument.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [argument] = args.args.as_slice()
            && let Some(result) = negate_decimal(argument)?
        {
            return Ok(result);
        }
        let config_options = if self.ansi_mode {
            Arc::clone(&ANSI_CONFIG)
        } else {
            Arc::clone(&NON_ANSI_CONFIG)
        };
        let args = ScalarFunctionArgs {
            config_options,
            ..args
        };
        self.inner.invoke_with_args(args)
    }
}
