use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions::math::round::RoundFunc;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use crate::scalar::math::utils::decimal::round_decimal_base;

/// Spark `round(expr [, scale])`, which types a decimal result differently from
/// DataFusion.
///
/// DataFusion keeps the input precision and only lowers the scale, so
/// `round(decimal(23,13), 2)` is `decimal(23,2)`. Spark re-derives the precision from
/// the integral digits — `(p - s + 1) + min(s, scale)` — giving `decimal(13,2)`. That
/// is `RoundBase.dataType`, which Sail already implements as [`round_decimal_base`]
/// (wired into `ceil`/`floor`); only `round` was left on DataFusion's type.
///
/// Only the declared type changes: DataFusion's kernel reads the output precision and
/// scale back from `args.return_type()`, so it stamps Spark's type onto the result
/// directly and no cast is needed. Everything else delegates to the same `RoundFunc`,
/// so this costs no extra work per batch.
///
/// Non-decimal inputs are untouched: Spark's `Round.dataType` ends in `case t => t`, so
/// a DOUBLE stays DOUBLE (unlike `ceil`/`floor`, which map every numeric to decimal).
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1503-L1520>
#[derive(Debug)]
pub struct SparkRound {
    inner: RoundFunc,
}

impl Default for SparkRound {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRound {
    pub fn new() -> Self {
        Self {
            inner: RoundFunc::new(),
        }
    }
}

impl PartialEq for SparkRound {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRound {}

impl std::hash::Hash for SparkRound {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

/// The rounding scale Spark takes from the (foldable) second argument, defaulting to 0.
/// `None` means it is not a literal, in which case no per-row scale can be baked into
/// the type and DataFusion's own rule is left in place.
fn target_scale(args: &ReturnFieldArgs) -> Option<i32> {
    match args.scalar_arguments.get(1) {
        None => Some(0),
        Some(None) => None,
        Some(Some(scalar)) if scalar.is_null() => Some(0),
        Some(Some(scalar)) => match scalar {
            ScalarValue::Int8(Some(v)) => Some(i32::from(*v)),
            ScalarValue::Int16(Some(v)) => Some(i32::from(*v)),
            ScalarValue::Int32(Some(v)) => Some(*v),
            ScalarValue::Int64(Some(v)) => i32::try_from(*v).ok(),
            _ => None,
        },
    }
}

impl ScalarUDFImpl for SparkRound {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let Some(input_field) = args.arg_fields.first() else {
            return self.inner.return_field_from_args(args);
        };
        let (DataType::Decimal128(precision, scale), Some(target)) =
            (input_field.data_type(), target_scale(&args))
        else {
            return self.inner.return_field_from_args(args);
        };
        let (precision, scale) =
            round_decimal_base(i32::from(*precision), i32::from(*scale), target, true);
        // Spark's `Round` is always nullable: rounding a decimal yields NULL when the
        // value no longer fits the result precision.
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(precision, scale),
            true,
        )))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
}
