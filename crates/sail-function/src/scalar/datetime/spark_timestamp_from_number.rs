use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::cast as cast_array;
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit, i256};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext, SimplifyContextBuilder};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, cast, col, lit, try_cast, when,
};
use datafusion_functions::math::expr_fn::trunc;
use datafusion_physical_expr::create_physical_expr;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::misc::raise_error::RaiseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampNumberMode {
    Seconds,
    Millis,
    Micros,
    CastAnsi,
    CastLegacy,
    TryCast,
}

// Native expressions handle casts and narrow inputs; wide constructors retain checked scaling.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampFromNumber {
    signature: Signature,
    mode: TimestampNumberMode,
}

impl SparkTimestampFromNumber {
    pub fn new(mode: TimestampNumberMode) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            mode,
        }
    }

    /// Build the native expression before it can acquire a UDF column name.
    pub fn expression(&self, value: Expr, schema: &DFSchemaRef) -> Result<Expr> {
        let info = SimplifyContextBuilder::default()
            .with_schema(Arc::clone(schema))
            .build();
        let data_type = info.get_data_type(&value)?;
        let repeats_value = match (&data_type, self.mode) {
            (_, TimestampNumberMode::Micros | TimestampNumberMode::Millis) => false,
            (DataType::Float32 | DataType::Float64, _) => true,
            (DataType::Decimal128(_, scale), TimestampNumberMode::Seconds) => *scale > 6,
            (DataType::Decimal128(precision, scale), _) => {
                i16::from(*precision) - i16::from(*scale) + 6 > 18
            }
            (_, TimestampNumberMode::Seconds) => false,
            (DataType::Int64 | DataType::UInt64, _) => true,
            _ => false,
        };
        if repeats_value && value.is_volatile() {
            // A UDF binds the input once before range and precision checks reuse it.
            return Ok(ScalarUDF::from(Self::new(self.mode)).call(vec![value]));
        }
        let micros = match self.mode {
            TimestampNumberMode::Micros => value,
            TimestampNumberMode::Millis => self.checked_scale(value, &data_type, 1_000),
            _ => match data_type {
                DataType::Float32 | DataType::Float64 => self.floating_micros(value)?,
                DataType::Decimal128(precision, scale) => {
                    self.decimal_micros(value, precision, scale)?
                }
                DataType::Decimal256(_, _) => value * lit(1_000_000_i64),
                _ if data_type.is_integer() || data_type.is_null() => {
                    self.integer_micros(value, &data_type)?
                }
                _ if self.mode == TimestampNumberMode::Seconds => {
                    self.checked_scale(value, &data_type, 1_000_000)
                }
                _ => cast(value, DataType::Int64) * lit(1_000_000_i64),
            },
        };
        let expression = cast(micros, self.return_type(&[])?);
        ExprSimplifier::new(info).coerce(expression, schema)
    }

    fn checked_scale(&self, value: Expr, data_type: &DataType, factor: i64) -> Expr {
        let value = cast(value, DataType::Int64);
        if matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
        ) {
            return value * lit(factor);
        }
        // A UDF defers overflow in unused CASE arms; a checked literal cast fails during folding.
        cast(
            ScalarUDF::from(Self::new(self.mode)).call(vec![value]),
            DataType::Int64,
        )
    }

    fn integer_micros(&self, value: Expr, data_type: &DataType) -> Result<Expr> {
        if self.mode == TimestampNumberMode::Seconds {
            return Ok(self.checked_scale(value, data_type, 1_000_000));
        }
        let value = cast(value, DataType::Int64);
        let scaled = value.clone() * lit(1_000_000_i64);
        if matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
        ) {
            return Ok(scaled);
        }
        // Java TimeUnit.SECONDS.toMicros saturates instead of overflowing.
        // Single-branch cases avoid DataFusion's general CASE row-index allocation.
        when(value.clone().gt(lit(i64::MAX / 1_000_000)), lit(i64::MAX))
            .otherwise(when(value.lt(lit(i64::MIN / 1_000_000)), lit(i64::MIN)).otherwise(scaled)?)
    }

    fn floating_micros(&self, value: Expr) -> Result<Expr> {
        let value = cast(value, DataType::Float64);
        let scaled = value.clone() * lit(1_000_000_f64);
        let finite = value
            .clone()
            .gt(lit(f64::NEG_INFINITY))
            .and(value.clone().lt(lit(f64::INFINITY)));
        let in_range = scaled
            .clone()
            .gt_eq(lit(i64::MIN as f64))
            .and(scaled.clone().lt_eq(lit(i64::MAX as f64)));
        let bounded = when(scaled.clone().gt_eq(lit(i64::MAX as f64)), lit(i64::MAX))
            .when(scaled.clone().lt_eq(lit(i64::MIN as f64)), lit(i64::MIN))
            // An unused CASE arm can be constant-folded before its condition.
            .otherwise(try_cast(scaled, DataType::Int64))?;
        match self.mode {
            TimestampNumberMode::CastAnsi => {
                let message = when(
                    finite,
                    lit("cannot be cast to \"BIGINT\" due to an overflow"),
                )
                .otherwise(lit(
                    "cannot be cast to \"TIMESTAMP\" because it is malformed",
                ))?;
                when(value.is_null(), lit(ScalarValue::Int64(None)))
                    .when(in_range, bounded)
                    .otherwise(cast(
                        ScalarUDF::from(RaiseError::new()).call(vec![message]),
                        DataType::Int64,
                    ))
            }
            TimestampNumberMode::TryCast => {
                when(in_range, bounded).otherwise(lit(ScalarValue::Int64(None)))
            }
            _ => when(finite, bounded).otherwise(lit(ScalarValue::Int64(None))),
        }
    }

    fn decimal_micros(&self, value: Expr, precision: u8, scale: i8) -> Result<Expr> {
        let wide = cast(value, DataType::Decimal256(precision, scale));
        let scaled = wide * lit(ScalarValue::Decimal256(Some(i256::from(1_000_000)), 7, 0));
        // Decimal rescaling rounds; truncate the fractional part before rescaling.
        let integer = cast(trunc(vec![scaled.clone()]), DataType::Decimal256(76, 0));
        if self.mode == TimestampNumberMode::Seconds {
            let result = cast(scaled.clone(), DataType::Int64);
            return if scale <= 6 {
                Ok(result)
            } else {
                // Spark's longValueExact rejects fractional microseconds as well as overflow.
                when(
                    scaled.clone().is_not_null().and(scaled.not_eq(integer)),
                    cast(
                        ScalarUDF::from(RaiseError::new()).call(vec![lit(
                            "timestamp_seconds decimal overflow or sub-microsecond precision",
                        )]),
                        DataType::Int64,
                    ),
                )
                .otherwise(result)
            };
        }
        if i16::from(precision) - i16::from(scale) + 6 <= 18 {
            return Ok(cast(scaled, DataType::Int64));
        }
        // BigDecimal.longValue truncates and wraps modulo 2^64.
        let modulus = lit(ScalarValue::Decimal256(
            Some(i256::from_i128(1_i128 << 64)),
            20,
            0,
        ));
        let wrapped = integer % modulus.clone();
        Ok(cast(
            when(
                wrapped.clone().gt(lit(i64::MAX)),
                wrapped.clone() - modulus.clone(),
            )
            .when(wrapped.clone().lt(lit(i64::MIN)), wrapped.clone() + modulus)
            .otherwise(wrapped)?,
            DataType::Int64,
        ))
    }
}

impl ScalarUDFImpl for SparkTimestampFromNumber {
    fn name(&self) -> &str {
        match self.mode {
            TimestampNumberMode::Seconds => "spark_timestamp_seconds",
            TimestampNumberMode::Millis => "spark_timestamp_millis",
            TimestampNumberMode::Micros => "spark_timestamp_micros",
            TimestampNumberMode::CastAnsi => "spark_cast_number_to_timestamp",
            TimestampNumberMode::CastLegacy => "spark_cast_number_to_timestamp_legacy",
            TimestampNumberMode::TryCast => "spark_try_cast_number_to_timestamp",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| {
            field.is_nullable()
                || (matches!(field.data_type(), DataType::Float32 | DataType::Float64)
                    && self.mode != TimestampNumberMode::CastAnsi)
        });
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[])?,
            nullable,
        )))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_type = self.return_type(&[])?;
        make_scalar_function(|values| {
            let [value] = values else {
                return internal_err!("numeric timestamp conversion requires one argument");
            };
            if value.data_type() == &DataType::Int64
                && matches!(
                    self.mode,
                    TimestampNumberMode::Seconds | TimestampNumberMode::Millis
                )
            {
                let factor = if self.mode == TimestampNumberMode::Seconds {
                    1_000_000
                } else {
                    1_000
                };
                let micros = numeric::mul(value, &Int64Array::new_scalar(factor))?;
                return Ok(cast_array(&micros, &data_type)?);
            }
            // Reuse the same conversion with an already-evaluated column as its input.
            let schema = Arc::new(DFSchema::from_unqualified_fields(
                vec![Field::new("value", value.data_type().clone(), true)].into(),
                Default::default(),
            )?);
            let expression = self.expression(col("value"), &schema)?;
            create_physical_expr(
                &expression,
                &schema,
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )?
            .evaluate(&RecordBatch::try_new(
                Arc::clone(schema.inner()),
                vec![Arc::clone(value)],
            )?)?
            .into_array(value.len())
        })(&args.args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args.iter().any(Expr::is_volatile) {
            return Ok(ExprSimplifyResult::Original(args));
        }
        if matches!(
            self.mode,
            TimestampNumberMode::Seconds | TimestampNumberMode::Millis
        ) && let [value] = args.as_slice()
            && info.get_data_type(value)? == DataType::Int64
        {
            return Ok(ExprSimplifyResult::Original(args));
        }
        Ok(ExprSimplifyResult::Simplified(
            self.expression(args.one()?, info.schema())?,
        ))
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 1 {
            return exec_err!("numeric timestamp conversion requires one argument");
        }
        // Preserve Sail's castable constructor inputs and wide decimal extension.
        Ok(types.to_vec())
    }
}
