use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StructArray, make_array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Field, FieldRef, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, IntervalUnit, IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type, i256,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use half::f16;

use crate::aggregate::quantile_summaries::{QuantileSummaries, Stats};
use crate::aggregate::utils::{cast_to_type, evaluate_percentile_literal, scalar_to_f64};
use crate::error::invalid_arg_count_exec_err;

/// Spark's `ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY`.
const DEFAULT_PERCENTILE_ACCURACY: i64 = 10_000;

/// Spark-compatible `approx_percentile` / `percentile_approx` aggregate.
///
/// ```text
/// percentile_approx(col, percentage, accuracy)
/// ```
///
/// Unlike DataFusion's `approx_percentile_cont`, this matches Spark semantics:
///
/// - `percentage` may be a single number in `[0.0, 1.0]` (returning a scalar) or
///   an array of such numbers (returning `array<input_type>`).
/// - The result preserves the input type (e.g. `int -> int`,
///   `decimal(10,2) -> decimal(10,2)`, `date -> date`).
/// - Values are accumulated in a Greenwald-Khanna sketch
///   ([`QuantileSummaries`]) whose relative error is `1 / accuracy`, so memory
///   stays bounded, and the `relativeError` short-circuits that return the
///   minimum and the maximum behave as they do in Spark.
///
/// Like Spark, every input is widened to `f64` on the way in and narrowed back
/// to the input type on the way out, so wide `bigint` and `decimal` inputs lose
/// precision exactly as they do in Spark.
///
/// Results are identical to Spark's whenever the sketch is lossless (small
/// inputs, or an `accuracy` high enough that nothing is merged). Once the
/// sketch actually compresses, the selected quantile depends on the order and
/// partitioning of the input, so Sail and Spark agree only within the
/// `1 / accuracy` bound. That is inherent to the algorithm rather than a gap:
/// Spark does not agree with itself either, and returns a different quantile
/// for the same query at a different input partitioning.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkApproxPercentile {
    signature: Signature,
}

impl Default for SparkApproxPercentile {
    fn default() -> Self {
        Self::new()
    }
}

/// The requested percentiles and the accuracy driving the sketch's relative
/// error.
struct ResolvedArgs {
    percentiles: Vec<f64>,
    accuracy: i64,
}

impl SparkApproxPercentile {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn resolve_args(exprs: &[Arc<dyn PhysicalExpr>], is_array: bool) -> Result<ResolvedArgs> {
        let percentage_expr = exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "approx_percentile requires a percentage argument".to_string(),
            )
        })?;

        // `coerce_types` has already normalized the percentage to `Float64` or
        // `List<Float64>`, so the shape is known: extracting the wrong one would
        // replace a precise error with a misleading one.
        let percentiles = match evaluate_percentile_literal(percentage_expr)? {
            ScalarValue::List(array) if is_array => {
                let values = array.values();
                (0..values.len())
                    .map(|index| {
                        // Spark reads the array with `ArrayData.toDoubleArray`
                        // (`ApproximatePercentile.scala:118`), which reads every
                        // slot with `getDouble` and no null check
                        // (`ArrayData.scala:154-162`), so a NULL is 0.0. See
                        // `coerce_types` for why the NULL gets this far.
                        if values.is_null(index) {
                            Ok(0.0)
                        } else {
                            scalar_to_f64(&ScalarValue::try_from_array(values.as_ref(), index)?)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            scalar => vec![scalar_to_f64(&scalar)?],
        };
        for percentile in &percentiles {
            if !(0.0..=1.0).contains(percentile) {
                return Err(DataFusionError::Execution(format!(
                    "The percentage must be between [0.0, 1.0], but got {percentile}"
                )));
            }
        }

        let accuracy = match exprs.get(2) {
            Some(expr) => scalar_to_f64(&evaluate_percentile_literal(expr)?)? as i64,
            None => DEFAULT_PERCENTILE_ACCURACY,
        };
        if accuracy <= 0 || accuracy > i32::MAX as i64 {
            return Err(DataFusionError::Execution(format!(
                "The accuracy must be between (0, 2147483647], but got {accuracy}"
            )));
        }

        Ok(ResolvedArgs {
            percentiles,
            accuracy,
        })
    }

    /// The element type of the state's `sampled` list, mirroring Spark's
    /// serialized `Stats(value, g, delta)`.
    fn stats_fields() -> Vec<FieldRef> {
        vec![
            Field::new("value", DataType::Float64, false).into(),
            Field::new("g", DataType::Int64, false).into(),
            Field::new("delta", DataType::Int64, false).into(),
        ]
    }
}

impl AggregateUDFImpl for SparkApproxPercentile {
    fn name(&self) -> &str {
        "approx_percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(invalid_arg_count_exec_err(
                "percentile_approx",
                (2, 3),
                arg_types.len(),
            ));
        }
        // Spark accepts NumericType, DateType, TimestampType, TimestampNTZType
        // and both ANSI interval types, since all of them are numeric
        // internally. The argument is also `ImplicitCastInputTypes`, so a STRING
        // or an untyped NULL is CAST rather than rejected, and the result type
        // is then DOUBLE: `implicitCast` walks the collection
        // (`TypeCoercion.scala:240`), `(StringType, NumericType)` yields
        // `NumericType.defaultConcreteType` = DOUBLE (`:212`,
        // `AbstractDataType.scala:131`), and `(NullType, target)` yields the
        // collection's own default, also DOUBLE (`:202`, `:66`).
        let input = match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                DataType::Float64
            }
            other
                if other.is_numeric()
                    || matches!(
                        other,
                        DataType::Date32
                            | DataType::Date64
                            | DataType::Timestamp(_, _)
                            | DataType::Interval(IntervalUnit::YearMonth)
                            | DataType::Duration(_)
                    ) =>
            {
                other.clone()
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "percentile_approx requires a numeric, date, timestamp or interval input type, got {other}"
                )));
            }
        };
        // The percentage is a single Float64 or an array of Float64.
        //
        // The element is declared NULLABLE even though Spark's `inputTypes` says
        // `ArrayType(DoubleType, containsNull = false)`
        // (`ApproximatePercentile.scala:109`), because Spark never enforces that
        // half of the declaration:
        //
        // - `implicitCast` REFUSES to convert a nullable array to a
        //   non-nullable one — `case (ArrayType(_, true), ArrayType(_, false))
        //   => null` (`TypeCoercion.scala:258`) — so no cast is inserted.
        // - The type check passes anyway: `acceptsType` is `sameType`
        //   (`DataType.scala:117`), which is `equalsIgnoreNullability`
        //   (`:90`), and that drops `containsNull` for arrays (`:569`).
        // - So the NULL reaches `eval` intact, and `ArrayData.toDoubleArray`
        //   (`ArrayData.scala:154-162`) reads every slot with `getDouble` and
        //   no null check, yielding 0.0.
        //
        // Arrow's cast does enforce the flag, so declaring the element
        // non-nullable here would reject `array(0.5, CAST(NULL AS DOUBLE))`
        // outright — a query Spark answers. `resolve_args` maps the NULL to 0.0.
        let percentage = match &arg_types[1] {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
            }
            _ => DataType::Float64,
        };
        let mut coerced = vec![input, percentage];
        if let Some(accuracy) = arg_types.get(2) {
            // Spark declares `accuracy` as `IntegralType`, and `implicitCast`
            // reaches it ONLY when the argument is already integral (`:199`) or
            // an untyped NULL (`:202` -> `IntegerType`,
            // `AbstractDataType.scala:140`). Nothing takes a STRING, a
            // fractional or a BOOLEAN there: `:212`, `:220` and `:228` all
            // target the `NumericType` CLASS, and the `IntegralType` object is
            // not an instance of it. So those fail analysis whatever their
            // value — `accuracy` is a rule about the TYPE, which is why an
            // integral-valued `DECIMAL(10,0)` is rejected too.
            if !(accuracy.is_integer() || matches!(accuracy, DataType::Null)) {
                return Err(DataFusionError::Plan(format!(
                    "The third parameter requires the \"INTEGRAL\" type, however it has the type \"{accuracy}\"."
                )));
            }
            // Read as Int64 so values above `i32::MAX` are rejected rather than
            // wrapping, matching Spark's `accuracy > Int.MaxValue` check.
            coerced.push(DataType::Int64);
        }
        Ok(coerced)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // `coerce_types` has already validated the input type and rewritten the
        // percentage argument to Float64 (scalar) or List<Float64> (array).
        // When the percentage is an array, the result is an array of percentiles
        // whose element type matches the input.
        if matches!(arg_types.get(1), Some(DataType::List(_))) {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                arg_types[0].clone(),
                element_is_nullable(&arg_types[0]),
            ))))
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // `return_field` already encodes both facts this needs: the array form
        // is exactly the one whose return type is a list, and its element type
        // is the (coerced) input type.
        let (is_array, data_type) = match args.return_field.data_type() {
            DataType::List(field) => (true, field.data_type().clone()),
            other => (false, other.clone()),
        };
        let ResolvedArgs {
            percentiles,
            accuracy,
        } = Self::resolve_args(args.exprs, is_array)?;

        Ok(Box::new(ApproxPercentileAccumulator {
            data_type,
            summaries: QuantileSummaries::new(1.0 / accuracy as f64),
            percentiles,
            is_array,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let stats = DataType::Struct(Self::stats_fields().into());
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count"),
                DataType::Int64,
                false,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "sampled"),
                DataType::List(Arc::new(Field::new_list_field(stats, false))),
                false,
            )
            .into(),
        ])
    }
}

/// Accumulator backing [`SparkApproxPercentile`].
///
/// Mirrors Spark's `PercentileDigest`: observations are inserted into the
/// sketch as `f64`, the sketch is compressed before it is read or merged, and
/// the queried quantiles are narrowed back to the input type.
struct ApproxPercentileAccumulator {
    data_type: DataType,
    summaries: QuantileSummaries,
    percentiles: Vec<f64>,
    is_array: bool,
}

impl Debug for ApproxPercentileAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ApproxPercentileAccumulator({}, percentiles={:?})",
            self.data_type, self.percentiles
        )
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Spark's `PercentileDigest.quantileSummaries`: compress lazily, so the
        // sketch is readable without discarding anything.
        if !self.summaries.is_compressed() {
            self.summaries.compress();
        }
        let count = self.summaries.count();
        let sampled = self.summaries.sampled();

        let values = Float64Array::from_iter_values(sampled.iter().map(|s| s.value));
        let g = Int64Array::from_iter_values(sampled.iter().map(|s| s.g));
        let delta = Int64Array::from_iter_values(sampled.iter().map(|s| s.delta));
        let stats = StructArray::new(
            SparkApproxPercentile::stats_fields().into(),
            vec![Arc::new(values), Arc::new(g), Arc::new(delta)],
            None,
        );

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            i32::try_from(stats.len()).map_err(|_| {
                DataFusionError::Execution(
                    "approx_percentile sketch exceeded the maximum list length".to_string(),
                )
            })?,
        ]));
        let list = ListArray::new(
            Arc::new(Field::new_list_field(
                DataType::Struct(SparkApproxPercentile::stats_fields().into()),
                false,
            )),
            offsets,
            Arc::new(stats),
            None,
        );

        Ok(vec![
            ScalarValue::Int64(Some(count)),
            ScalarValue::List(Arc::new(list)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(input) = values.first() else {
            return Ok(());
        };
        // Decimals are widened exactly; everything else goes through the cast.
        match input.data_type() {
            DataType::Decimal32(_, scale) => {
                let scale = *scale;
                for unscaled in input.as_primitive::<Decimal32Type>().iter().flatten() {
                    self.summaries
                        .insert(decimal_to_f64(i128::from(unscaled), scale)?);
                }
            }
            DataType::Decimal64(_, scale) => {
                let scale = *scale;
                for unscaled in input.as_primitive::<Decimal64Type>().iter().flatten() {
                    self.summaries
                        .insert(decimal_to_f64(i128::from(unscaled), scale)?);
                }
            }
            DataType::Decimal128(_, scale) => {
                let scale = *scale;
                for unscaled in input.as_primitive::<Decimal128Type>().iter().flatten() {
                    self.summaries.insert(decimal_to_f64(unscaled, scale)?);
                }
            }
            DataType::Decimal256(_, scale) => {
                let scale = *scale;
                for unscaled in input.as_primitive::<Decimal256Type>().iter().flatten() {
                    self.summaries.insert(unscaled_to_f64(unscaled, scale)?);
                }
            }
            _ => {
                let doubles = spark_internal_as_f64(input)?;
                for value in doubles.as_primitive::<Float64Type>().iter().flatten() {
                    self.summaries.insert(value);
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let (Some(counts), Some(sampled)) = (states.first(), states.get(1)) else {
            return Ok(());
        };
        let counts = counts.as_primitive::<Int64Type>();
        let sampled = sampled.as_list::<i32>();

        // Spark compresses the receiving digest before merging.
        if !self.summaries.is_compressed() {
            self.summaries.compress();
        }
        // Every accumulator in a query shares the same foldable `accuracy`, so
        // the peer summary is rebuilt with our own relative error.
        let relative_error = self.summaries.relative_error();
        for index in 0..sampled.len() {
            if sampled.is_null(index) {
                continue;
            }
            let entry = sampled.value(index);
            let Some(entry) = entry.as_any().downcast_ref::<StructArray>() else {
                return Err(DataFusionError::Execution(
                    "approx_percentile state is not a struct list".to_string(),
                ));
            };
            let (Some(values), Some(g), Some(delta)) = (
                entry.column_by_name("value"),
                entry.column_by_name("g"),
                entry.column_by_name("delta"),
            ) else {
                return Err(DataFusionError::Execution(
                    "approx_percentile state is missing a sketch column".to_string(),
                ));
            };
            let values = values.as_primitive::<Float64Type>();
            let g = g.as_primitive::<Int64Type>();
            let delta = delta.as_primitive::<Int64Type>();

            let stats = (0..entry.len())
                .map(|i| Stats {
                    value: values.value(i),
                    g: g.value(i),
                    delta: delta.value(i),
                })
                .collect();
            let other = QuantileSummaries::from_parts(relative_error, stats, counts.value(index));
            self.summaries.merge_with(&other);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Reads the sketch without consuming it: DataFusion calls `evaluate`
        // once per row on a shared accumulator for window frames.
        if !self.summaries.is_compressed() {
            self.summaries.compress();
        }
        let data_type = &self.data_type;
        let queried = self.summaries.query(&self.percentiles);

        if self.is_array {
            let element_field =
                Field::new_list_field(data_type.clone(), element_is_nullable(data_type));
            let element_type = DataType::List(Arc::new(element_field.clone()));
            // Spark collapses BOTH an empty result set and an empty percentage
            // array to NULL: `if (result.length == 0) null`
            // (ApproximatePercentile.scala:219-220).
            let Some(values) = queried.filter(|values| !values.is_empty()) else {
                return ScalarValue::try_from(&element_type);
            };
            let scalars = values
                .into_iter()
                .map(|value| f64_to_input_type(value, data_type))
                .collect::<Result<Vec<_>>>()?;
            let values_array = ScalarValue::iter_to_array(scalars)?;
            let offsets =
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values_array.len() as i32]));
            let list_array = ListArray::new(Arc::new(element_field), offsets, values_array, None);
            Ok(ScalarValue::List(Arc::new(list_array)))
        } else {
            match queried.as_ref().and_then(|values| values.first()) {
                Some(value) => f64_to_input_type(*value, data_type),
                None => ScalarValue::try_from(data_type),
            }
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.summaries.allocated_size()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

/// Widens one decimal to `f64` the way Spark's `Decimal.toDouble` does —
/// `toBigDecimal.doubleValue`, a single correctly-rounded step from the exact
/// value (`Decimal.scala:245`).
///
/// Arrow's decimal cast instead computes `unscaled as f64 / 10^scale`, which
/// rounds twice and drifts: `CAST(123456789012345678.90 AS DECIMAL(38,2))`
/// widens to `1.2345678901234566e17` rather than `…68e17`, and the error then
/// propagates all the way to the returned quantile.
///
/// Fixed upstream in <https://github.com/apache/arrow-rs/pull/10509>; this can
/// go once Sail picks up an arrow release that carries it, at which point
/// `spark_internal_as_f64`'s cast handles decimals correctly on its own.
fn unscaled_to_f64<T: std::fmt::Display>(unscaled: T, scale: i8) -> Result<f64> {
    // Rust's float parser is correctly rounded, so routing the exact decimal
    // through scientific notation gives the same single rounding as BigDecimal.
    let rendered = format!("{unscaled}e{}", -(scale as i32));
    rendered.parse::<f64>().map_err(|_| {
        DataFusionError::Execution(format!("cannot widen decimal {rendered} to a double"))
    })
}

/// The powers of ten that are exactly representable as an `f64`.
const EXACT_POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// [`unscaled_to_f64`] for the decimals whose unscaled value fits an `i128`,
/// skipping the render-and-parse round trip when the division is already exact.
///
/// When both the unscaled value and `10^scale` are exactly representable, the
/// quotient is a single correctly-rounded IEEE step — the same value the parser
/// returns, without allocating per row.
fn decimal_to_f64(unscaled: i128, scale: i8) -> Result<f64> {
    if let Some(pow10) = usize::try_from(scale).ok().and_then(|s| EXACT_POW10.get(s))
        && unscaled.unsigned_abs() <= 1u128 << f64::MANTISSA_DIGITS
    {
        return Ok(unscaled as f64 / pow10);
    }
    unscaled_to_f64(unscaled, scale)
}

/// Widens an input array to `f64` using the same internal representation Spark
/// reads in `ApproximatePercentile.update`: dates and year-month intervals are
/// their `i32` backing value, timestamps and day-time intervals their `i64`.
fn spark_internal_as_f64(array: &ArrayRef) -> Result<ArrayRef> {
    let backing = match array.data_type() {
        DataType::Date32 | DataType::Interval(IntervalUnit::YearMonth) => Some(DataType::Int32),
        DataType::Date64 | DataType::Timestamp(_, _) | DataType::Duration(_) => {
            Some(DataType::Int64)
        }
        _ => None,
    };
    let array = match backing {
        // Zero-copy reinterpretation: same buffers, same width, numeric view.
        Some(data_type) => {
            let data = array
                .to_data()
                .into_builder()
                .data_type(data_type)
                .build()?;
            make_array(data)
        }
        None => Arc::clone(array),
    };
    cast_to_type(&array, &DataType::Float64)
}

/// Whether an element of the array result can be NULL.
///
/// Spark declares the element non-nullable — `ArrayType(child.dataType, false)`
/// (`ApproximatePercentile.scala:243`) — yet still writes NULLs into it for a
/// decimal whose rescaled value overflows the declared precision, because
/// `InternalRow` never checks. Arrow does check, so the declaration has to be
/// honest for exactly the types [`f64_to_input_type`] can narrow to NULL.
fn element_is_nullable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Narrows a quantile back to the input type, mirroring the match in Spark's
/// `ApproximatePercentile.eval`.
///
/// Scala narrows `Double` to `Byte`/`Short` via `Int`, keeping the low bits
/// rather than saturating, so the `as i32 as i8` chain below is deliberate.
fn f64_to_input_type(value: f64, data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Int8 => {
            ScalarValue::new_primitive::<Int8Type>(Some(value as i32 as i8), data_type)
        }
        DataType::Int16 => {
            ScalarValue::new_primitive::<Int16Type>(Some(value as i32 as i16), data_type)
        }
        DataType::Int32 => ScalarValue::new_primitive::<Int32Type>(Some(value as i32), data_type),
        DataType::Int64 => ScalarValue::new_primitive::<Int64Type>(Some(value as i64), data_type),
        DataType::UInt8 => {
            ScalarValue::new_primitive::<UInt8Type>(Some(value as u32 as u8), data_type)
        }
        DataType::UInt16 => {
            ScalarValue::new_primitive::<UInt16Type>(Some(value as u32 as u16), data_type)
        }
        DataType::UInt32 => ScalarValue::new_primitive::<UInt32Type>(Some(value as u32), data_type),
        DataType::UInt64 => ScalarValue::new_primitive::<UInt64Type>(Some(value as u64), data_type),
        DataType::Float16 => {
            ScalarValue::new_primitive::<Float16Type>(Some(f16::from_f64(value)), data_type)
        }
        DataType::Float32 => {
            ScalarValue::new_primitive::<Float32Type>(Some(value as f32), data_type)
        }
        DataType::Float64 => ScalarValue::new_primitive::<Float64Type>(Some(value), data_type),
        DataType::Decimal32(precision, scale) => {
            let unscaled = spark_decimal_unscaled(value, *scale)
                .filter(|digits| fits_precision(digits, *precision))
                .and_then(|digits| digits.parse::<i32>().ok());
            ScalarValue::new_primitive::<Decimal32Type>(unscaled, data_type)
        }
        DataType::Decimal64(precision, scale) => {
            let unscaled = spark_decimal_unscaled(value, *scale)
                .filter(|digits| fits_precision(digits, *precision))
                .and_then(|digits| digits.parse::<i64>().ok());
            ScalarValue::new_primitive::<Decimal64Type>(unscaled, data_type)
        }
        DataType::Decimal128(precision, scale) => {
            // Spark returns NULL when the rescaled value does not fit the
            // declared precision (`changePrecision` fails).
            let unscaled = spark_decimal_unscaled(value, *scale)
                .filter(|digits| fits_precision(digits, *precision))
                .and_then(|digits| digits.parse::<i128>().ok());
            ScalarValue::new_primitive::<Decimal128Type>(unscaled, data_type)
        }
        DataType::Decimal256(precision, scale) => {
            let unscaled = spark_decimal_unscaled(value, *scale)
                .filter(|digits| fits_precision(digits, *precision))
                .and_then(|digits| i256::from_string(&digits));
            ScalarValue::new_primitive::<Decimal256Type>(unscaled, data_type)
        }
        DataType::Date32 => ScalarValue::new_primitive::<Date32Type>(Some(value as i32), data_type),
        DataType::Date64 => ScalarValue::new_primitive::<Date64Type>(Some(value as i64), data_type),
        DataType::Timestamp(TimeUnit::Second, _) => {
            ScalarValue::new_primitive::<TimestampSecondType>(Some(value as i64), data_type)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            ScalarValue::new_primitive::<TimestampMillisecondType>(Some(value as i64), data_type)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            ScalarValue::new_primitive::<TimestampMicrosecondType>(Some(value as i64), data_type)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            ScalarValue::new_primitive::<TimestampNanosecondType>(Some(value as i64), data_type)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            ScalarValue::new_primitive::<IntervalYearMonthType>(Some(value as i32), data_type)
        }
        DataType::Duration(TimeUnit::Second) => {
            ScalarValue::new_primitive::<DurationSecondType>(Some(value as i64), data_type)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            ScalarValue::new_primitive::<DurationMillisecondType>(Some(value as i64), data_type)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            ScalarValue::new_primitive::<DurationMicrosecondType>(Some(value as i64), data_type)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            ScalarValue::new_primitive::<DurationNanosecondType>(Some(value as i64), data_type)
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "percentile_approx not supported for {other}"
        ))),
    }
}

/// Rescales a quantile to a decimal's unscaled representation, the way Spark
/// does it in `ApproximatePercentile.eval`.
///
/// Spark narrows with `Decimal(double)` (`Decimal.scala:590`), which goes
/// through `BigDecimal.valueOf(d)` — the double's **shortest round-trip decimal
/// string** — and then rescales exactly with `changePrecision(p, s, HALF_UP)`
/// (`Decimal.scala:352`). Everything past the double's significant digits is
/// therefore zero.
///
/// Multiplying by `10^scale` in binary floating point does NOT reproduce that:
/// both the power and the product are inexact, so the double's garbage bits
/// leak into the result digits. This works on the decimal string instead, so
/// the arithmetic is exact.
///
/// Arrow allows the negative scale that Spark rejects in
/// `DecimalType.checkNegativeScale` (`Decimal.scala:395`); the rescale below
/// still holds for it, where Spark would have raised instead.
///
/// Returns the unscaled digits, or `None` for a value with no decimal
/// representation (NaN / infinity).
fn spark_decimal_unscaled(value: f64, scale: i8) -> Option<String> {
    if !value.is_finite() {
        return None;
    }
    // Rust's `Display` for `f64` is the shortest round-trip representation and
    // never uses exponent notation, matching the digits `Double.toString` emits
    // on JDK 19 and later. JDK 17 predates JDK-4511638 and can render a longer,
    // non-shortest string — `Double.toString(2e23)` is `1.9999999999999998E23`
    // there — so Spark on JDK 17 keeps trailing digits this drops.
    let rendered = format!("{value}");
    let (negative, magnitude) = match rendered.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, rendered.as_str()),
    };
    let (int_part, frac_part) = match magnitude.split_once('.') {
        Some(parts) => parts,
        None => (magnitude, ""),
    };

    let mut digits = String::with_capacity(int_part.len() + frac_part.len() + 40);
    digits.push_str(int_part);
    digits.push_str(frac_part);

    let current_scale = frac_part.len() as i64;
    let target_scale = scale as i64;
    if target_scale >= current_scale {
        for _ in 0..(target_scale - current_scale) {
            digits.push('0');
        }
    } else {
        let dropped = (current_scale - target_scale) as usize;
        // HALF_UP looks only at the first digit being discarded.
        let round_up = match digits.len().cmp(&dropped) {
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => digits.as_bytes().first() >= Some(&b'5'),
            std::cmp::Ordering::Greater => {
                digits.as_bytes().get(digits.len() - dropped) >= Some(&b'5')
            }
        };
        let keep = digits.len().saturating_sub(dropped);
        digits.truncate(keep);
        if round_up {
            digits = increment_digits(&digits);
        }
    }

    let trimmed = digits.trim_start_matches('0');
    Some(match (negative, trimmed.is_empty()) {
        (_, true) => "0".to_string(),
        (true, false) => format!("-{trimmed}"),
        (false, false) => trimmed.to_string(),
    })
}

/// Adds one to a non-negative decimal digit string.
fn increment_digits(digits: &str) -> String {
    let mut out: Vec<u8> = digits.as_bytes().to_vec();
    for byte in out.iter_mut().rev() {
        if *byte == b'9' {
            *byte = b'0';
        } else {
            *byte += 1;
            return String::from_utf8_lossy(&out).into_owned();
        }
    }
    let mut carried = String::with_capacity(out.len() + 1);
    carried.push('1');
    carried.push_str(&String::from_utf8_lossy(&out));
    carried
}

/// Whether the unscaled digits fit the declared decimal precision. Spark's
/// `changePrecision` fails — yielding NULL — when they do not.
fn fits_precision(digits: &str, precision: u8) -> bool {
    let significant = digits.trim_start_matches('-').trim_start_matches('0');
    significant.len() <= precision as usize
}
