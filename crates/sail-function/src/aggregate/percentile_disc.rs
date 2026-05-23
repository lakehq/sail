use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, Float64Array, ListArray, PrimitiveArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion::common::{DataFusionError, HashSet, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};

use crate::aggregate::percentile_disc_groups::{
    DistinctPercentileDiscAccumulator, PercentileDiscGroupsAccumulator,
};
use crate::aggregate::utils::{
    calculate_percentile_disc, cast_to_type, extract_percentile_literal, extract_percentiles_array,
};

macro_rules! dispatch_numeric_type {
    ($input_dt:expr, $helper:ident, $err_msg:expr) => {
        match &$input_dt {
            DataType::Int8 => $helper!(Int8Type, $input_dt),
            DataType::Int16 => $helper!(Int16Type, $input_dt),
            DataType::Int32 => $helper!(Int32Type, $input_dt),
            DataType::Int64 => $helper!(Int64Type, $input_dt),
            DataType::UInt8 => $helper!(UInt8Type, $input_dt),
            DataType::UInt16 => $helper!(UInt16Type, $input_dt),
            DataType::UInt32 => $helper!(UInt32Type, $input_dt),
            DataType::UInt64 => $helper!(UInt64Type, $input_dt),
            DataType::Float16 => $helper!(Float16Type, $input_dt),
            DataType::Float32 => $helper!(Float32Type, $input_dt),
            DataType::Float64 => $helper!(Float64Type, $input_dt),
            DataType::Decimal128(_, _) => $helper!(Decimal128Type, $input_dt),
            DataType::Decimal256(_, _) => $helper!(Decimal256Type, $input_dt),
            _ => Err(DataFusionError::NotImplemented(format!(
                "{} not supported for {}",
                $err_msg, $input_dt,
            ))),
        }
    };
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PercentileDisc {
    signature: Signature,
}

impl Default for PercentileDisc {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileDisc {
    pub fn new() -> Self {
        // `Signature::user_defined` so the planner calls our `coerce_types`,
        // which:
        // * coerces the ORDER BY column from any numeric (including
        //   `Decimal128`/`Decimal256` of arbitrary precision/scale) to itself
        //   when primitive, or to `Float64` for decimals
        // * coerces the percentile argument to either `Float64` (scalar form)
        //   or `List<Float64>` (array-of-percentiles form)
        //
        // This unlocks both `percentile_disc(0.5) WITHIN GROUP (ORDER BY x)`
        // and `percentile_disc(array(p1, p2, ...)) WITHIN GROUP (ORDER BY x)`
        // for every numeric type without enumerating one `Exact` variant per
        // `(precision, scale)` pair.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// `true` when the (single) `ORDER BY` direction is `DESC`.
    fn is_descending(args: &AccumulatorArgs) -> bool {
        args.order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false)
    }

    /// Extract the scalar percentile literal. We do NOT invert for `DESC` here:
    /// `percentile_disc`'s discrete index is asymmetric, so the descending
    /// adjustment is applied inside `calculate_percentile_disc` instead.
    fn resolve_percentile(args: &AccumulatorArgs) -> Result<f64> {
        extract_percentile_literal(&args.exprs[1])
    }

    /// Returns `Some(percentiles)` if `args.exprs[1]` is a list literal of
    /// percentile values, `None` if it's a scalar.
    fn try_resolve_percentiles_array(args: &AccumulatorArgs) -> Result<Option<Vec<f64>>> {
        // `coerce_types` normalizes any list flavor to `List(Float64)`, so we
        // only need to check for `List`.
        if !matches!(args.exprs[1].data_type(args.schema)?, DataType::List(_)) {
            return Ok(None);
        }
        let percentiles = extract_percentiles_array(&args.exprs[1])?;
        Ok(Some(percentiles))
    }
}

impl AggregateUDFImpl for PercentileDisc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "percentile_disc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn supports_within_group_clause(&self) -> bool {
        // `percentile_disc` is an ordered-set aggregate (`WITHIN GROUP`).
        // Tell DataFusion so it can format the schema name correctly
        // (`WITHIN GROUP (ORDER BY ...)` instead of `ORDER BY ...`) and skip
        // the duplicated leading arg when rendering.
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "percentile_disc expects 2 arguments, got {}",
                arg_types.len()
            )));
        }
        // arg[0]: ORDER BY column. Spark accepts every numeric type and STRING
        // (cast-to-double semantics) and always returns `double`. Route ALL
        // accepted inputs through `Float64` so the accumulator produces `f64`
        // consistently. Spark also accepts `INTERVAL`/`DURATION` (returns the
        // interval type), but Sail's percentile_disc does not yet implement
        // those accumulators — reject them at planning time with a clear error.
        let order_by = match &arg_types[0] {
            dt if dt.is_numeric() => DataType::Float64,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Float64,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "percentile_disc: ORDER BY column must be numeric or string, got {other}"
                )));
            }
        };
        // arg[1]: percentile literal — either a single numeric (scalar form) or
        // a `List`/`LargeList`/`FixedSizeList` of numerics (array form). Coerce
        // to `Float64` / `List<Float64>` respectively so the accumulator gets a
        // predictable shape.
        let percentile = match &arg_types[1] {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
            }
            _ => DataType::Float64,
        };
        Ok(vec![order_by, percentile])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // arg_types here are already coerced by `coerce_types`, so decimals
        // arrive as `Float64`. For the array-of-percentiles form we return
        // `List<Float64>` (Spark's `array<double>`); for the scalar form we
        // preserve the (already-coerced) input numeric type.
        if matches!(arg_types.get(1), Some(DataType::List(_))) {
            return Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Float64,
                true,
            ))));
        }
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // After `coerce_types`, decimals have been mapped to `Float64`. Storage
        // type is `Float64` for the array form, otherwise the (already-coerced)
        // input numeric type.
        let is_array_percentiles = matches!(
            args.input_fields.get(1).map(|f| f.data_type()),
            Some(DataType::List(_))
        );
        let value_type = if is_array_percentiles {
            DataType::Float64
        } else {
            args.input_fields[0].data_type().clone()
        };
        let field = Field::new_list_field(value_type, true);
        let state_name = if args.is_distinct {
            "distinct_percentile_disc"
        } else {
            "percentile_disc"
        };

        Ok(vec![Field::new(
            format_state_name(args.name, state_name),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Spark rejects `DISTINCT` on `WITHIN GROUP` aggregates with
        // `INVALID_WITHIN_GROUP_EXPRESSION.DISTINCT_UNSUPPORTED`. The DISTINCT
        // keyword is meaningless here (the function's value is a position in
        // the sorted population, not a count of distinct elements) — surface a
        // matching error rather than silently de-duplicate the input.
        if args.is_distinct {
            return Err(DataFusionError::Plan(
                "percentile_disc does not support DISTINCT with WITHIN GROUP".into(),
            ));
        }

        // Array-of-percentiles form: `percentile_disc(array(p1, p2, ...))`.
        if let Some(percentiles) = Self::try_resolve_percentiles_array(&args)? {
            return Ok(Box::new(MultiPercentileDiscAccumulator {
                all_values: vec![],
                percentiles,
                descending: Self::is_descending(&args),
            }));
        }

        // Scalar percentile form: `percentile_disc(p)`.
        let percentile = Self::resolve_percentile(&args)?;
        let descending = Self::is_descending(&args);

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                if args.is_distinct {
                    Ok(Box::new(DistinctPercentileDiscAccumulator::<$t> {
                        data_type: $dt.clone(),
                        distinct_values: HashSet::default(),
                        percentile,
                        descending,
                    }))
                } else {
                    Ok(Box::new(PercentileDiscAccumulator::<$t> {
                        data_type: $dt.clone(),
                        all_values: vec![],
                        percentile,
                        descending,
                    }))
                }
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscAccumulator")
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if args.is_distinct {
            return false;
        }
        // Array-of-percentiles is not yet supported by the groups accumulator
        // fast path; fall back to the regular per-group accumulator above.
        let arr_form = args
            .exprs
            .get(1)
            .and_then(|e| e.data_type(args.schema).ok())
            .map(|dt| matches!(dt, DataType::List(_)))
            .unwrap_or(false);
        !arr_form
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let percentile = Self::resolve_percentile(&args)?;
        let descending = Self::is_descending(&args);

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileDiscGroupsAccumulator::<$t>::new(
                    $dt, percentile, descending,
                )))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscGroupsAccumulator")
    }
}

struct PercentileDiscAccumulator<T: ArrowNumericType> {
    data_type: DataType,
    all_values: Vec<T::Native>,
    percentile: f64,
    descending: bool,
}

impl<T: ArrowNumericType> Debug for PercentileDiscAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PercentileDiscAccumulator({}, percentile={}, descending={})",
            self.data_type, self.percentile, self.descending
        )
    }
}

impl<T: ArrowNumericType> Accumulator for PercentileDiscAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, self.all_values.len() as i32]));

        let values_array = PrimitiveArray::<T>::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        )
        .with_data_type(self.data_type.clone());

        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(values_array),
            None,
        );

        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_array = cast_to_type(&values[0], &self.data_type)?;

        let null_count = values_array.null_count();
        let values = values_array.as_primitive::<T>();
        self.all_values.reserve(values.len() - null_count);
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let d = std::mem::take(&mut self.all_values);
        let value = calculate_percentile_disc::<T>(d, self.percentile, self.descending);
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
    }
}

/// Accumulator for the array-of-percentiles form: stores values as `f64`
/// and computes each percentile from the same buffer.
///
/// Output is `List<Float64>` matching Spark's `array<double>` return type.
struct MultiPercentileDiscAccumulator {
    all_values: Vec<f64>,
    percentiles: Vec<f64>,
    descending: bool,
}

impl Debug for MultiPercentileDiscAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiPercentileDiscAccumulator(percentiles={:?}, len={}, descending={})",
            self.percentiles,
            self.all_values.len(),
            self.descending
        )
    }
}

impl Accumulator for MultiPercentileDiscAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            self.all_values.len() as i32,
        ]));
        let values_array = Float64Array::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        );
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_array = cast_to_type(&values[0], &DataType::Float64)?;
        let null_count = values_array.null_count();
        let values = values_array.as_primitive::<Float64Type>();
        self.all_values.reserve(values.len() - null_count);
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // `calculate_percentile_disc::<Float64Type>` returns `Option<f64>`. When
        // the input is empty OR the percentile array is empty (e.g. callers
        // passed `array()`), return a NULL list — matches Spark, which
        // collapses `percentile_disc(array()) WITHIN GROUP (ORDER BY x)` to
        // NULL rather than an empty `array<double>`.
        if self.all_values.is_empty() || self.percentiles.is_empty() {
            let field = Arc::new(Field::new_list_field(DataType::Float64, true));
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(field, 1))));
        }
        // Compute each percentile on a fresh copy of the buffer (the kernel
        // uses `select_nth_unstable_by`, which would reorder shared state).
        let mut results: Vec<Option<f64>> = Vec::with_capacity(self.percentiles.len());
        for &p in &self.percentiles {
            let buf = self.all_values.clone();
            results.push(calculate_percentile_disc::<Float64Type>(
                buf,
                p,
                self.descending,
            ));
        }
        let values_array = Float64Array::from_iter(results);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values_array.len() as i32]));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.all_values.capacity() * size_of::<f64>()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

pub fn percentile_disc_udaf() -> Arc<datafusion::logical_expr::AggregateUDF> {
    Arc::new(datafusion::logical_expr::AggregateUDF::from(
        PercentileDisc::new(),
    ))
}
