// [CREDIT]: https://github.com/apache/datafusion/blob/70f2ed01b8392127fa0eb4f1fb5e57869c42ecfa/datafusion/functions-aggregate/src/approx_percentile_cont.rs

use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, ListArray, RecordBatch, RecordBatchOptions, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, Schema};
use datafusion_common::{
    Result, ScalarValue, downcast_value, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{Accumulator, AggregateUDFImpl, ColumnarValue, Signature, Volatility};
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, is_volatile};
use sail_common::spec::SAIL_SPARK_INTERVAL_METADATA_KEY;

use crate::aggregate::quantile_summary::{QuantileSummary, Sample};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxPercentileCont {
    signature: Signature,
}

impl Default for ApproxPercentileCont {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxPercentileCont {
    /// Create a new [`ApproxPercentileCont`] aggregate function.
    pub fn new() -> Self {
        let signature = Signature::user_defined(Volatility::Immutable);
        Self { signature }
    }

    pub(crate) fn create_accumulator(
        &self,
        args: &AccumulatorArgs,
    ) -> Result<ApproxPercentileAccumulator> {
        if args.is_distinct {
            return not_impl_err!("percentile_approx does not support DISTINCT");
        }
        let percentage = get_parameter(&args.exprs[1])?;
        let percentiles = match percentage {
            ScalarValue::List(array) if !array.is_null(0) => {
                let values = array.value(0);
                let values = downcast_value!(values, Float64Array);
                if values.null_count() != 0 {
                    return plan_err!("percentile_approx percentages must not contain nulls");
                }
                values.values().to_vec()
            }
            ScalarValue::Float64(Some(value)) => vec![value],
            _ => return plan_err!("percentile_approx percentage must not be null"),
        };
        if percentiles.iter().any(|p| !(0.0..=1.0).contains(p)) {
            return plan_err!("percentile_approx percentages must be between 0 and 1");
        }
        let accuracy = if args.exprs.len() == 3 {
            validate_input_max_size_expr(&args.exprs[2])?
        } else {
            10000
        };
        Ok(ApproxPercentileAccumulator {
            digest: QuantileSummary::new(accuracy),
            percentiles,
            return_type: args.return_field.data_type().clone(),
            value_type: args.expr_fields[0].data_type().clone(),
        })
    }
}

fn get_parameter(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    if is_volatile(expr) {
        return plan_err!("percentile_approx percentage and accuracy must be constant expressions");
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;
    match expr.evaluate(&batch).map_err(|_| {
        plan_datafusion_err!(
            "percentile_approx percentage and accuracy must be constant expressions"
        )
    })? {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0),
    }
}

fn validate_input_max_size_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    let max_size = match get_parameter(expr)? {
        ScalarValue::UInt8(Some(q)) => u64::from(q),
        ScalarValue::UInt16(Some(q)) => u64::from(q),
        ScalarValue::UInt32(Some(q)) => u64::from(q),
        ScalarValue::UInt64(Some(q)) => q,
        ScalarValue::Int8(Some(q)) if q > 0 => q as u64,
        ScalarValue::Int16(Some(q)) if q > 0 => q as u64,
        ScalarValue::Int32(Some(q)) if q > 0 => q as u64,
        ScalarValue::Int64(Some(q)) if q > 0 => q as u64,
        _ => return plan_err!("percentile_approx accuracy must be a positive integer constant"),
    };
    if max_size == 0 || max_size > i32::MAX as u64 {
        return plan_err!("percentile_approx accuracy must be between 1 and 2147483647");
    }
    Ok(max_size as usize)
}

impl AggregateUDFImpl for ApproxPercentileCont {
    fn name(&self) -> &str {
        "percentile_approx"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let types = self.coerce_types(arg_types)?;
        if matches!(types[1], DataType::List(_)) {
            Ok(DataType::List(Arc::new(Field::new_list_field(
                types[0].clone(),
                false,
            ))))
        } else {
            Ok(types[0].clone())
        }
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types = arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let value_type = match &data_type {
            DataType::List(element) => element.data_type(),
            data_type => data_type,
        };
        let metadata = matches!(
            value_type,
            DataType::Interval(IntervalUnit::YearMonth) | DataType::Duration(_)
        )
        .then(|| {
            arg_fields[0]
                .metadata()
                .get(SAIL_SPARK_INTERVAL_METADATA_KEY)
        })
        .flatten()
        .map(|value| (SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), value.clone()))
        .into_iter()
        .collect();
        let field = match data_type {
            DataType::List(element) => Field::new_list(
                self.name(),
                element.as_ref().clone().with_metadata(metadata),
                true,
            ),
            data_type => Field::new(self.name(), data_type, true).with_metadata(metadata),
        };
        Ok(Arc::new(field))
    }

    #[inline]
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(self.create_accumulator(&acc_args)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let value_type = args.input_fields[0].data_type();
        Ok(vec![
            Field::new_list(
                format_state_name(args.name, "values"),
                Field::new_list_field(value_type.clone(), false),
                false,
            ),
            Field::new_list(
                format_state_name(args.name, "gaps"),
                Field::new_list_field(DataType::UInt64, false),
                false,
            ),
            Field::new_list(
                format_state_name(args.name, "deltas"),
                Field::new_list_field(DataType::UInt64, false),
                false,
            ),
            Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                false,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&arg_types.len()) {
            return plan_err!("percentile_approx requires 2 or 3 arguments");
        }
        fn dictionary_value_type(mut data_type: &DataType) -> &DataType {
            while let DataType::Dictionary(_, value) = data_type {
                data_type = value;
            }
            data_type
        }
        let input = dictionary_value_type(&arg_types[0]);
        let input = match input {
            DataType::Null => DataType::Float64,
            dt if dt.is_numeric()
                || matches!(
                    dt,
                    DataType::Interval(IntervalUnit::YearMonth) | DataType::Duration(_)
                ) =>
            {
                dt.clone()
            }
            dt => {
                return plan_err!(
                    "percentile_approx requires numeric input or a supported interval, got {dt}"
                );
            }
        };
        let percentage = match dictionary_value_type(&arg_types[1]) {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
                if dictionary_value_type(field.data_type()).is_numeric()
                    || dictionary_value_type(field.data_type()) == &DataType::Null =>
            {
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
            }
            dt if dt.is_numeric() || dt == &DataType::Null => DataType::Float64,
            dt => {
                return plan_err!(
                    "percentile_approx percentage must be numeric or a numeric array, got {dt}"
                );
            }
        };
        let mut types = vec![input, percentage];
        if let Some(accuracy) = arg_types.get(2) {
            let accuracy = dictionary_value_type(accuracy);
            if !accuracy.is_integer() {
                return plan_err!("percentile_approx accuracy must be an integer");
            }
            types.push(accuracy.clone());
        }
        Ok(types)
    }
}

#[derive(Debug)]
pub struct ApproxPercentileAccumulator {
    digest: QuantileSummary,
    percentiles: Vec<f64>,
    return_type: DataType,
    value_type: DataType,
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        for index in 0..values.len() {
            if !values.is_null(index) {
                self.digest
                    .insert(ScalarValue::try_from_array(values, index)?);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.digest.compress();
        if self.digest.count == 0 || self.percentiles.is_empty() {
            return ScalarValue::try_from(self.return_type.clone());
        }
        let values = self.digest.quantiles(&self.percentiles);
        if let DataType::List(element) = &self.return_type {
            let values = ScalarValue::iter_to_array(values)?;
            Ok(ScalarValue::List(Arc::new(ListArray::new(
                Arc::clone(element),
                OffsetBuffer::from_lengths([values.len()]),
                values,
                None,
            ))))
        } else {
            Ok(values[0].clone())
        }
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.digest.size() - size_of_val(&self.digest)
            + self.percentiles.capacity() * size_of_val(&0.0_f64)
            + self.return_type.size()
            - size_of_val(&self.return_type)
            + self.value_type.size()
            - size_of_val(&self.value_type)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.digest.compress();
        let values = self
            .digest
            .sampled
            .iter()
            .map(|s| s.value.clone())
            .collect::<Vec<_>>();
        let gaps = self
            .digest
            .sampled
            .iter()
            .map(|s| ScalarValue::UInt64(Some(s.g)))
            .collect::<Vec<_>>();
        let deltas = self
            .digest
            .sampled
            .iter()
            .map(|s| ScalarValue::UInt64(Some(s.delta)))
            .collect::<Vec<_>>();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&values, &self.value_type, false)),
            ScalarValue::List(ScalarValue::new_list(&gaps, &DataType::UInt64, false)),
            ScalarValue::List(ScalarValue::new_list(&deltas, &DataType::UInt64, false)),
            ScalarValue::UInt64(Some(self.digest.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        for row in 0..states[0].len() {
            let values = downcast_value!(states[0], ListArray).value(row);
            let gaps = downcast_value!(states[1], ListArray).value(row);
            let deltas = downcast_value!(states[2], ListArray).value(row);
            let gaps = downcast_value!(gaps, UInt64Array);
            let deltas = downcast_value!(deltas, UInt64Array);
            let sampled = (0..values.len())
                .map(|i| {
                    Ok(Sample {
                        value: ScalarValue::try_from_array(&values, i)?,
                        g: gaps.value(i),
                        delta: deltas.value(i),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.digest
                .merge(sampled, downcast_value!(states[3], UInt64Array).value(row));
        }
        Ok(())
    }
}
