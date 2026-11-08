// TODO: Remove the filter mode once datafusion-proto serializes window aggregate filters and
//   orderings; the ordering mode is for WITHIN GROUP ... OVER, which DataFusion SQL rejects.

use std::mem::size_of_val;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::{SortOptions, filter, prep_null_mask_filter};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::expr::{AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ReversedUDAF, SetMonotonicity, Signature,
};

/// An aggregate function over a window with Spark clauses that DataFusion does not pass to
/// the aggregate function of a window.
///
/// - With a filter, it aggregates only the rows where the filter is true. This is how Sail plans
///   `FILTER (WHERE ...)` of an aggregate function over a window. The filter is the last
///   argument, and rows where it is not true (false or NULL) are removed before they reach the
///   accumulator of the inner function, but they still count toward the window frame, as in
///   Spark. DataFusion supports a filter for window aggregates natively, but it does not
///   serialize the filter for distributed execution, so the filter is an argument here instead.
///   <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionResolution.scala#L479-L560>
/// - With an ordering, the inner function receives the ordering of its first argument, as for
///   `percentile_cont(p) WITHIN GROUP (ORDER BY x DESC)`. DataFusion does not pass the ordering
///   of a window to the aggregate function of the window.
///
/// Everything else is delegated to the inner function, and the name of the expression is the
/// name the inner function gives it with the filter.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WindowAggregate {
    inner: Arc<AggregateUDF>,
    filter: bool,
    ordering: Option<SortOptions>,
    signature: Signature,
}

impl WindowAggregate {
    pub fn new(inner: Arc<AggregateUDF>, filter: bool, ordering: Option<SortOptions>) -> Self {
        let signature = Signature::user_defined(inner.signature().volatility);
        Self {
            inner,
            filter,
            ordering,
            signature,
        }
    }

    pub fn inner(&self) -> &Arc<AggregateUDF> {
        &self.inner
    }

    pub fn filter(&self) -> bool {
        self.filter
    }

    pub fn ordering(&self) -> Option<SortOptions> {
        self.ordering
    }

    fn with_inner(&self, inner: Arc<AggregateUDF>) -> Self {
        Self::new(inner, self.filter, self.ordering)
    }

    /// Splits the arguments into the arguments of the inner function and the filter.
    fn split_args<'a, T>(&self, args: &'a [T]) -> Result<(&'a [T], Option<&'a T>)> {
        if !self.filter {
            return Ok((args, None));
        }
        match args.split_last() {
            Some((filter, args)) => Ok((args, Some(filter))),
            None => plan_err!("window aggregate function expects a filter argument"),
        }
    }

    fn inner_aggregate_params(
        &self,
        params: &AggregateFunctionParams,
    ) -> Result<AggregateFunctionParams> {
        let (args, filter) = self.split_args(&params.args)?;
        Ok(AggregateFunctionParams {
            args: args.to_vec(),
            filter: filter.map(|filter| Box::new(filter.clone())),
            ..params.clone()
        })
    }

    fn inner_window_params(&self, params: &WindowFunctionParams) -> Result<WindowFunctionParams> {
        let (args, filter) = self.split_args(&params.args)?;
        Ok(WindowFunctionParams {
            args: args.to_vec(),
            filter: filter.map(|filter| Box::new(filter.clone())),
            ..params.clone()
        })
    }

    /// The ordering that the inner function receives.
    fn order_bys(&self, args: &AccumulatorArgs) -> Result<Vec<PhysicalSortExpr>> {
        match (self.ordering, args.exprs.first()) {
            (Some(options), Some(expr)) if args.order_bys.is_empty() => {
                Ok(vec![PhysicalSortExpr::new(Arc::clone(expr), options)])
            }
            (Some(_), None) => plan_err!("window aggregate function expects an argument to order"),
            _ => Ok(args.order_bys.to_vec()),
        }
    }

    fn inner_accumulator_args<'a>(
        &self,
        args: &AccumulatorArgs<'a>,
        order_bys: &'a [PhysicalSortExpr],
    ) -> Result<AccumulatorArgs<'a>> {
        let (exprs, _) = self.split_args(args.exprs)?;
        let (expr_fields, _) = self.split_args(args.expr_fields)?;
        Ok(AccumulatorArgs {
            return_field: Arc::clone(&args.return_field),
            schema: args.schema,
            ignore_nulls: args.ignore_nulls,
            order_bys,
            is_reversed: args.is_reversed,
            name: args.name,
            is_distinct: args.is_distinct,
            exprs,
            expr_fields,
        })
    }

    fn wrap_accumulator(&self, inner: Box<dyn Accumulator>) -> Box<dyn Accumulator> {
        if self.filter {
            Box::new(FilteredAccumulator { inner })
        } else {
            inner
        }
    }
}

impl AggregateUDFImpl for WindowAggregate {
    fn name(&self) -> &str {
        // The name differs from the name of the inner function, so that optimizer rules that
        // find functions by name do not take this function for the inner function.
        "spark_window_aggregate"
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner
            .schema_name(&self.inner_aggregate_params(params)?)
    }

    fn human_display(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner
            .human_display(&self.inner_aggregate_params(params)?)
    }

    fn window_function_schema_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner
            .window_function_schema_name(&self.inner_window_params(params)?)
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner
            .display_name(&self.inner_aggregate_params(params)?)
    }

    fn window_function_display_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner
            .window_function_display_name(&self.inner_window_params(params)?)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let (inner_types, filter) = self.split_args(arg_types)?;
        let fields = inner_types
            .iter()
            .map(|data_type| Arc::new(Field::new("", data_type.clone(), true)))
            .collect::<Vec<_>>();
        let mut data_types = fields_with_udf(&fields, self.inner.as_ref())?
            .into_iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        if filter.is_some() {
            data_types.push(DataType::Boolean);
        }
        Ok(data_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (arg_types, _) = self.split_args(arg_types)?;
        self.inner.return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let (arg_fields, _) = self.split_args(arg_fields)?;
        self.inner.return_field(arg_fields)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let order_bys = self.order_bys(&acc_args)?;
        let inner = self
            .inner
            .accumulator(self.inner_accumulator_args(&acc_args, &order_bys)?)?;
        Ok(self.wrap_accumulator(inner))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let order_bys = self.order_bys(&args)?;
        let inner = self
            .inner
            .create_sliding_accumulator(self.inner_accumulator_args(&args, &order_bys)?)?;
        Ok(self.wrap_accumulator(inner))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let (input_fields, _) = self.split_args(args.input_fields)?;
        self.inner.state_fields(StateFieldsArgs {
            input_fields,
            ..args
        })
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(self
            .inner
            .as_ref()
            .clone()
            .with_beneficial_ordering(beneficial_ordering)?
            .map(|inner| Arc::new(self.with_inner(Arc::new(inner))) as Arc<dyn AggregateUDFImpl>))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        match self.inner.reverse_udf() {
            ReversedUDAF::Identical => ReversedUDAF::Identical,
            ReversedUDAF::NotSupported => ReversedUDAF::NotSupported,
            ReversedUDAF::Reversed(inner) => {
                ReversedUDAF::Reversed(Arc::new(AggregateUDF::from(self.with_inner(inner))))
            }
        }
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        self.inner.supports_null_handling_clause()
    }

    fn supports_within_group_clause(&self) -> bool {
        self.inner.supports_within_group_clause()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
    }
}

/// Removes the rows where the filter (the last argument) is not true.
fn filter_rows(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    let Some((mask, values)) = values.split_last() else {
        return exec_err!("window aggregate function expects a filter argument");
    };
    let Some(mask) = mask.as_boolean_opt() else {
        return exec_err!(
            "window aggregate function expects a boolean filter, got {}",
            mask.data_type()
        );
    };
    if mask.null_count() == 0 && mask.true_count() == mask.len() {
        return Ok(values.to_vec());
    }
    // NULL counts as false. `prep_null_mask_filter` requires a mask with NULL values.
    let prepared;
    let mask = if mask.null_count() > 0 {
        prepared = prep_null_mask_filter(mask);
        &prepared
    } else {
        mask
    };
    Ok(values
        .iter()
        .map(|value| filter(value, mask))
        .collect::<Result<Vec<_>, _>>()?)
}

#[derive(Debug)]
struct FilteredAccumulator {
    inner: Box<dyn Accumulator>,
}

impl Accumulator for FilteredAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&filter_rows(values)?)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(&filter_rows(values)?)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::expressions::col;

    use super::*;

    #[test]
    fn test_filtered_accumulator_removes_rows() -> Result<()> {
        let udaf = AggregateUDF::from(WindowAggregate::new(sum_udaf(), true, None));
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("f", DataType::Boolean, true),
        ]);
        let exprs = vec![col("x", &schema)?, col("f", &schema)?];
        let expr_fields = schema.fields().iter().cloned().collect::<Vec<_>>();
        let return_field = udaf.return_field(&expr_fields)?;
        assert_eq!(return_field.data_type(), &DataType::Int64);
        let args = || AccumulatorArgs {
            return_field: Arc::clone(&return_field),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "sum(x) FILTER (WHERE f)",
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &expr_fields,
        };
        let values: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2, 4, 8])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ];

        let mut accumulator = udaf.accumulator(args())?;
        accumulator.update_batch(&values)?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::Int64(Some(9)));

        let mut accumulator = udaf.create_sliding_accumulator(args())?;
        assert!(accumulator.supports_retract_batch());
        accumulator.update_batch(&values)?;
        accumulator.retract_batch(&[
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::Int64(Some(8)));
        Ok(())
    }

    #[test]
    fn test_filtered_aggregate_types_and_names() -> Result<()> {
        let udaf = WindowAggregate::new(sum_udaf(), true, None);
        assert_eq!(
            udaf.coerce_types(&[DataType::Int32, DataType::Boolean])?,
            vec![DataType::Int64, DataType::Boolean]
        );
        let params = WindowFunctionParams {
            args: vec![
                datafusion_expr::col("x"),
                datafusion_expr::col("y").gt(datafusion_expr::lit(1)),
            ],
            partition_by: vec![],
            order_by: vec![],
            window_frame: datafusion_expr::WindowFrame::new(None),
            filter: None,
            null_treatment: None,
            distinct: false,
        };
        let inner_params = WindowFunctionParams {
            args: vec![datafusion_expr::col("x")],
            filter: Some(Box::new(
                datafusion_expr::col("y").gt(datafusion_expr::lit(1)),
            )),
            ..params.clone()
        };
        assert_eq!(
            udaf.window_function_schema_name(&params)?,
            sum_udaf().window_function_schema_name(&inner_params)?
        );
        assert!(
            udaf.window_function_schema_name(&params)?
                .starts_with("sum(x) FILTER (WHERE y > Int32(1))")
        );
        Ok(())
    }

    #[test]
    fn test_window_aggregate_passes_ordering() -> Result<()> {
        use datafusion::arrow::array::Float64Array;
        use datafusion::functions_aggregate::percentile_cont::percentile_cont_udaf;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::Literal;

        let schema = Schema::new(vec![Field::new("x", DataType::Float64, true)]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            col("x", &schema)?,
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.25)))),
        ];
        let expr_fields = vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("p", DataType::Float64, false)),
        ];
        let values: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![3.0, 1.0, 2.0, 2.0])),
            Arc::new(Float64Array::from(vec![0.25; 4])),
        ];
        for (descending, expected) in [(false, 1.75), (true, 2.25)] {
            let udaf = AggregateUDF::from(WindowAggregate::new(
                percentile_cont_udaf(),
                false,
                Some(SortOptions {
                    descending,
                    nulls_first: false,
                }),
            ));
            let return_field = udaf.return_field(&expr_fields)?;
            let mut accumulator = udaf.accumulator(AccumulatorArgs {
                return_field,
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                is_reversed: false,
                name: "percentile_cont(x, 0.25)",
                is_distinct: false,
                exprs: &exprs,
                expr_fields: &expr_fields,
            })?;
            accumulator.update_batch(&values)?;
            assert_eq!(
                accumulator.evaluate()?,
                ScalarValue::Float64(Some(expected))
            );
        }
        Ok(())
    }

    #[test]
    fn test_window_aggregate_reverse_expr() {
        use crate::aggregate::first_last::{FirstValue, LastValue};

        let ordering = Some(SortOptions {
            descending: true,
            nulls_first: false,
        });
        let first = Arc::new(AggregateUDF::from(FirstValue::new()));
        let reversed =
            AggregateUDF::from(WindowAggregate::new(first, true, ordering)).reverse_udf();
        let ReversedUDAF::Reversed(reversed) = reversed else {
            unreachable!("the reverse of first_value is last_value")
        };
        let window = reversed.inner().downcast_ref::<WindowAggregate>();
        assert!(window.is_some_and(|window| {
            window.inner().inner().is::<LastValue>()
                && window.filter()
                && window.ordering() == ordering
        }));

        let sum = AggregateUDF::from(WindowAggregate::new(sum_udaf(), true, None));
        assert!(matches!(sum.reverse_udf(), ReversedUDAF::Identical));
    }
}
