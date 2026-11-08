use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::ops::Deref;
use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/max_min_by.rs>
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility, function,
};
use datafusion::prelude::Expr;
use sail_common_datafusion::min_max::common::MinMaxValue;
use sail_common_datafusion::min_max::min_max_agg::{MovingMax, MovingMin};
use sail_common_datafusion::ordering::spark_nested_sort_options;

use crate::aggregate::first_last::LastValue;

#[derive(Debug)]
struct MaxMinByAccumulator {
    value: ScalarValue,
    // Compare nested values like Spark; see `MinMaxValue`.
    ordering: MinMaxValue,
    is_max: bool,
}

impl MaxMinByAccumulator {
    fn new(
        value_type: &DataType,
        ordering_type: &DataType,
        is_max: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            value: ScalarValue::try_from(value_type)?,
            ordering: MinMaxValue(ScalarValue::try_from(ordering_type)?),
            is_max,
        })
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let value_array = &values[0];
        let ordering_array = &values[1];
        let ordering_nulls = ordering_array.logical_nulls();

        for i in 0..ordering_array.len() {
            if ordering_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(i))
            {
                continue;
            }
            let ordering_val = MinMaxValue(ScalarValue::try_from_array(ordering_array, i)?);
            // Like Spark, the newer row wins on a tie.
            let should_update = if self.ordering.0.is_null() {
                true
            } else {
                match ordering_val.partial_cmp(&self.ordering) {
                    Some(Ordering::Greater) => self.is_max,
                    Some(Ordering::Less) => !self.is_max,
                    Some(Ordering::Equal) => true,
                    None => false,
                }
            };
            if should_update {
                self.value = ScalarValue::try_from_array(value_array, i)?;
                self.ordering = ordering_val;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        Ok(vec![self.value.clone(), self.ordering.0.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        self.value.size() + self.ordering.0.size() + std::mem::size_of::<bool>()
    }
}

/// A value of a sliding window `max_by` or `min_by` together with its ordering,
/// which compares by the ordering only.
#[derive(Debug)]
struct MaxMinByEntry {
    value: ScalarValue,
    ordering: MinMaxValue,
}

impl PartialEq for MaxMinByEntry {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for MaxMinByEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.ordering.partial_cmp(&other.ordering)
    }
}

#[derive(Debug)]
enum MovingMaxMinBy {
    Max(MovingMax<MaxMinByEntry>),
    Min(MovingMin<MaxMinByEntry>),
}

/// The `max_by` or `min_by` accumulator for window frames that do not start at the
/// beginning of the partition. Like Spark, rows with a NULL ordering are skipped, and the
/// newer row wins on a tie, which `MovingMax` and `MovingMin` do by dropping the older
/// candidates that are not greater (or not less) than a new row.
#[derive(Debug)]
struct SlidingMaxMinByAccumulator {
    /// Typed NULL values returned when the window contains no non-null orderings
    empty_value: ScalarValue,
    empty_ordering: ScalarValue,
    moving: MovingMaxMinBy,
}

impl SlidingMaxMinByAccumulator {
    fn try_new(
        value_type: &DataType,
        ordering_type: &DataType,
        is_max: bool,
    ) -> Result<Self, DataFusionError> {
        let moving = if is_max {
            MovingMaxMinBy::Max(MovingMax::new())
        } else {
            MovingMaxMinBy::Min(MovingMin::new())
        };
        Ok(Self {
            empty_value: ScalarValue::try_from(value_type)?,
            empty_ordering: ScalarValue::try_from(ordering_type)?,
            moving,
        })
    }

    fn current(&self) -> Option<&MaxMinByEntry> {
        match &self.moving {
            MovingMaxMinBy::Max(moving) => moving.max(),
            MovingMaxMinBy::Min(moving) => moving.min(),
        }
    }
}

impl Accumulator for SlidingMaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let value_array = &values[0];
        let ordering_array = &values[1];
        let ordering_nulls = ordering_array.logical_nulls();

        for i in 0..ordering_array.len() {
            if ordering_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(i))
            {
                continue;
            }
            let entry = MaxMinByEntry {
                value: ScalarValue::try_from_array(value_array, i)?,
                ordering: MinMaxValue(ScalarValue::try_from_array(ordering_array, i)?),
            };
            match &mut self.moving {
                MovingMaxMinBy::Max(moving) => moving.push(entry),
                MovingMaxMinBy::Min(moving) => moving.push(entry),
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(match self.current() {
            Some(entry) => entry.value.clone(),
            None => self.empty_value.clone(),
        })
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        Ok(match self.current() {
            Some(entry) => vec![entry.value.clone(), entry.ordering.0.clone()],
            None => vec![self.empty_value.clone(), self.empty_ordering.clone()],
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.update_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        // The retracted rows are the oldest ones, in the order they were added. Rows with a
        // NULL ordering were never pushed, so pop once per row with a non-null ordering.
        let count = values[1].len() - values[1].logical_null_count();
        for _ in 0..count {
            match &mut self.moving {
                MovingMaxMinBy::Max(moving) => moving.pop(),
                MovingMaxMinBy::Min(moving) => moving.pop(),
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        let entry_heap = |entry: &MaxMinByEntry| {
            entry.value.size() - size_of_val(&entry.value) + entry.ordering.0.size()
                - size_of_val(&entry.ordering.0)
        };
        let moving = match &self.moving {
            MovingMaxMinBy::Max(moving) => moving.heap_size(entry_heap),
            MovingMaxMinBy::Min(moving) => moving.heap_size(entry_heap),
        };
        size_of_val(self) - size_of_val(&self.empty_value) - size_of_val(&self.empty_ordering)
            + self.empty_value.size()
            + self.empty_ordering.size()
            + moving
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct MaxByFunction {
    signature: Signature,
}

impl Debug for MaxByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(input_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl AggregateUDFImpl for MaxByFunction {
    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            true,
        )?))
    }

    fn create_sliding_accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(SlidingMaxMinByAccumulator::try_new(
            &value_type,
            &ordering_type,
            true,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "value"), value_type, true).into(),
            Field::new(
                format_state_name(args.name, "ordering"),
                ordering_type,
                true,
            )
            .into(),
        ])
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: AggregateFunction, _: &SimplifyContext| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let null_filter = second_arg.clone().is_not_null();
            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(null_filter))),
                None => Some(Box::new(null_filter)),
            };

            // ASC NULLS FIRST orders nested values like Spark; NULL orderings are filtered out.
            let options = spark_nested_sort_options(false);
            order_by.push(Sort::new(
                second_arg,
                !options.descending,
                options.nulls_first,
            ));

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(AggregateUDF::from(LastValue::new())),
                vec![first_arg],
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct MinByFunction {
    signature: Signature,
}

impl Debug for MinByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MinByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for MinByFunction {
    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            false,
        )?))
    }

    fn create_sliding_accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(SlidingMaxMinByAccumulator::try_new(
            &value_type,
            &ordering_type,
            false,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "value"), value_type, true).into(),
            Field::new(
                format_state_name(args.name, "ordering"),
                ordering_type,
                true,
            )
            .into(),
        ])
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: AggregateFunction, _: &SimplifyContext| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let null_filter = second_arg.clone().is_not_null();
            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(null_filter))),
                None => Some(Box::new(null_filter)),
            };

            // DESC NULLS LAST orders nested values like Spark; NULL orderings are filtered out.
            let options = spark_nested_sort_options(true);
            order_by.push(Sort::new(
                second_arg,
                !options.descending,
                options.nulls_first,
            ));

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(AggregateUDF::from(LastValue::new())),
                vec![first_arg],
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        DictionaryArray, FixedSizeListArray, Int32Array, LargeListArray, ListArray, StringArray,
        StructArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Fields, Int32Type};

    use super::*;

    /// Evaluates the accumulator over the frame `[i - preceding, i + following]` of each row,
    /// adding rows when they enter the frame and retracting them when they leave it, like a
    /// window with a sliding frame.
    fn sliding(
        orderings: ArrayRef,
        is_max: bool,
        preceding: usize,
        following: usize,
    ) -> Vec<Option<i32>> {
        let n = orderings.len();
        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=n as i32));
        let mut acc =
            SlidingMaxMinByAccumulator::try_new(&DataType::Int32, orderings.data_type(), is_max)
                .unwrap();
        let (mut start, mut end) = (0, 0);
        let mut result = vec![];
        for i in 0..n {
            let new_end = (i + following + 1).min(n);
            let new_start = i.saturating_sub(preceding);
            acc.update_batch(&[
                values.slice(end, new_end - end),
                orderings.slice(end, new_end - end),
            ])
            .unwrap();
            acc.retract_batch(&[
                values.slice(start, new_start - start),
                orderings.slice(start, new_start - start),
            ])
            .unwrap();
            (start, end) = (new_start, new_end);
            match acc.evaluate().unwrap() {
                ScalarValue::Int32(v) => result.push(v),
                other => panic!("unexpected value {other:?}"),
            }
        }
        result
    }

    /// Evaluates the accumulator over all rows.
    fn growing(orderings: ArrayRef, is_max: bool) -> Option<i32> {
        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=orderings.len() as i32));
        let mut acc =
            MaxMinByAccumulator::new(&DataType::Int32, orderings.data_type(), is_max).unwrap();
        acc.update_batch(&[values, orderings]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Int32(v) => v,
            other => panic!("unexpected value {other:?}"),
        }
    }

    fn struct_orderings(a: Vec<Option<i32>>, valid: Vec<bool>) -> ArrayRef {
        Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("a", DataType::Int32, true)]),
                vec![Arc::new(Int32Array::from(a)) as ArrayRef],
                Some(NullBuffer::from(valid)),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_ties_newer_row_wins() {
        // The orderings of `VALUES (1, 1), (2, 1), (3, NULL), (4, 2), (5, 2), (6, 0), (7, 2),
        // (8, NULL)`, for which Spark returns 7 for `max_by` and 6 for `min_by` over the whole
        // partition, and the newer row on ties in sliding frames.
        let orderings: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            Some(2),
            Some(2),
            Some(0),
            Some(2),
            None,
        ]));
        assert_eq!(growing(Arc::clone(&orderings), true), Some(7));
        assert_eq!(growing(Arc::clone(&orderings), false), Some(6));
        // ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        assert_eq!(
            sliding(Arc::clone(&orderings), true, 2, 0),
            [1, 2, 2, 4, 5, 5, 7, 7].map(Some)
        );
        assert_eq!(
            sliding(Arc::clone(&orderings), false, 2, 0),
            [1, 2, 2, 2, 5, 6, 6, 6].map(Some)
        );
        // ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
        let following = |is_max| {
            let n = orderings.len();
            (0..n)
                .map(|i| {
                    let start = (i + 1).min(n);
                    let end = (i + 3).min(n);
                    let mut acc = SlidingMaxMinByAccumulator::try_new(
                        &DataType::Int32,
                        &DataType::Int32,
                        is_max,
                    )
                    .unwrap();
                    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=n as i32));
                    acc.update_batch(&[
                        values.slice(start, end - start),
                        orderings.slice(start, end - start),
                    ])
                    .unwrap();
                    match acc.evaluate().unwrap() {
                        ScalarValue::Int32(v) => v,
                        other => panic!("unexpected value {other:?}"),
                    }
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            following(true),
            [
                Some(2),
                Some(4),
                Some(5),
                Some(5),
                Some(7),
                Some(7),
                None,
                None
            ]
        );
    }

    #[test]
    fn test_struct_null_fields_are_smallest() {
        // `(1, {a: 1}), (2, {a: NULL}), (3, {a: NULL}), (4, NULL), (5, {a: 0})`
        let orderings = struct_orderings(
            vec![Some(1), None, None, None, Some(0)],
            vec![true, true, true, false, true],
        );
        assert_eq!(growing(Arc::clone(&orderings), true), Some(1));
        assert_eq!(growing(Arc::clone(&orderings), false), Some(3));
        // ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        assert_eq!(
            sliding(Arc::clone(&orderings), true, 1, 0),
            [1, 1, 3, 3, 5].map(Some)
        );
        assert_eq!(sliding(orderings, false, 1, 0), [1, 2, 3, 3, 5].map(Some));
    }

    #[test]
    fn test_list_null_elements_are_smallest() {
        // `(1, [1]), (2, [NULL]), (3, [1, NULL]), (4, NULL), (5, [0])`
        let rows = vec![
            Some(vec![Some(1)]),
            Some(vec![None]),
            Some(vec![Some(1), None]),
            None,
            Some(vec![Some(0)]),
        ];
        let lists: Vec<ArrayRef> = vec![
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                rows.clone(),
            )),
            Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(rows)),
        ];
        for orderings in lists {
            assert_eq!(growing(Arc::clone(&orderings), true), Some(3));
            assert_eq!(growing(Arc::clone(&orderings), false), Some(2));
            // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            assert_eq!(
                sliding(Arc::clone(&orderings), true, 1, 1),
                [1, 3, 3, 3, 5].map(Some)
            );
            assert_eq!(sliding(orderings, false, 1, 1), [2, 2, 2, 5, 5].map(Some));
        }
    }

    #[test]
    fn test_fixed_size_list_null_elements_are_smallest() {
        // `(1, [1, 1]), (2, [NULL, 5]), (3, [1, NULL]), (4, NULL), (5, [0, 0])`
        let orderings: ArrayRef =
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(1), Some(1)]),
                    Some(vec![None, Some(5)]),
                    Some(vec![Some(1), None]),
                    None,
                    Some(vec![Some(0), Some(0)]),
                ],
                2,
            ));
        assert_eq!(growing(Arc::clone(&orderings), true), Some(1));
        assert_eq!(growing(Arc::clone(&orderings), false), Some(2));
        assert_eq!(
            sliding(Arc::clone(&orderings), true, 1, 0),
            [1, 1, 3, 3, 5].map(Some)
        );
        assert_eq!(sliding(orderings, false, 1, 0), [1, 2, 2, 3, 5].map(Some));
    }

    #[test]
    fn test_dictionary_orderings() {
        // `(1, 'b'), (2, 'a'), (3, 'a'), (4, 'c')`, plus a NULL dictionary value and key.
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(1), Some(2), Some(3), None]);
        let values = StringArray::from(vec![Some("b"), Some("a"), Some("c"), None]);
        let orderings: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());
        assert_eq!(growing(Arc::clone(&orderings), true), Some(4));
        assert_eq!(growing(Arc::clone(&orderings), false), Some(3));
        assert_eq!(
            sliding(Arc::clone(&orderings), true, 1, 0),
            [Some(1), Some(1), Some(3), Some(4), Some(4), None]
        );
        assert_eq!(
            sliding(orderings, false, 1, 0),
            [Some(1), Some(2), Some(3), Some(3), Some(4), None]
        );
    }

    #[test]
    fn test_sliding_all_null_orderings() {
        let orderings: ArrayRef = Arc::new(Int32Array::from(vec![None, None, Some(1), None]));
        assert_eq!(
            sliding(Arc::clone(&orderings), true, 0, 0),
            [None, None, Some(3), None]
        );
        assert_eq!(
            sliding(orderings, false, 1, 0),
            [None, None, Some(3), Some(3)]
        );
    }
}
