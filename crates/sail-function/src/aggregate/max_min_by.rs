use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/max_min_by.rs>
use datafusion::arrow::array::{Array, ArrayRef, ListArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use datafusion::common::cast::as_list_array;
use datafusion::common::{HashSet, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, Sort};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ScalarUDF, Signature, Volatility, function,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::prelude::Expr;
use sail_common_datafusion::ordering::{
    contains_float, is_orderable, is_orderable_field, normalize_floats_for_ordering,
};

use crate::error::{generic_exec_err, generic_internal_err, invalid_arg_count_exec_err};
use crate::scalar::spark_ordering_key::SparkOrderingKey;

/// Builds the converter that encodes an ordering key in Spark's order.
///
/// `ScalarValue::partial_cmp` does not follow Spark: it treats a NULL struct field as equal to
/// anything, a NULL list element as the greatest value, and `-0.0` as less than `0.0`. The row
/// format orders a nested NULL first, as Spark does, once floats are normalized by
/// `normalize_floats_for_ordering`.
fn ordering_converter(ordering_type: &DataType) -> Result<RowConverter, DataFusionError> {
    Ok(RowConverter::new(vec![SortField::new_with_options(
        ordering_type.clone(),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])?)
}

/// Encodes a batch of ordering keys, returning them with the rows whose key is NULL.
fn ordering_keys(
    converter: &RowConverter,
    ordering: &ArrayRef,
) -> Result<(Rows, Option<NullBuffer>), DataFusionError> {
    // A `NullArray` has no validity buffer, so only the logical nulls see a VOID key as NULL.
    let nulls = ordering.logical_nulls();
    let keys = converter.convert_columns(&[normalize_floats_for_ordering(ordering)?])?;
    Ok((keys, nulls))
}

#[derive(Debug)]
struct MaxMinByAccumulator {
    value: ScalarValue,
    ordering: ScalarValue,
    /// `ordering` encoded in Spark's order, or `None` until a non-null key has been seen.
    ordering_key: Option<OwnedRow>,
    converter: RowConverter,
    is_max: bool,
}

impl MaxMinByAccumulator {
    fn new(
        value_type: &DataType,
        ordering_type: &DataType,
        is_max: bool,
    ) -> Result<Self, DataFusionError> {
        let converter = ordering_converter(ordering_type)?;
        Ok(Self {
            value: ScalarValue::try_from(value_type)?,
            ordering: ScalarValue::try_from(ordering_type)?,
            ordering_key: None,
            converter,
            is_max,
        })
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let value_array = &values[0];
        let ordering_array = &values[1];
        let (keys, ordering_nulls) = ordering_keys(&self.converter, ordering_array)?;

        for i in 0..ordering_array.len() {
            if ordering_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(i))
            {
                continue;
            }
            let key = keys.row(i);
            let should_update = match &self.ordering_key {
                None => true,
                Some(current) => match key.cmp(&current.row()) {
                    Ordering::Greater => self.is_max,
                    Ordering::Less => !self.is_max,
                    // Spark's predicate is strict (`If(old > new, old, new)`), so a tie takes
                    // the newer row.
                    Ordering::Equal => true,
                },
            };
            if should_update {
                self.value = ScalarValue::try_from_array(value_array, i)?;
                self.ordering = ScalarValue::try_from_array(ordering_array, i)?;
                self.ordering_key = Some(key.owned());
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        Ok(vec![self.value.clone(), self.ordering.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        self.value.size()
            + self.ordering.size()
            + self
                .ordering_key
                .as_ref()
                .map_or(0, |key| key.row().as_ref().len())
            + self.converter.size()
            + std::mem::size_of::<bool>()
    }
}

/// One row retained by the top-k form.
#[derive(Debug)]
struct TopKEntry {
    key: OwnedRow,
    ordering: ScalarValue,
    value: ScalarValue,
}

impl TopKEntry {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.key.row().as_ref().len()
            + self.ordering.size()
            + self.value.size()
    }
}

/// Builds `MaxMinByK`'s result: the values in order, or a NULL array when there are none.
fn top_k_result<'a>(
    value_type: &DataType,
    values: impl Iterator<Item = &'a ScalarValue>,
) -> ScalarValue {
    let values = values.cloned().collect::<Vec<_>>();
    if values.is_empty() {
        return ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(Field::new_list_field(value_type.clone(), true)),
            1,
        )));
    }
    ScalarValue::List(ScalarValue::new_list_nullable(&values, value_type))
}

/// The accumulator for the top-k form, mirroring `MaxMinByK` and `MaxMinByKHeap`.
///
/// Spark keeps a `k`-element heap. This keeps a vector that is sorted and cut back to `k`
/// whenever it reaches `2 * k`: the same O(k) memory bound and O(n log k) amortised work. The
/// sort is stable, so of the rows tied at the boundary the older ones stay, as in Spark's heap,
/// which only replaces its root with a strictly more extreme row.
///
/// Spark removes duplicate `(value, ordering)` pairs for a `DISTINCT` aggregate, so the pairs
/// already seen are remembered for as long as the accumulator lives.
#[derive(Debug)]
struct MaxMinByKAccumulator {
    /// Never longer than `2 * k`.
    entries: Vec<TopKEntry>,
    seen: Option<HashSet<(ScalarValue, ScalarValue)>>,
    converter: RowConverter,
    value_type: DataType,
    ordering_type: DataType,
    k: usize,
    is_max: bool,
}

impl MaxMinByKAccumulator {
    fn new(
        value_type: &DataType,
        ordering_type: &DataType,
        k: usize,
        is_max: bool,
        distinct: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            entries: Vec::new(),
            seen: distinct.then(HashSet::default),
            converter: ordering_converter(ordering_type)?,
            value_type: value_type.clone(),
            ordering_type: ordering_type.clone(),
            k,
            is_max,
        })
    }

    fn push_batch(
        &mut self,
        value_array: &ArrayRef,
        ordering_array: &ArrayRef,
    ) -> Result<(), DataFusionError> {
        let (keys, ordering_nulls) = ordering_keys(&self.converter, ordering_array)?;
        for i in 0..ordering_array.len() {
            // Spark skips a NULL ordering only; a NULL value is kept.
            if ordering_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(i))
            {
                continue;
            }
            let ordering = ScalarValue::try_from_array(ordering_array, i)?;
            let value = ScalarValue::try_from_array(value_array, i)?;
            if let Some(seen) = self.seen.as_mut()
                && !seen.insert((value.clone(), ordering.clone()))
            {
                continue;
            }
            self.entries.push(TopKEntry {
                key: keys.row(i).owned(),
                ordering,
                value,
            });
            if self.entries.len() >= self.k.saturating_mul(2) {
                self.compact();
            }
        }
        Ok(())
    }

    /// Sorts the most extreme rows first and drops everything past `k`.
    fn compact(&mut self) {
        let is_max = self.is_max;
        self.entries.sort_by(|a, b| {
            let ordering = a.key.cmp(&b.key);
            if is_max { ordering.reverse() } else { ordering }
        });
        self.entries.truncate(self.k);
    }
}

impl Accumulator for MaxMinByKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let function_name = max_min_by_name(self.is_max);
        let (value_array, ordering_array, _) = max_min_by_top_k_args(function_name, values)?;
        self.push_batch(value_array, ordering_array)
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        self.compact();
        Ok(top_k_result(
            &self.value_type,
            self.entries.iter().map(|entry| &entry.value),
        ))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        self.compact();
        let values = self
            .entries
            .iter()
            .map(|entry| entry.value.clone())
            .collect::<Vec<_>>();
        let orderings = self
            .entries
            .iter()
            .map(|entry| entry.ordering.clone())
            .collect::<Vec<_>>();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&values, &self.value_type)),
            ScalarValue::List(ScalarValue::new_list_nullable(
                &orderings,
                &self.ordering_type,
            )),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        let (values_state, orderings_state) =
            max_min_by_args(max_min_by_name(self.is_max), states)?;
        let values_list = as_list_array(values_state)?;
        let orderings_list = as_list_array(orderings_state)?;
        for i in 0..values_list.len() {
            if values_list.is_null(i) || orderings_list.is_null(i) {
                continue;
            }
            self.push_batch(&values_list.value(i), &orderings_list.value(i))?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.entries.iter().map(TopKEntry::size).sum::<usize>()
            + self.seen.as_ref().map_or(0, |seen| {
                seen.iter()
                    .map(|(value, ordering)| value.size() + ordering.size())
                    .sum::<usize>()
            })
            + self.converter.size()
    }
}

/// The window accumulator for a frame that does not start at `UNBOUNDED PRECEDING`.
///
/// Rows leave a sliding frame in the order they entered it, so every row in the frame is kept:
/// the non-null keys in a sorted map, and all keys in a queue in arrival order that
/// `retract_batch` pops from. The same map answers the top-k form, whose result is its first
/// `k` entries.
#[derive(Debug)]
struct SlidingMaxMinByAccumulator {
    /// Keyed by the ordering key and a sequence number chosen so that, among equal keys, the
    /// newest row is the last entry for `max_by` and the first entry for `min_by`.
    rows: BTreeMap<(OwnedRow, u64), (ScalarValue, ScalarValue)>,
    arrivals: VecDeque<Option<(OwnedRow, u64)>>,
    arrived: u64,
    heap_size: usize,
    converter: RowConverter,
    value_type: DataType,
    k: Option<usize>,
    is_max: bool,
}

impl SlidingMaxMinByAccumulator {
    fn new(
        value_type: &DataType,
        ordering_type: &DataType,
        k: Option<usize>,
        is_max: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            rows: BTreeMap::new(),
            arrivals: VecDeque::new(),
            arrived: 0,
            heap_size: 0,
            converter: ordering_converter(ordering_type)?,
            value_type: value_type.clone(),
            k,
            is_max,
        })
    }

    /// The rows in the frame, most extreme first.
    fn ranked(&self) -> Box<dyn Iterator<Item = &(ScalarValue, ScalarValue)> + '_> {
        if self.is_max {
            Box::new(self.rows.values().rev())
        } else {
            Box::new(self.rows.values())
        }
    }

    fn entry_size(key: &(OwnedRow, u64), row: &(ScalarValue, ScalarValue)) -> usize {
        key.0.row().as_ref().len() + row.0.size() + row.1.size()
    }
}

impl Accumulator for SlidingMaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let function_name = max_min_by_name(self.is_max);
        let (value_array, ordering_array, _) = max_min_by_top_k_args(function_name, values)?;
        let (keys, ordering_nulls) = ordering_keys(&self.converter, ordering_array)?;

        for i in 0..ordering_array.len() {
            if ordering_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(i))
            {
                self.arrivals.push_back(None);
                continue;
            }
            let sequence = if self.is_max {
                self.arrived
            } else {
                u64::MAX - self.arrived
            };
            self.arrived += 1;
            let key = (keys.row(i).owned(), sequence);
            let row = (
                ScalarValue::try_from_array(value_array, i)?,
                ScalarValue::try_from_array(ordering_array, i)?,
            );
            self.heap_size += Self::entry_size(&key, &row);
            self.arrivals.push_back(Some(key.clone()));
            self.rows.insert(key, row);
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let function_name = max_min_by_name(self.is_max);
        let (_, ordering, _) = max_min_by_top_k_args(function_name, values)?;
        for _ in 0..ordering.len() {
            let Some(arrival) = self.arrivals.pop_front() else {
                return Err(generic_internal_err(
                    function_name,
                    "retracted more rows than the window frame holds",
                ));
            };
            if let Some(key) = arrival
                && let Some(row) = self.rows.remove(&key)
            {
                self.heap_size -= Self::entry_size(&key, &row);
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        match self.k {
            Some(k) => Ok(top_k_result(
                &self.value_type,
                self.ranked().take(k).map(|(value, _)| value),
            )),
            None => match self.ranked().next() {
                Some((value, _)) => Ok(value.clone()),
                None => Ok(ScalarValue::try_from(&self.value_type)?),
            },
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        // A window frame is evaluated in place and never merged, so no state is needed.
        Err(generic_internal_err(
            max_min_by_name(self.is_max),
            "a sliding window accumulator has no partial state",
        ))
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<(), DataFusionError> {
        Err(generic_internal_err(
            max_min_by_name(self.is_max),
            "a sliding window accumulator has no partial state",
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.heap_size + self.converter.size()
    }
}

fn max_min_by_name(is_max: bool) -> &'static str {
    if is_max { "max_by" } else { "min_by" }
}

/// The argument count Spark 4.2 accepts: `MaxByBuilder` and `MinByBuilder` route a third
/// argument to the top-k form `MaxMinByK`, which returns `ARRAY<value type>`.
const MAX_MIN_BY_ARG_COUNT: (i32, i32) = (2, 3);

/// The partial-aggregation state, which is a `(value, ordering)` pair in either form.
const MAX_MIN_BY_STATE_COUNT: (i32, i32) = (2, 2);

/// `MaxMinByK.MAX_K`.
pub const MAX_MIN_BY_MAX_K: i32 = 100_000;

/// Splits a partial-aggregation state into the value and the ordering.
fn max_min_by_args<'a, T>(
    function_name: &str,
    args: &'a [T],
) -> Result<(&'a T, &'a T), DataFusionError> {
    match args {
        [value, ordering] => Ok((value, ordering)),
        _ => Err(invalid_arg_count_exec_err(
            function_name,
            MAX_MIN_BY_STATE_COUNT,
            args.len(),
        )),
    }
}

/// Splits the arguments into the value, the ordering, and the `k` of the top-k form.
///
/// The signature is `user_defined`, so DataFusion runs no arity check of its own. Every
/// hook that reaches for an argument must go through here, or it indexes a short slice and
/// panics instead of reporting an error.
fn max_min_by_top_k_args<'a, T>(
    function_name: &str,
    args: &'a [T],
) -> Result<(&'a T, &'a T, Option<&'a T>), DataFusionError> {
    match args {
        [value, ordering] => Ok((value, ordering, None)),
        [value, ordering, k] => Ok((value, ordering, Some(k))),
        _ => Err(invalid_arg_count_exec_err(
            function_name,
            MAX_MIN_BY_ARG_COUNT,
            args.len(),
        )),
    }
}

/// Checks `k` against `MaxMinByK`'s range. A NULL reads as zero, which is what
/// `kExpr.eval().asInstanceOf[Int]` yields in Scala.
pub fn check_max_min_by_k(function_name: &str, k: Option<i32>) -> Result<usize, DataFusionError> {
    let k = k.unwrap_or(0);
    match usize::try_from(k) {
        Ok(k) if (1..=MAX_MIN_BY_MAX_K as usize).contains(&k) => Ok(k),
        _ => Err(generic_exec_err(
            function_name,
            &format!("The `k` must be between [1, {MAX_MIN_BY_MAX_K}] (current value = {k})"),
        )),
    }
}

/// Reads `k` from the physical arguments.
///
/// `coerce_types` casts it to INT and the optimizer folds a foldable expression to a literal,
/// so anything else is a non-foldable `k`, which Spark rejects in analysis.
fn physical_k(
    function_name: &str,
    exprs: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<usize>, DataFusionError> {
    let (_, _, k) = max_min_by_top_k_args(function_name, exprs)?;
    let Some(k) = k else {
        return Ok(None);
    };
    match k.downcast_ref::<Literal>().map(|literal| literal.value()) {
        Some(ScalarValue::Int32(value)) => Ok(Some(check_max_min_by_k(function_name, *value)?)),
        _ => Err(generic_exec_err(
            function_name,
            &format!("the input k should be a foldable int expression; however, got {k}"),
        )),
    }
}

fn get_min_max_by_result_type(
    function_name: &str,
    input_types: &[DataType],
) -> Result<Vec<DataType>, DataFusionError> {
    let (value_type, ordering_type, k_type) = max_min_by_top_k_args(function_name, input_types)?;
    if !is_orderable(ordering_type) {
        return Err(generic_exec_err(
            function_name,
            &format!("does not support ordering on type {ordering_type}"),
        ));
    }
    // Answering a shorter list than it was given makes DataFusion reject the call with a
    // `Failed to coerce arguments` planning error before any hook below runs, so every
    // argument is carried over and only the ones that need rewriting are replaced.
    let mut coerced = input_types.to_vec();
    // `MaxMinByK` is `ImplicitCastInputTypes` with `k: IntegerType`, so `k` is cast to INT, which
    // Spark allows implicitly only from a numeric type, a string, or NULL.
    if let Some(k_type) = k_type {
        let castable = k_type.is_null() || k_type.is_numeric() || k_type.is_string();
        if !castable {
            return Err(generic_exec_err(
                function_name,
                &format!(
                    "The third parameter requires the \"INT\" type, however the input has the type {k_type}"
                ),
            ));
        }
        if let Some(k) = coerced.get_mut(2) {
            *k = DataType::Int32;
        }
    }
    // The value type is unwrapped from a dictionary so that the accumulator stores the plain
    // value. Not covered by a scenario because a `Dictionary` column cannot be built from SQL.
    if let DataType::Dictionary(_, dict_value_type) = value_type {
        // TODO add checker, if the value type is complex data type
        if let Some(first) = coerced.first_mut() {
            *first = dict_value_type.deref().clone();
        }
    }
    Ok(coerced)
}

/// The orderability check that `coerce_types` cannot make.
///
/// `coerce_types` only sees `DataType`s, so it misses the Spark types Sail carries in field
/// metadata — GEOMETRY/GEOGRAPHY, and a VARIANT whose child fields are unmarked. `return_field`
/// receives the real argument fields, so the check is repeated here to catch them.
fn check_ordering_field(
    function_name: &str,
    arg_fields: &[FieldRef],
) -> Result<(), DataFusionError> {
    let (_, ordering_field, _) = max_min_by_top_k_args(function_name, arg_fields)?;
    if !is_orderable_field(ordering_field) {
        return Err(generic_exec_err(
            function_name,
            &format!(
                "does not support ordering on type {}",
                ordering_field.data_type()
            ),
        ));
    }
    Ok(())
}

/// Spark's `dataType`: the value type, or `ARRAY<value type>` with nullable elements for the
/// top-k form, and `nullable = true` either way.
///
/// The value field is carried over rather than rebuilt from its `DataType`, because Sail keeps
/// logical types such as GEOMETRY and GEOGRAPHY in the field metadata.
fn min_max_by_return_field(
    function_name: &str,
    arg_fields: &[FieldRef],
) -> Result<FieldRef, DataFusionError> {
    check_ordering_field(function_name, arg_fields)?;
    let (value_field, _, k) = max_min_by_top_k_args(function_name, arg_fields)?;
    let value_field = value_field.as_ref().clone().with_nullable(true);
    let field = match k {
        None => value_field.with_name(function_name),
        Some(_) => Field::new(
            function_name,
            DataType::List(Arc::new(
                value_field.with_name(Field::LIST_FIELD_DEFAULT_NAME),
            )),
            true,
        ),
    };
    Ok(Arc::new(field))
}

fn min_max_by_accumulator(
    is_max: bool,
    acc_args: AccumulatorArgs,
) -> Result<Box<dyn Accumulator>, DataFusionError> {
    let function_name = max_min_by_name(is_max);
    let (_, ordering, _) = max_min_by_top_k_args(function_name, acc_args.exprs)?;
    let ordering_type = ordering.data_type(acc_args.schema)?;
    let return_type = acc_args.return_field.data_type();
    match physical_k(function_name, acc_args.exprs)? {
        None => Ok(Box::new(MaxMinByAccumulator::new(
            return_type,
            &ordering_type,
            is_max,
        )?)),
        Some(k) => Ok(Box::new(MaxMinByKAccumulator::new(
            top_k_element_type(function_name, return_type)?,
            &ordering_type,
            k,
            is_max,
            acc_args.is_distinct,
        )?)),
    }
}

fn min_max_by_sliding_accumulator(
    is_max: bool,
    acc_args: AccumulatorArgs,
) -> Result<Box<dyn Accumulator>, DataFusionError> {
    let function_name = max_min_by_name(is_max);
    let (_, ordering, _) = max_min_by_top_k_args(function_name, acc_args.exprs)?;
    let ordering_type = ordering.data_type(acc_args.schema)?;
    let return_type = acc_args.return_field.data_type();
    let k = physical_k(function_name, acc_args.exprs)?;
    let value_type = match k {
        None => return_type,
        Some(_) => top_k_element_type(function_name, return_type)?,
    };
    Ok(Box::new(SlidingMaxMinByAccumulator::new(
        value_type,
        &ordering_type,
        k,
        is_max,
    )?))
}

fn top_k_element_type<'a>(
    function_name: &str,
    return_type: &'a DataType,
) -> Result<&'a DataType, DataFusionError> {
    match return_type {
        DataType::List(element) => Ok(element.data_type()),
        _ => Err(generic_internal_err(
            function_name,
            "the top-k form must return an array",
        )),
    }
}

/// The partial-aggregation state, Spark's `valuesAttr` and `orderingsAttr`: a single value and
/// ordering, or the retained ones as arrays for the top-k form.
fn min_max_by_state_fields(
    function_name: &str,
    args: StateFieldsArgs,
) -> Result<Vec<FieldRef>, DataFusionError> {
    let (_, ordering, k) = max_min_by_top_k_args(function_name, args.input_fields)?;
    let ordering_type = match k {
        None => ordering.data_type().clone(),
        Some(_) => DataType::List(Arc::new(Field::new_list_field(
            ordering.data_type().clone(),
            true,
        ))),
    };
    Ok(vec![
        Field::new(
            format_state_name(args.name, "value"),
            args.return_field.data_type().clone(),
            true,
        )
        .into(),
        Field::new(
            format_state_name(args.name, "ordering"),
            ordering_type,
            true,
        )
        .into(),
    ])
}

/// Rewrites `max_by(x, y)` to `last_value(x ORDER BY y) FILTER (WHERE y IS NOT NULL)`, and
/// `min_by(x, y)` to the same with `first_value`.
///
/// Both sort ascending with nulls first, because that ranks a NULL nested inside the key below
/// any non-null value, as Spark's ordering does; a descending sort would rank it above. A key
/// that holds floats is wrapped in [`SparkOrderingKey`] so that `-0.0` and `0.0` tie.
///
/// The top-k form is returned unchanged and runs its own accumulator. `ExprSimplifier` counts
/// any call to this hook as a change, so such a call is re-simplified until the simplifier's
/// cycle limit; that terminates, and the hook has no way to report "unchanged".
fn rewrite_min_max_by(
    is_max: bool,
    mut aggr_func: AggregateFunction,
    info: &SimplifyContext,
) -> Result<Expr, DataFusionError> {
    let function_name = max_min_by_name(is_max);
    let (_, _, k) = max_min_by_top_k_args(function_name, &aggr_func.params.args)?;
    if k.is_some() {
        return Ok(Expr::AggregateFunction(aggr_func));
    }
    let mut order_by = aggr_func.params.order_by;
    let (ordering, value) = (
        aggr_func.params.args.remove(1),
        aggr_func.params.args.remove(0),
    );

    let null_filter = ordering.clone().is_not_null();
    let filter = match aggr_func.params.filter {
        Some(existing) => Some(Box::new((*existing).and(null_filter))),
        None => Some(Box::new(null_filter)),
    };

    // `NullType` is orderable in Spark, so the orderability check lets `max_by(x, NULL)` and
    // `max_by(x, CAST(NULL AS VOID))` through to here. Sorting by a literal is a no-op, and a
    // constant sort key makes DataFusion's ordered `last_value` panic on a global aggregate, so
    // the key is only pushed when it is not one. Foldable arguments reach this point already
    // reduced to a literal.
    if !matches!(ordering, Expr::Literal(_, _)) {
        let key = if contains_float(&info.get_data_type(&ordering)?) {
            Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::new(ScalarUDF::from(SparkOrderingKey::new())),
                vec![ordering],
            ))
        } else {
            ordering
        };
        order_by.push(Sort::new(key, true, true));
    }

    let udaf = if is_max {
        last_value_udaf()
    } else {
        first_value_udaf()
    };
    Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        vec![value],
        aggr_func.params.distinct,
        filter,
        order_by,
        aggr_func.params.null_treatment,
    )))
}

macro_rules! define_max_min_by_udaf {
    ($udaf:ident, $name:expr_2021, $debug_name:expr_2021, $is_max:expr_2021) => {
        #[derive(PartialEq, Eq, Hash)]
        pub struct $udaf {
            signature: Signature,
        }

        impl Debug for $udaf {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.debug_struct($debug_name)
                    .field("name", &self.name())
                    .field("signature", &self.signature)
                    .field("accumulator", &"<FUNC>")
                    .finish()
            }
        }

        impl Default for $udaf {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $udaf {
            pub fn new() -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                }
            }
        }

        impl AggregateUDFImpl for $udaf {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
                let (value_type, _, k) = max_min_by_top_k_args(self.name(), arg_types)?;
                Ok(match k {
                    None => value_type.clone(),
                    Some(_) => {
                        DataType::List(Arc::new(Field::new_list_field(value_type.clone(), true)))
                    }
                })
            }

            fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef, DataFusionError> {
                min_max_by_return_field(self.name(), arg_fields)
            }

            fn accumulator(
                &self,
                acc_args: AccumulatorArgs,
            ) -> Result<Box<dyn Accumulator>, DataFusionError> {
                min_max_by_accumulator($is_max, acc_args)
            }

            fn create_sliding_accumulator(
                &self,
                acc_args: AccumulatorArgs,
            ) -> Result<Box<dyn Accumulator>, DataFusionError> {
                min_max_by_sliding_accumulator($is_max, acc_args)
            }

            fn state_fields(
                &self,
                args: StateFieldsArgs,
            ) -> Result<Vec<FieldRef>, DataFusionError> {
                min_max_by_state_fields(self.name(), args)
            }

            fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
                Some(Box::new(|aggr_func, info| {
                    rewrite_min_max_by($is_max, aggr_func, info)
                }))
            }

            fn coerce_types(
                &self,
                arg_types: &[DataType],
            ) -> Result<Vec<DataType>, DataFusionError> {
                get_min_max_by_result_type(self.name(), arg_types)
            }
        }
    };
}

define_max_min_by_udaf!(MaxByFunction, "max_by", "MaxBy", true);
define_max_min_by_udaf!(MinByFunction, "min_by", "MinBy", false);
