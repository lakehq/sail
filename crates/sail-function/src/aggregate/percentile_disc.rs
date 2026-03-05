use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, ListArray, PrimitiveArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion::common::{DataFusionError, HashSet, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::aggregates::NUMERICS;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature, TypeSignature, Volatility,
};

use crate::aggregate::percentile_disc_groups::{
    DistinctPercentileDiscAccumulator, PercentileDiscGroupsAccumulator,
};
use crate::aggregate::utils::{calculate_percentile_disc, cast_to_type, validate_percentile};

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
        let mut variants = Vec::with_capacity(NUMERICS.len());
        for num in NUMERICS {
            variants.push(TypeSignature::Exact(vec![num.clone(), DataType::Float64]));
        }
        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }

    /// Extracts percentile value and adjusts for descending order.
    fn resolve_percentile(args: &AccumulatorArgs) -> Result<f64> {
        let percentile = validate_percentile(&args.exprs[1])?;

        let is_descending = args
            .order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false);

        Ok(if is_descending {
            1.0 - percentile
        } else {
            percentile
        })
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = args.input_fields[0].data_type().clone();
        let field = Field::new_list_field(input_type, true);
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
        let percentile = Self::resolve_percentile(&args)?;

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                if args.is_distinct {
                    Ok(Box::new(DistinctPercentileDiscAccumulator::<$t> {
                        data_type: $dt.clone(),
                        distinct_values: HashSet::default(),
                        percentile,
                    }))
                } else {
                    Ok(Box::new(PercentileDiscAccumulator::<$t> {
                        data_type: $dt.clone(),
                        all_values: vec![],
                        percentile,
                    }))
                }
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscAccumulator")
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let percentile = Self::resolve_percentile(&args)?;

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileDiscGroupsAccumulator::<$t>::new(
                    $dt, percentile,
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
}

impl<T: ArrowNumericType> Debug for PercentileDiscAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PercentileDiscAccumulator({}, percentile={})",
            self.data_type, self.percentile
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
        let value = calculate_percentile_disc::<T>(d, self.percentile);
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
    }
}

pub fn percentile_disc_udaf() -> Arc<datafusion::logical_expr::AggregateUDF> {
    Arc::new(datafusion::logical_expr::AggregateUDF::from(
        PercentileDisc::new(),
    ))
}
