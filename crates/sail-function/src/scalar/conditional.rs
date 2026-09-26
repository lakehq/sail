use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, StructArray, UInt64Array, make_array, new_empty_array,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options, nullif, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, internal_err, plan_datafusion_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    expr,
};

use crate::error::invalid_arg_count_exec_err;

/// Resolve NVL2's value branches after its inputs have been analyzed, then lower
/// to a lazy CASE. The tested argument does not participate in type coercion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkNvl2 {
    signature: Signature,
    session_timezone: Arc<str>,
}

impl SparkNvl2 {
    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            session_timezone,
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn lower(tested: Expr, if_non_null: Expr, if_null: Expr) -> Expr {
        // Simple CASE retains the OR of both branches' nullability, like Spark
        // IF. Searched CASE can infer a nullable THEN branch is never NULL from
        // its predicate, which would change NVL2's declared schema.
        Expr::Case(expr::Case {
            expr: Some(Box::new(tested.is_null())),
            when_then_expr: vec![(Box::new(datafusion_expr::lit(true)), Box::new(if_null))],
            else_expr: Some(Box::new(if_non_null)),
        })
    }

    fn common_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, left, right] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (3, 3),
                arg_types.len(),
            ));
        };
        let branches = [left.clone(), right.clone()];
        if branches.iter().all(|data_type| {
            matches!(
                data_type,
                DataType::Null | DataType::Date32 | DataType::Timestamp(_, _)
            )
        }) && branches
            .iter()
            .any(|data_type| matches!(data_type, DataType::Timestamp(_, _)))
        {
            // Spark timestamps have microsecond precision. DATE/TIMESTAMP
            // coercion must also retain LTZ when either branch has a timezone.
            // TODO: Match Spark's nullable DATE-to-TIMESTAMP_NTZ cast in NVL2's
            // schema; the shared cast currently preserves non-nullability.
            // TODO: Preserve stored-view temporal cast timezones during later analysis;
            // shared DATE/TIMESTAMP casts can still use the reader's timezone.
            let timezone = branches
                .iter()
                .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
                .then(|| Arc::clone(&self.session_timezone));
            return Ok(DataType::Timestamp(TimeUnit::Microsecond, timezone));
        }
        let common_type =
            get_coerce_type_for_case_expression(&branches, None).ok_or_else(|| {
                plan_datafusion_err!("Cannot find a common NVL2 type for {branches:?}")
            })?;
        let source = if left.is_null() { right } else { left };
        Ok(preserve_nested_metadata(source, &common_type))
    }
}

impl ScalarUDFImpl for SparkNvl2 {
    fn name(&self) -> &str {
        "spark_nvl2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.common_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [_, left, right] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (3, 3),
                args.arg_fields.len(),
            ));
        };
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.common_type(&arg_types)?,
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let common_type = self.common_type(arg_types)?;
        Ok(vec![arg_types[0].clone(), common_type.clone(), common_type])
    }

    fn simplify(&self, args: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [tested, if_non_null, if_null]: [Expr; 3] =
            args.try_into().map_err(|args: Vec<Expr>| {
                invalid_arg_count_exec_err(self.name(), (3, 3), args.len())
            })?;
        Ok(ExprSimplifyResult::Simplified(Self::lower(
            tested,
            if_non_null,
            if_null,
        )))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("spark_nvl2 should have been simplified to CASE")
    }
}

/// Strict casts introduced by ANSI conditional branch coercion. Keeping the cast
/// in a UDF lets DataFusion defer invalid literals in unselected CASE branches.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkConditionalCast {
    signature: Signature,
    target_type: DataType,
}

impl SparkConditionalCast {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
        }
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }
}

impl ScalarUDFImpl for SparkConditionalCast {
    fn name(&self) -> &str {
        "spark_conditional_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        if source.data_type() == &self.target_type {
            return Ok(Arc::new(source.as_ref().clone().with_name(self.name())));
        }
        let nullable = source.is_nullable()
            || (source.data_type().is_string() && self.target_type.is_numeric());
        Ok(Arc::new(Field::new(
            self.name(),
            self.target_type.clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.args.len(),
            ));
        };
        // IN-list planning probes expressions with an empty batch. Keep a row-dependent
        // NVL2 from looking constant when its CASE happens to return a scalar for that batch.
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.target_type)));
        }
        if arg.data_type() == self.target_type {
            return Ok(arg.clone());
        }
        // TODO: Match Spark's numeric STRING grammar when shared cast support is available:
        // control-character trimming, floating-point suffixes/hex literals, and DECIMAL exponents.
        // Arrow's parser currently rejects these forms, as it does for ordinary CAST expressions.
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        match arg {
            ColumnarValue::Scalar(value) if value.data_type().is_nested() => {
                let array = cast_visible_values(&value.to_array()?, &self.target_type, &options)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(
                value.cast_to_with_options(&self.target_type, &options)?,
            )),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_visible_values(
                array,
                &self.target_type,
                &options,
            )?)),
        }
    }
}

fn cast_visible_values(
    array: &ArrayRef,
    target: &DataType,
    options: &CastOptions<'_>,
) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    Ok(cast_with_options(
        &visible_nested_values(array, Some(target))?,
        target,
        options,
    )?)
}

/// Spark casts only present nested values. Arrow casts every child slot, including
/// values masked by a NULL parent or outside the offsets of a sliced list/map.
fn visible_nested_values(array: &ArrayRef, target: Option<&DataType>) -> Result<ArrayRef> {
    if target == Some(array.data_type()) || !array.data_type().is_nested() {
        return Ok(Arc::clone(array));
    }
    let sliced_values = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            list.value_offsets()[0] != 0
                || list.value_offsets()[list.len()] as usize != list.values().len()
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            list.value_offsets()[0] != 0
                || list.value_offsets()[list.len()] as usize != list.values().len()
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            map.value_offsets()[0] != 0
                || map.value_offsets()[map.len()] as usize != map.entries().len()
        }
        _ => false,
    };
    let mask_struct = matches!(array.data_type(), DataType::Struct(_)) && array.null_count() > 0;
    let array = if (array.null_count() > 0 && !mask_struct) || sliced_values {
        // List/map take drops entries belonging to null parents and unused
        // prefixes/suffixes without introducing NULLs into non-nullable entries.
        let indices =
            UInt64Array::from_iter((0..array.len()).map(|i| array.is_valid(i).then_some(i as u64)));
        take(array, &indices, None)?
    } else {
        Arc::clone(array)
    };
    let data = array.to_data();
    let parent_mask = if mask_struct {
        array
            .nulls()
            .map(|nulls| BooleanArray::new(!nulls.inner(), None))
    } else {
        None
    };
    let mut changed = mask_struct;
    // Unchanged siblings are not cast, so their hidden values need no masking or
    // compaction. In particular, avoid copying large lists beside a cast scalar.
    // For reordered structs, leave matching to Arrow and keep the existing full
    // masking path rather than assuming that source and target positions agree.
    let target_fields = match (array.data_type(), target) {
        (DataType::Struct(source), Some(DataType::Struct(target)))
            if source.len() == target.len()
                && source.iter().zip(target).all(|(a, b)| a.name() == b.name()) =>
        {
            Some(target)
        }
        _ => None,
    };
    let children = data
        .child_data()
        .iter()
        .enumerate()
        .map(|(index, child)| {
            let child = make_array(child.clone());
            let child_target = match (array.data_type(), target) {
                (
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _),
                    Some(
                        DataType::List(field)
                        | DataType::LargeList(field)
                        | DataType::FixedSizeList(field, _),
                    ),
                )
                | (DataType::Map(_, _), Some(DataType::Map(field, _))) => Some(field.data_type()),
                _ => target_fields.map(|fields| fields[index].data_type()),
            };
            if child_target == Some(child.data_type()) {
                return Ok(child);
            }
            // The kernel changes only validity, without copying or revalidating
            // value buffers (including potentially large STRING children).
            let child = match &parent_mask {
                Some(mask) => nullif(child.as_ref(), mask)?,
                None => child,
            };
            let visible = visible_nested_values(&child, child_target)?;
            changed |= !Arc::ptr_eq(&child, &visible);
            Ok(visible)
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        if let DataType::Struct(fields) = array.data_type() {
            return Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                array.nulls().cloned(),
            )?));
        }
        Ok(make_array(
            data.into_builder()
                .child_data(children.iter().map(|child| child.to_data()).collect())
                .build()?,
        ))
    } else {
        Ok(array)
    }
}

pub fn preserve_nested_metadata(source: &DataType, target: &DataType) -> DataType {
    if source == target {
        return target.clone();
    }
    let preserve_field = |source: &FieldRef, target: &FieldRef| {
        Arc::new(
            target
                .as_ref()
                .clone()
                .with_metadata(source.metadata().clone())
                .with_data_type(preserve_nested_metadata(
                    source.data_type(),
                    target.data_type(),
                )),
        )
    };
    match (source, target) {
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            DataType::Struct(
                source
                    .iter()
                    .zip(target)
                    .map(|(source, target)| preserve_field(source, target))
                    .collect(),
            )
        }
        (
            DataType::List(source)
            | DataType::LargeList(source)
            | DataType::FixedSizeList(source, _),
            target,
        ) => match target {
            DataType::List(target) => DataType::List(preserve_field(source, target)),
            DataType::LargeList(target) => DataType::LargeList(preserve_field(source, target)),
            DataType::FixedSizeList(target, size) => {
                DataType::FixedSizeList(preserve_field(source, target), *size)
            }
            _ => target.clone(),
        },
        (DataType::Map(source, _), DataType::Map(target, sorted)) => {
            DataType::Map(preserve_field(source, target), *sorted)
        }
        _ => target.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nvl2_coercion_uses_only_value_branches() -> Result<()> {
        let function = SparkNvl2::new(Arc::from("America/Los_Angeles"));
        assert_eq!(
            function.coerce_types(&[DataType::Float64, DataType::Int32, DataType::Int32])?,
            vec![DataType::Float64, DataType::Int32, DataType::Int32],
        );
        let timestamp = DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("America/Los_Angeles")),
        );
        assert_eq!(
            function.coerce_types(&[
                DataType::Int32,
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            ])?,
            vec![DataType::Int32, timestamp.clone(), timestamp],
        );
        Ok(())
    }
}
