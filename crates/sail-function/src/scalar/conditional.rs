use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::unary;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int32Type, Int64Type, TimeUnit};
use datafusion_common::{Result, ScalarValue, internal_err, plan_datafusion_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    expr,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

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

/// Retain a BIGINT shift count's low six bits directly in an INT buffer.
/// Shifts inspect only these bits; masking then casting would allocate two buffers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkShiftCount {
    signature: Signature,
}

impl Default for SparkShiftCount {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkShiftCount {
    fn name(&self) -> &str {
        "spark_shift_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int32,
            source.is_nullable(),
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
        match arg {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int32(value.map(|count| (count & 63) as i32)),
            )),
            ColumnarValue::Array(array) if array.data_type() == &DataType::Int64 => {
                Ok(ColumnarValue::Array(Arc::new(unary::<
                    Int64Type,
                    _,
                    Int32Type,
                >(
                    array.as_primitive::<Int64Type>(),
                    |count| (count & 63) as i32,
                ))))
            }
            _ => Err(unsupported_data_type_exec_err(
                self.name(),
                "BIGINT",
                &arg.data_type(),
            )),
        }
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
