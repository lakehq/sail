use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, internal_err, plan_datafusion_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, cast, expr,
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

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [tested, if_non_null, if_null]: [Expr; 3] =
            args.try_into().map_err(|args: Vec<Expr>| {
                invalid_arg_count_exec_err(self.name(), (3, 3), args.len())
            })?;
        // Keep a nullable branch in ELSE so CASE's predicate inference agrees
        // with Spark's branch-based NVL2 nullability.
        let if_null_nullable = if_null.nullable(info.schema().as_ref())?;
        let (condition, then_expr, else_expr) = if if_null_nullable {
            (tested.is_not_null(), if_non_null, if_null)
        } else {
            (tested.is_null(), if_null, if_non_null)
        };
        let result = Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(condition), Box::new(then_expr))],
            else_expr: Some(Box::new(else_expr)),
        });
        let result = if if_null_nullable {
            // TODO: Fix DataFusion's empty-batch IN-list constant detection when
            // the non-null result is scalar and the null result is a nullable column.
            result
        } else {
            // The identity UDF prevents empty-batch IN-list probes from treating
            // NVL2 as constant. Cache its type only after analysis establishes it.
            let data_type = result.get_type(info.schema().as_ref())?;
            let result =
                ScalarUDF::from(SparkConditionalCast::new(data_type.clone())).call(vec![result]);
            cast(result, data_type)
        };
        Ok(ExprSimplifyResult::Simplified(result))
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
        // TODO: Match Spark's numeric STRING grammar when shared cast support is available:
        // control-character trimming, floating-point suffixes/hex literals, and DECIMAL exponents.
        // Arrow's parser currently rejects these forms, as it does for ordinary CAST expressions.
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        match arg {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(
                value.cast_to_with_options(&self.target_type, &options)?,
            )),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_with_options(
                array,
                &self.target_type,
                &options,
            )?)),
        }
    }
}

pub fn preserve_nested_metadata(source: &DataType, target: &DataType) -> DataType {
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
