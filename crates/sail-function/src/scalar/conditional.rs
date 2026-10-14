use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::unary;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int32Type, Int64Type};
use datafusion_common::{Result, ScalarValue, internal_err, plan_datafusion_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    expr,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// Preserve Spark's CASE schema until all branches have been analyzed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkCaseWhen {
    signature: Signature,
}

impl Default for SparkCaseWhen {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCaseWhen {
    fn name(&self) -> &str {
        "spark_case_when"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return Err(plan_datafusion_err!(
                "CASE requires at least one WHEN/THEN pair"
            ));
        }
        let then_types = arg_types
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| pair[1].clone())
            .collect::<Vec<_>>();
        let else_type = if arg_types.len().is_multiple_of(2) {
            None
        } else {
            arg_types.last()
        };
        let common = get_coerce_type_for_case_expression(&then_types, else_type)
            .ok_or_else(|| plan_datafusion_err!("Cannot coerce CASE branches to common types"))?;
        Ok(then_types
            .iter()
            .chain(else_type)
            .find(|t| !t.is_null())
            .map_or(common.clone(), |source| {
                preserve_nested_metadata(source, &common)
            }))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let mut nullable = false;
        let mut exhaustive = false;
        for (index, pair) in args.arg_fields.as_chunks::<2>().0.iter().enumerate() {
            nullable |= pair[1].is_nullable();
            if matches!(
                args.scalar_arguments[index * 2],
                Some(ScalarValue::Boolean(Some(true)))
            ) {
                exhaustive = true;
                break;
            }
        }
        if !exhaustive {
            nullable |= args.arg_fields.len().is_multiple_of(2)
                || args
                    .arg_fields
                    .last()
                    .is_none_or(|field| field.is_nullable());
        }
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let common = self.return_type(arg_types)?;
        Ok((0..arg_types.len())
            .map(|index| {
                if index % 2 == 0 && index + 1 < arg_types.len() {
                    DataType::Boolean
                } else {
                    common.clone()
                }
            })
            .collect())
    }

    fn simplify(&self, mut args: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let else_expr = if args.len().is_multiple_of(2) {
            None
        } else {
            args.pop().map(Box::new)
        };
        let mut args = args.into_iter();
        let mut when_then_expr = Vec::new();
        while let (Some(condition), Some(value)) = (args.next(), args.next()) {
            when_then_expr.push((Box::new(condition), Box::new(value)));
        }
        Ok(ExprSimplifyResult::Simplified(Expr::Case(expr::Case {
            expr: None,
            when_then_expr,
            else_expr,
        })))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("spark_case_when should have been simplified to CASE")
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
