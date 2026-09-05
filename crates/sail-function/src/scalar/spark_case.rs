use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;

/// Preserves Spark's CASE schema while leaving the complete CASE expression
/// available to DataFusion for validation and conditional execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkCase {
    nullable: bool,
    signature: Signature,
}

impl SparkCase {
    pub fn new(nullable: bool) -> Self {
        Self {
            nullable,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCase {
    fn name(&self) -> &str {
        if self.nullable {
            "spark_case_nullable"
        } else {
            "spark_case_not_nullable"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return plan_err!("{} expects exactly one argument", self.name());
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("{} expects exactly one argument", self.name());
        };
        // CASE does not inherit field metadata from a selected result column.
        Ok(Arc::new(Field::new(
            self.name(),
            field.data_type().clone(),
            self.nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.one()
    }
}

/// Keeps casts inside CASE subject to conditional execution. DataFusion folds
/// literal CAST failures into planning errors, but defers scalar UDF failures.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkCaseCast {
    signature: Signature,
}

impl Default for SparkCaseCast {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCaseCast {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCaseCast {
    fn name(&self) -> &str {
        "spark_case_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, target_type] = arg_types else {
            return plan_err!("{} expects exactly two arguments", self.name());
        };
        Ok(target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source, target] = args.arg_fields else {
            return plan_err!("{} expects exactly two arguments", self.name());
        };
        Ok(Arc::new(
            source
                .as_ref()
                .clone()
                .with_data_type(target.data_type().clone()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (value, _) = args.args.two()?;
        // Match CastExpr's default options and scalar/array handling exactly.
        value.cast_to(args.return_field.data_type(), None)
    }
}
