use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion_common::Result;
use datafusion_common::utils::take_function_args;
use datafusion_expr::ReturnFieldArgs;

/// Identity function that only exists to force its output's declared `nullable`
/// flag to `true`, independent of whatever `nullable()` the wrapped expression
/// computes on its own.
///
/// This is needed because DataFusion's own nullability computation for some
/// expressions (e.g. `Case`) is more precise than Spark's: DataFusion excludes a
/// `WHEN` branch guarded by a literal-`false` condition from the OR of branch
/// nullability, while Spark's `CaseWhen.nullable` (conditionalExpressions.scala:191-200)
/// is `branches.exists(_.nullable) || elseValue.forall(_.nullable)` unconditionally,
/// with no such reachability analysis. Neither `Cast::new_from_field` nor any other
/// field-metadata override changes a wrapped expression's own `nullable()` -- it is
/// always inherited from the inner expression -- so matching Spark here requires an
/// actual identity UDF whose `return_field_from_args` declares nullability directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkForceNullable {
    signature: Signature,
}

impl Default for SparkForceNullable {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkForceNullable {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkForceNullable {
    fn name(&self) -> &str {
        "spark_force_nullable"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = take_function_args(self.name(), arg_types)?;
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            field.data_type().clone(),
            true,
        )))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        Ok(arg)
    }
}
