use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::plan_err;
use datafusion_expr::ScalarFunctionArgs;

use crate::udf_utils::arg_data_types;

pub fn explode_name_to_kind(name: &str) -> Result<ExplodeKind> {
    match name {
        "explode" => Ok(ExplodeKind::Explode),
        "explode_outer" => Ok(ExplodeKind::ExplodeOuter),
        "posexplode" => Ok(ExplodeKind::PosExplode),
        "posexplode_outer" => Ok(ExplodeKind::PosExplodeOuter),
        "inline" => Ok(ExplodeKind::Inline),
        "inline_outer" => Ok(ExplodeKind::InlineOuter),
        _ => Err(datafusion::error::DataFusionError::Plan(
            "Invalid explode function name".to_string(),
        )),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Explode {
    signature: Signature,
    kind: ExplodeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExplodeKind {
    Explode,
    ExplodeOuter,
    PosExplode,
    PosExplodeOuter,
    Inline,
    InlineOuter,
}

impl Explode {
    pub fn new(kind: ExplodeKind) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            kind,
        }
    }

    /// Public because `sail-plan`'s `ExplodeRewriter` needs the exploded element type to
    /// build the replacement projection; this UDF is a placeholder that never executes.
    pub fn output_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[DataType::List(f)]
            | &[DataType::LargeList(f)]
            | &[DataType::FixedSizeList(f, _)]
            | &[DataType::Map(f, _)] => Ok(f.data_type().clone()),
            _ => plan_err!("{} should only be called with a list or map", self.name()),
        }
    }

    pub fn kind(&self) -> &ExplodeKind {
        &self.kind
    }
}

impl ScalarUDFImpl for Explode {
    fn name(&self) -> &str {
        match self.kind {
            ExplodeKind::Explode => "explode",
            ExplodeKind::ExplodeOuter => "explode_outer",
            ExplodeKind::PosExplode => "posexplode",
            ExplodeKind::PosExplodeOuter => "posexplode_outer",
            ExplodeKind::Inline => "inline",
            ExplodeKind::InlineOuter => "inline_outer",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    crate::unused_return_type!();

    // Generator: `explode_outer` emits a NULL row for an empty or NULL input, and the
    // element type of the exploded array is nullable in Spark.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = self.output_type(&arg_data_types(&args))?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
