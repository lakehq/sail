use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::plan_err;

pub fn explode_name_to_kind(name: &str) -> Result<ExplodeKind> {
    match name {
        "explode" => Ok(ExplodeKind::Explode),
        "explode_outer" => Ok(ExplodeKind::ExplodeOuter),
        "posexplode" => Ok(ExplodeKind::PosExplode),
        "posexplode_outer" => Ok(ExplodeKind::PosExplodeOuter),
        _ => Err(datafusion::error::DataFusionError::Plan(
            "Invalid explode function name".to_string(),
        )),
    }
}

#[derive(Debug)]
pub struct Explode {
    signature: Signature,
    kind: ExplodeKind,
}

#[derive(Debug, Clone)]
pub enum ExplodeKind {
    Explode,
    ExplodeOuter,
    PosExplode,
    PosExplodeOuter,
}

impl Explode {
    pub fn new(kind: ExplodeKind) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            kind,
        }
    }

    pub fn kind(&self) -> &ExplodeKind {
        &self.kind
    }
}

impl ScalarUDFImpl for Explode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        match self.kind {
            ExplodeKind::Explode => "explode",
            ExplodeKind::ExplodeOuter => "explode_outer",
            ExplodeKind::PosExplode => "posexplode",
            ExplodeKind::PosExplodeOuter => "posexplode_outer",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[DataType::List(f)]
            | &[DataType::LargeList(f)]
            | &[DataType::FixedSizeList(f, _)]
            | &[DataType::Map(f, _)] => Ok(f.data_type().clone()),
            _ => plan_err!("{} should only be called with a list or map", self.name()),
        }
    }

    fn invoke(&self, _: &[ColumnarValue]) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
