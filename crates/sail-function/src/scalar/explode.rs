use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{internal_err, plan_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match args.arg_fields {
            [field] => match field.data_type() {
                DataType::List(element)
                | DataType::LargeList(element)
                | DataType::ListView(element)
                | DataType::LargeListView(element)
                | DataType::FixedSizeList(element, _)
                | DataType::Map(element, _) => Ok(Arc::new(
                    element
                        .as_ref()
                        .clone()
                        .with_name(self.name())
                        .with_nullable(true),
                )),
                _ => plan_err!("{} should only be called with a list or map", self.name()),
            },
            _ => plan_err!("{} should only be called with a list or map", self.name()),
        }
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn explode_preserves_element_metadata() -> Result<()> {
        let element = Arc::new(
            Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )
            .with_metadata(HashMap::from([("logical_type".into(), "element".into())])),
        );
        for data_type in [
            DataType::List(Arc::clone(&element)),
            DataType::LargeList(Arc::clone(&element)),
            DataType::ListView(Arc::clone(&element)),
            DataType::LargeListView(Arc::clone(&element)),
            DataType::FixedSizeList(Arc::clone(&element), 2),
            DataType::Map(Arc::clone(&element), false),
        ] {
            let input = Arc::new(Field::new("input", data_type, false));
            for kind in [
                ExplodeKind::Explode,
                ExplodeKind::ExplodeOuter,
                ExplodeKind::PosExplode,
                ExplodeKind::PosExplodeOuter,
                ExplodeKind::Inline,
                ExplodeKind::InlineOuter,
            ] {
                let func = Explode::new(kind);
                let result = func.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[Arc::clone(&input)],
                    scalar_arguments: &[None],
                })?;
                assert_eq!(
                    result.as_ref(),
                    &element
                        .as_ref()
                        .clone()
                        .with_name(func.name())
                        .with_nullable(true)
                );
                assert!(func.return_type(&[input.data_type().clone()]).is_err());
            }
        }
        Ok(())
    }
}
