use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::plan_err;
use datafusion_expr::ScalarFunctionArgs;

pub fn explode_name_to_kind(name: &str) -> Result<ExplodeKind> {
    match name {
        "explode" => Ok(ExplodeKind::Explode),
        "explode_outer" => Ok(ExplodeKind::ExplodeOuter),
        "posexplode" => Ok(ExplodeKind::PosExplode),
        "posexplode_outer" => Ok(ExplodeKind::PosExplodeOuter),
        "inline" => Ok(ExplodeKind::Inline),
        "inline_outer" => Ok(ExplodeKind::InlineOuter),
        "variant_explode" => Ok(ExplodeKind::VariantExplode),
        "variant_explode_outer" => Ok(ExplodeKind::VariantExplodeOuter),
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
    VariantExplode,
    VariantExplodeOuter,
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
            ExplodeKind::Inline => "inline",
            ExplodeKind::InlineOuter => "inline_outer",
            ExplodeKind::VariantExplode => "variant_explode",
            ExplodeKind::VariantExplodeOuter => "variant_explode_outer",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.kind {
            ExplodeKind::VariantExplode | ExplodeKind::VariantExplodeOuter => {
                // variant_explode accepts a variant (struct) and returns struct<pos, key, value>
                // but the actual return type is handled by the rewriter
                Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
            }
            _ => match &arg_types {
                &[DataType::List(f)]
                | &[DataType::LargeList(f)]
                | &[DataType::FixedSizeList(f, _)]
                | &[DataType::Map(f, _)] => Ok(f.data_type().clone()),
                _ => plan_err!("{} should only be called with a list or map", self.name()),
            },
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
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Field;

    use super::*;

    fn list_item_field() -> Arc<Field> {
        Arc::new(Field::new("item", DataType::Int32, true))
    }

    fn map_entries_field() -> Arc<Field> {
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        ))
    }

    #[test]
    fn test_explode_name_to_kind_maps_all_supported_names() -> Result<()> {
        let cases = [
            ("explode", ExplodeKind::Explode),
            ("explode_outer", ExplodeKind::ExplodeOuter),
            ("posexplode", ExplodeKind::PosExplode),
            ("posexplode_outer", ExplodeKind::PosExplodeOuter),
            ("inline", ExplodeKind::Inline),
            ("inline_outer", ExplodeKind::InlineOuter),
            ("variant_explode", ExplodeKind::VariantExplode),
            ("variant_explode_outer", ExplodeKind::VariantExplodeOuter),
        ];

        for (name, expected_kind) in cases {
            assert_eq!(explode_name_to_kind(name)?, expected_kind);
        }

        Ok(())
    }

    #[test]
    fn test_explode_name_to_kind_rejects_unknown_name() {
        let error = explode_name_to_kind("explode_variant").unwrap_err();
        assert!(error.to_string().contains("Invalid explode function name"));
    }

    #[test]
    fn test_explode_names_match_kinds() {
        let cases = [
            (ExplodeKind::Explode, "explode"),
            (ExplodeKind::ExplodeOuter, "explode_outer"),
            (ExplodeKind::PosExplode, "posexplode"),
            (ExplodeKind::PosExplodeOuter, "posexplode_outer"),
            (ExplodeKind::Inline, "inline"),
            (ExplodeKind::InlineOuter, "inline_outer"),
            (ExplodeKind::VariantExplode, "variant_explode"),
            (ExplodeKind::VariantExplodeOuter, "variant_explode_outer"),
        ];

        for (kind, expected_name) in cases {
            assert_eq!(Explode::new(kind).name(), expected_name);
        }
    }

    #[test]
    fn test_return_type_for_list_and_map_inputs() -> Result<()> {
        let explode = Explode::new(ExplodeKind::Explode);
        let list_field = list_item_field();
        let map_field = map_entries_field();

        assert_eq!(
            explode.return_type(&[DataType::List(list_field.clone())])?,
            list_field.data_type().clone()
        );
        assert_eq!(
            explode.return_type(&[DataType::LargeList(list_field.clone())])?,
            list_field.data_type().clone()
        );
        assert_eq!(
            explode.return_type(&[DataType::FixedSizeList(list_field.clone(), 2)])?,
            list_field.data_type().clone()
        );
        assert_eq!(
            explode.return_type(&[DataType::Map(map_field.clone(), false)])?,
            map_field.data_type().clone()
        );

        Ok(())
    }

    #[test]
    fn test_return_type_for_variant_kinds_preserves_argument_type() -> Result<()> {
        let variant_type = DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, false),
            ]
            .into(),
        );

        for kind in [
            ExplodeKind::VariantExplode,
            ExplodeKind::VariantExplodeOuter,
        ] {
            let explode = Explode::new(kind);
            assert_eq!(explode.return_type(&[variant_type.clone()])?, variant_type);
        }

        assert_eq!(
            Explode::new(ExplodeKind::VariantExplode).return_type(&[])?,
            DataType::Null
        );

        Ok(())
    }

    #[test]
    fn test_return_type_rejects_non_collection_inputs_for_non_variant_kinds() {
        let error = Explode::new(ExplodeKind::Explode)
            .return_type(&[DataType::Int32])
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("explode should only be called with a list or map"));
    }

    #[test]
    fn test_invoke_with_args_reports_rewrite_requirement() {
        let error = Explode::new(ExplodeKind::VariantExplode)
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                return_field: Arc::new(Field::new("result", DataType::Null, true)),
                arg_fields: vec![],
                number_rows: 0,
                config_options: Default::default(),
            })
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("variant_explode should be rewritten during logical plan analysis"));
    }
}
