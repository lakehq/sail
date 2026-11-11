use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{exec_err, plan_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};

/// Marks an entry from a non-outer map generator as non-null after unnest.
/// Unnest's generic schema allows padding, but this lowering emits no padded rows.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapExplodeEntry {
    signature: Signature,
}

impl Default for MapExplodeEntry {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapExplodeEntry {
    fn name(&self) -> &str {
        "map_explode_entry"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [data_type @ DataType::Struct(_)] => Ok(data_type.clone()),
            _ => plan_err!("map_explode_entry expects a struct"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("map_explode_entry expects one argument");
        };
        self.return_type(&[field.data_type().clone()])?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut args = args.args;
        let Some(value) = args.pop().filter(|_| args.is_empty()) else {
            return exec_err!("map_explode_entry expects one argument");
        };
        let has_null = match &value {
            ColumnarValue::Array(array) => array.null_count() != 0,
            ColumnarValue::Scalar(value) => value.is_null(),
        };
        if has_null {
            return exec_err!("non-outer map generator produced a null entry");
        }
        Ok(value)
    }
}

#[cfg(test)]
mod map_entry_tests {
    use arrow::array::{Array, ArrayRef, Int32Array, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn entry_refinement_preserves_fields_and_rejects_nulls() -> Result<()> {
        let child = Arc::new(
            Field::new("key", DataType::Int32, false)
                .with_metadata([("owner".to_string(), "retained".to_string())].into()),
        );
        let fields = vec![child].into();
        let array = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        ));
        let field = Arc::new(
            Field::new("entry", array.data_type().clone(), true)
                .with_metadata([("entry".to_string(), "retained".to_string())].into()),
        );
        let udf = MapExplodeEntry::default();
        let output_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&field)],
            scalar_arguments: &[None],
        })?;
        assert_eq!(
            output_field.as_ref(),
            &field.as_ref().clone().with_nullable(false)
        );
        let null = Arc::new(StructArray::new(
            array.fields().clone(),
            array.columns().to_vec(),
            Some(NullBuffer::new_null(1)),
        ));
        for (input, valid) in [
            (ColumnarValue::Array(Arc::clone(&array) as ArrayRef), true),
            (ColumnarValue::Scalar(ScalarValue::Struct(array)), true),
            (ColumnarValue::Array(Arc::clone(&null) as ArrayRef), false),
            (ColumnarValue::Scalar(ScalarValue::Struct(null)), false),
        ] {
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![input.clone()],
                arg_fields: vec![Arc::clone(&field)],
                number_rows: 1,
                return_field: Arc::clone(&output_field),
                config_options: Default::default(),
            });
            if valid {
                assert_eq!(result?.to_array(1)?.to_data(), input.to_array(1)?.to_data());
            } else {
                assert!(result.is_err());
            }
        }
        Ok(())
    }
}

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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[DataType::List(f)]
            | &[DataType::LargeList(f)]
            | &[DataType::FixedSizeList(f, _)]
            | &[DataType::Map(f, _)] => Ok(f.data_type().clone()),
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
