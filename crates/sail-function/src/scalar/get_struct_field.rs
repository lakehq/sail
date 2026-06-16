use std::sync::Arc;

use datafusion::arrow::array::{make_array, Array, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, plan_datafusion_err, Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Extracts a struct field like DataFusion's `get_field`, with two differences
/// required for Spark semantics:
///
/// - The parent struct's null buffer is unioned into the child column, so rows
///   where the parent struct is NULL yield NULL even when the underlying child
///   slot holds a value (DataFusion's `get_field` returns the raw child array).
/// - The child field (including its metadata) is preserved in the output
///   schema, which a `CASE WHEN parent IS NULL ...` workaround would drop.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetStructField {
    signature: Signature,
}

impl Default for GetStructField {
    fn default() -> Self {
        Self::new()
    }
}

impl GetStructField {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn field_name<'a>(&self, value: Option<&'a ScalarValue>) -> Result<&'a str> {
        value
            .and_then(|x| x.try_as_str().flatten())
            .filter(|x| !x.is_empty())
            .ok_or_else(|| {
                plan_datafusion_err!("{} requires a non-empty literal field name", self.name())
            })
    }
}

impl ScalarUDFImpl for GetStructField {
    fn name(&self) -> &str {
        "get_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [struct_field, _] = args.arg_fields else {
            return exec_err!(
                "{} requires 2 arguments, got {}",
                self.name(),
                args.arg_fields.len()
            );
        };
        let field_name = self.field_name(args.scalar_arguments.get(1).copied().flatten())?;
        let DataType::Struct(fields) = struct_field.data_type() else {
            return exec_err!(
                "{} requires a struct, got {}",
                self.name(),
                struct_field.data_type()
            );
        };
        let (_, child) = fields
            .find(field_name)
            .ok_or_else(|| exec_datafusion_err!("missing field: {field_name}"))?;
        // Clone the child field so its metadata survives in the output schema.
        Ok(Arc::new(child.as_ref().clone().with_nullable(
            struct_field.is_nullable() || child.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let field_name = args
            .get(1)
            .and_then(|arg| match arg {
                ColumnarValue::Scalar(value) => value.try_as_str().flatten(),
                ColumnarValue::Array(_) => None,
            })
            .filter(|x| !x.is_empty())
            .ok_or_else(|| {
                plan_datafusion_err!("{} requires a non-empty literal field name", self.name())
            })?;
        let arrays = ColumnarValue::values_to_arrays(&args[..1])?;
        let [array] = arrays.as_slice() else {
            return exec_err!("{} requires a struct argument", self.name());
        };
        let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
            return exec_err!(
                "{} requires a struct, got {}",
                self.name(),
                array.data_type()
            );
        };
        let (index, _) = struct_array
            .fields()
            .find(field_name)
            .ok_or_else(|| exec_datafusion_err!("missing field: {field_name}"))?;
        let values = struct_array.column(index);
        let nulls = NullBuffer::union(struct_array.nulls(), values.nulls());
        let data = values.to_data().into_builder().nulls(nulls).build()?;
        Ok(ColumnarValue::Array(make_array(data)))
    }
}
