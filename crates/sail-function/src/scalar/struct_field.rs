use std::sync::Arc;

use datafusion::arrow::array::{make_array, Array, ArrayRef, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, plan_datafusion_err, plan_err, Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;

/// Extract a single field from a struct column.
///
/// Unlike DataFusion's built-in `get_field`, this unions the parent struct's null
/// buffer with the child field's null buffer, so a `NULL` struct yields `NULL` for
/// the extracted field instead of the child type's default value.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StructField {
    signature: Signature,
}

impl Default for StructField {
    fn default() -> Self {
        Self::new()
    }
}

impl StructField {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
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

    fn project(array: &ArrayRef, field_name: &str) -> Result<ArrayRef> {
        let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
            return exec_err!("struct_field requires a struct, got {}", array.data_type());
        };
        let (index, _) = struct_array
            .fields()
            .find(field_name)
            .ok_or_else(|| exec_datafusion_err!("missing field: {field_name}"))?;
        let values = struct_array.column(index);
        let nulls = NullBuffer::union(struct_array.nulls(), values.nulls());
        let data = values.to_data().into_builder().nulls(nulls).build()?;
        Ok(make_array(data))
    }
}

impl ScalarUDFImpl for StructField {
    fn name(&self) -> &str {
        "struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(self.name(), (2, 2), arg_types.len()));
        }
        if !matches!(arg_types[0], DataType::Struct(_)) {
            return plan_err!(
                "The first argument of the `{}` function must be a struct, but got {}",
                self.name(),
                arg_types[0]
            );
        }
        let name_type = match arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => arg_types[1].clone(),
            DataType::Null => DataType::Utf8,
            _ => {
                return plan_err!(
                    "The second argument of the `{}` function must be a string, but got {}",
                    self.name(),
                    arg_types[1]
                )
            }
        };
        Ok(vec![arg_types[0].clone(), name_type])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [struct_field, _] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (2, 2),
                args.arg_fields.len(),
            ));
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
        // The result is null whenever the parent struct or the child field is null.
        let nullable = struct_field.is_nullable() || child.is_nullable();
        Ok(Arc::new(Field::new(
            self.name(),
            child.data_type().clone(),
            nullable,
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
        let out = Self::project(array, field_name)?;
        Ok(ColumnarValue::Array(out))
    }
}
