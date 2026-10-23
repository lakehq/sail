use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UpdateStructField {
    signature: Signature,
    field_names: Vec<String>,
}

impl UpdateStructField {
    pub fn new(field_names: Vec<String>) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            field_names,
        }
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    fn update_nested_field_from_array(
        array: &ArrayRef,
        field_names: &[String],
        new_field_array: &ArrayRef,
        new_data_type: &DataType,
    ) -> Result<ArrayRef> {
        if field_names.is_empty() {
            return exec_err!("Field name cannot be empty");
        }

        let struct_array = as_struct_array(&array)?;
        let current_field_name = field_names
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty attribute: {:?}", &field_names))?;
        let new_fields = match new_data_type {
            DataType::Struct(fields) => fields.clone(),
            _ => return exec_err!("Expected Struct return type, found {new_data_type}"),
        };
        let mut new_arrays = Vec::new();

        for field in new_fields.iter() {
            if field.name() == current_field_name {
                if field_names.len() == 1 {
                    new_arrays.push(Arc::clone(new_field_array));
                } else {
                    let existing_column =
                        struct_array.column_by_name(field.name()).ok_or_else(|| {
                            exec_datafusion_err!("Field `{}` not found", field.name())
                        })?;
                    let new_array = Self::update_nested_field_from_array(
                        existing_column,
                        &field_names[1..],
                        new_field_array,
                        field.data_type(),
                    )?;
                    new_arrays.push(new_array);
                }
            } else if let Some(column) = struct_array.column_by_name(field.name()) {
                new_arrays.push(Arc::clone(column));
            } else {
                return exec_err!("Unexpected field `{}` in updated struct", field.name());
            }
        }

        Ok(Arc::new(StructArray::try_new(
            new_fields,
            new_arrays,
            struct_array.nulls().cloned(),
        )?))
    }
}

impl ScalarUDFImpl for UpdateStructField {
    fn name(&self) -> &str {
        "update_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_update_struct_field::return_type(
            &self.field_names,
            arg_types,
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_update_struct_field::return_field_from_args(
            &self.field_names,
            args,
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;
        let args = ColumnarValue::values_to_arrays(&args)?;
        let [struct_array, new_field_array] = args.as_slice() else {
            return exec_err!(
                "update_struct_field function requires 2 arguments, got {}",
                args.len()
            );
        };
        if struct_array.data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        let new_array = Self::update_nested_field_from_array(
            struct_array,
            &self.field_names,
            new_field_array,
            return_field.data_type(),
        )?;
        Ok(ColumnarValue::Array(new_array))
    }
}
