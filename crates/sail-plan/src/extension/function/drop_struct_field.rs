use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct DropStructField {
    signature: Signature,
    field_names: Vec<String>,
}

impl DropStructField {
    pub fn new(field_names: Vec<String>) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            field_names,
        }
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    fn drop_nested_field(data_type: &DataType, field_names: &[String]) -> Result<DataType> {
        match data_type {
            DataType::Struct(fields) => {
                if field_names.is_empty() {
                    return plan_err!("Field name cannot be empty");
                }

                let current_field = &field_names[0];
                let mut new_fields = Vec::new();
                let mut field_found = false;

                for field in fields.iter() {
                    if field.name() == current_field {
                        field_found = true;
                        if field_names.len() == 1 {
                            continue;
                        } else {
                            let new_data_type =
                                Self::drop_nested_field(field.data_type(), &field_names[1..])?;
                            new_fields.push(Arc::new(Field::new(
                                field.name(),
                                new_data_type,
                                field.is_nullable(),
                            )));
                        }
                    } else {
                        new_fields.push(Arc::clone(field));
                    }
                }

                if !field_found {
                    plan_err!("Field `{current_field}` not found")
                } else {
                    Ok(DataType::Struct(new_fields.into()))
                }
            }
            _ => plan_err!("Expected Struct, found {data_type}"),
        }
    }

    fn drop_nested_field_from_array(array: &ArrayRef, field_names: &[String]) -> Result<ArrayRef> {
        if field_names.is_empty() {
            return exec_err!("Field name cannot be empty");
        }

        let struct_array = as_struct_array(&array)?;
        let new_data_type = Self::drop_nested_field(struct_array.data_type(), field_names)?;
        let new_fields = match new_data_type {
            DataType::Struct(fields) => fields,
            _ => unreachable!("drop_nested_field should always return a Struct"),
        };
        let mut new_arrays = Vec::new();

        for field in new_fields.iter() {
            if let Some(column) = struct_array.column_by_name(field.name()) {
                if field.data_type() != column.data_type() {
                    let new_array = Self::drop_nested_field_from_array(column, &field_names[1..])?;
                    new_arrays.push(new_array);
                } else {
                    new_arrays.push(Arc::clone(column));
                }
            } else {
                return exec_err!("Field `{}` not found", field.name());
            }
        }

        Ok(Arc::new(StructArray::try_new(
            new_fields,
            new_arrays,
            struct_array.nulls().cloned(),
        )?))
    }
}

impl ScalarUDFImpl for DropStructField {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "drop_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        Self::drop_nested_field(&arg_types[0], &self.field_names)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                args.len()
            );
        }
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let array = &arrays[0];
        let new_array = Self::drop_nested_field_from_array(array, &self.field_names)?;
        Ok(ColumnarValue::Array(new_array))
    }
}
