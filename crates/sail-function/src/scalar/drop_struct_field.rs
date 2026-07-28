use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn output_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        Self::drop_nested_field(&arg_types[0], &self.field_names)
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

                for field in fields.iter() {
                    if field.name() == current_field {
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

                // Spark's `dropFields` silently ignores field names that do not
                // exist (the struct is returned unchanged), but raises when every
                // field would be dropped.
                if new_fields.is_empty() {
                    plan_err!("[CANNOT_DROP_ALL_FIELDS] Cannot drop all fields in struct")
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
    fn name(&self) -> &str {
        "drop_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "{}: `return_type` should not be called; `return_field_from_args` is used instead",
            self.name()
        )
    }

    // Spark: `DropField` only rewrites the struct; nullability follows the input struct.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let arg_types = arg_types.as_slice();
        let data_type = self.output_type(arg_types)?;
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let args = ColumnarValue::values_to_arrays(&args)?;
        let [array] = args.as_slice() else {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                args.len()
            );
        };
        let new_array = Self::drop_nested_field_from_array(array, &self.field_names)?;
        Ok(ColumnarValue::Array(new_array))
    }
}
