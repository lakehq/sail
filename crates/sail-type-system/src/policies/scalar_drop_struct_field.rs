//! Configured function type policy.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::{Result, exec_err, plan_err};

pub fn drop_nested_field(data_type: &DataType, field_names: &[String]) -> Result<DataType> {
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
                            drop_nested_field(field.data_type(), &field_names[1..])?;
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

pub fn return_type(field_names: &[String], arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 1 {
        return exec_err!(
            "drop_struct_field function requires 1 argument, got {}",
            arg_types.len()
        );
    }
    drop_nested_field(&arg_types[0], field_names)
}
