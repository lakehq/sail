//! Configured function type policy.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn update_nested_field(
    data_type: &DataType,
    field_names: &[String],
    new_field: &Field,
) -> Result<DataType> {
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
                        new_fields.push(Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(new_field.data_type().clone())
                                .with_nullable(new_field.is_nullable()),
                        ));
                    } else {
                        let new_data_type =
                            update_nested_field(field.data_type(), &field_names[1..], new_field)?;
                        new_fields.push(Arc::new(
                            field.as_ref().clone().with_data_type(new_data_type),
                        ));
                    }
                } else {
                    new_fields.push(Arc::clone(field));
                }
            }

            if !field_found {
                if field_names.len() == 1 {
                    new_fields.push(Arc::new(new_field.clone()));
                } else {
                    let mut intermediate_type = new_field.data_type().clone();
                    for field_name in field_names.iter().rev().skip(1) {
                        intermediate_type = DataType::Struct(
                            vec![Arc::new(Field::new(field_name, intermediate_type, true))].into(),
                        );
                    }
                    new_fields.push(Arc::new(Field::new(current_field, intermediate_type, true)));
                }
            }

            Ok(DataType::Struct(new_fields.into()))
        }
        _ => plan_err!("Expected Struct, found {data_type}"),
    }
}

pub fn return_type(field_names: &[String], arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 2 {
        return exec_err!(
            "update_struct_field function requires 2 arguments, got {}",
            arg_types.len()
        );
    }
    let data_type = &arg_types[0];
    let new_field_type = &arg_types[1];
    let new_field = Field::new(
        field_names
            .last()
            .ok_or_else(|| exec_datafusion_err!("empty attribute: {:?}", field_names))?,
        new_field_type.clone(),
        true,
    );
    update_nested_field(data_type, field_names, &new_field)
}

pub fn return_field_from_args(field_names: &[String], args: ReturnFieldArgs) -> Result<FieldRef> {
    let [struct_field, value_field] = args.arg_fields else {
        return exec_err!(
            "update_struct_field function requires 2 arguments, got {}",
            args.arg_fields.len()
        );
    };
    let field_name = field_names
        .last()
        .ok_or_else(|| exec_datafusion_err!("empty attribute: {:?}", field_names))?;
    let new_field = Field::new(
        field_name,
        value_field.data_type().clone(),
        value_field.is_nullable(),
    );
    let data_type = update_nested_field(struct_field.data_type(), field_names, &new_field)?;
    Ok(Arc::new(Field::new(
        "update_struct_field",
        data_type,
        struct_field.is_nullable(),
    )))
}
