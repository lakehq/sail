use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ReturnFieldArgs;
pub fn field_name(value: Option<&ScalarValue>) -> Result<&str> {
    value
        .and_then(|x| x.try_as_str().flatten())
        .filter(|x| !x.is_empty())
        .ok_or_else(|| {
            plan_datafusion_err!(
                "{} requires a non-empty literal field name",
                "array_struct_field"
            )
        })
}
pub fn list_field(data_type: &DataType) -> Result<&FieldRef> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Ok(field)
        }
        _ => exec_err!("array_struct_field requires a list, got {data_type}"),
    }
}
pub fn list_item_field(field: &FieldRef, nullable: bool) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_name(Field::LIST_FIELD_DEFAULT_NAME)
            .with_nullable(nullable),
    )
}
pub fn return_type_for_list(data_type: &DataType, field_name: &str) -> Result<DataType> {
    let list_field = list_field(data_type)?;
    let DataType::Struct(fields) = list_field.data_type() else {
        return exec_err!(
            "array_struct_field requires a list of structs, got {}",
            list_field.data_type()
        );
    };
    let (_, child) = fields
        .find(field_name)
        .ok_or_else(|| exec_datafusion_err!("missing field: {field_name}"))?;
    let nullable = list_field.is_nullable() || child.is_nullable();
    let field = list_item_field(child, nullable);
    match data_type {
        DataType::List(_) => Ok(DataType::List(field)),
        DataType::LargeList(_) => Ok(DataType::LargeList(field)),
        DataType::FixedSizeList(_, size) => Ok(DataType::FixedSizeList(field, *size)),
        _ => unreachable!("list_field accepted only list data types"),
    }
}
pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let [array_field, _] = args.arg_fields else {
        return exec_err!(
            "{} requires 2 arguments, got {}",
            "array_struct_field",
            args.arg_fields.len()
        );
    };
    let field_name = field_name(args.scalar_arguments.get(1).copied().flatten())?;
    let data_type = return_type_for_list(array_field.data_type(), field_name)?;
    Ok(Arc::new(Field::new("array_struct_field", data_type, true)))
}

use datafusion_common::{exec_datafusion_err, exec_err, plan_datafusion_err};
