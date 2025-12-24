use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

pub fn to_struct_array(
    args: &[ArrayRef],
    field_names: &[String],
    arg_fields: &[FieldRef],
) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("struct requires at least one argument");
    }

    let vec: Vec<_> = args
        .iter()
        .zip(field_names.iter())
        .zip(arg_fields.iter())
        .map(|((arg, field_name), arg_field)| {
            // Use the nullability from the input field schema
            Ok((
                Arc::new(Field::new(
                    field_name.as_str(),
                    arg.data_type().clone(),
                    arg_field.is_nullable(),
                )),
                arg.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(StructArray::from(vec)))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructFunction {
    signature: Signature,
    field_names: Vec<String>,
}

impl StructFunction {
    pub fn new(field_names: Vec<String>) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            field_names,
        }
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }
}

impl ScalarUDFImpl for StructFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "struct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "StructFunction: return_type() is not used; return_field_from_args() is implemented"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Build struct fields preserving the nullability from input fields
        let fields: Vec<Field> = args
            .arg_fields
            .iter()
            .zip(self.field_names.iter())
            .map(|(arg_field, field_name)| {
                Field::new(
                    field_name.clone(),
                    arg_field.data_type().clone(),
                    arg_field.is_nullable(),
                )
            })
            .collect();

        // The struct is nullable if any of its input fields are nullable
        let struct_nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        let struct_type = DataType::Struct(Fields::from(fields));
        Ok(Arc::new(Field::new(
            self.name(),
            struct_type,
            struct_nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, arg_fields, ..
        } = args;
        let arrays = ColumnarValue::values_to_arrays(&args)?;
        Ok(ColumnarValue::Array(to_struct_array(
            arrays.as_slice(),
            self.field_names.as_slice(),
            &arg_fields,
        )?))
    }
}
