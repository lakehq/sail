use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, internal_err, plan_err,
};
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

    fn output_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return exec_err!(
                "update_struct_field function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        let data_type = &arg_types[0];
        let new_field_type = &arg_types[1];
        let new_field = Field::new(
            self.field_names
                .last()
                .ok_or_else(|| exec_datafusion_err!("empty attribute: {:?}", &self.field_names))?,
            new_field_type.clone(),
            true,
        );
        Self::update_nested_field(data_type, &self.field_names, &new_field)
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    fn update_nested_field(
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
                            new_fields.push(Arc::new(new_field.clone()));
                        } else {
                            let new_data_type = Self::update_nested_field(
                                field.data_type(),
                                &field_names[1..],
                                new_field,
                            )?;
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
                    if field_names.len() == 1 {
                        new_fields.push(Arc::new(new_field.clone()));
                    } else {
                        let mut intermediate_type = new_field.data_type().clone();
                        for field_name in field_names.iter().rev().skip(1) {
                            intermediate_type = DataType::Struct(
                                vec![Arc::new(Field::new(field_name, intermediate_type, true))]
                                    .into(),
                            );
                        }
                        new_fields.push(Arc::new(Field::new(
                            current_field,
                            intermediate_type,
                            true,
                        )));
                    }
                }

                Ok(DataType::Struct(new_fields.into()))
            }
            _ => plan_err!("Expected Struct, found {data_type}"),
        }
    }

    fn update_nested_field_from_array(
        array: &ArrayRef,
        field_names: &[String],
        new_field_array: &ArrayRef,
    ) -> Result<ArrayRef> {
        if field_names.is_empty() {
            return exec_err!("Field name cannot be empty");
        }

        let struct_array = as_struct_array(&array)?;
        let last_field_name = field_names
            .last()
            .ok_or_else(|| exec_datafusion_err!("empty attribute: {:?}", &field_names))?;
        let new_field = Field::new(last_field_name, new_field_array.data_type().clone(), true);
        let new_data_type =
            Self::update_nested_field(struct_array.data_type(), field_names, &new_field)?;
        let new_fields = match new_data_type {
            DataType::Struct(fields) => fields,
            _ => unreachable!("update_nested_field should always return a Struct"),
        };
        let mut new_arrays = Vec::new();

        for field in new_fields.iter() {
            if field.name() == last_field_name {
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "{}: `return_type` should not be called; `return_field_from_args` is used instead",
            self.name()
        )
    }

    // Spark: `UpdateFields` declares `override def nullable: Boolean = structExpr.nullable`
    // (complexTypeCreator.scala:755) — only the struct being updated decides. A nullable
    // replacement value changes the nested field's nullability but cannot make a non-null
    // input struct null, so this must not derive from all the arguments.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let arg_types = arg_types.as_slice();
        let data_type = self.output_type(arg_types)?;
        let [struct_field, ..] = args.arg_fields else {
            return internal_err!("{} requires at least 1 argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            struct_field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
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
        let new_array =
            Self::update_nested_field_from_array(struct_array, &self.field_names, new_field_array)?;
        Ok(ColumnarValue::Array(new_array))
    }
}
