use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, plan_err};
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
                            new_fields.push(Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(new_field.data_type().clone())
                                    .with_nullable(new_field.is_nullable()),
                            ));
                        } else {
                            let new_data_type = Self::update_nested_field(
                                field.data_type(),
                                &field_names[1..],
                                new_field,
                            )?;
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [struct_field, value_field] = args.arg_fields else {
            return exec_err!(
                "update_struct_field function requires 2 arguments, got {}",
                args.arg_fields.len()
            );
        };
        let field_name = self
            .field_names
            .last()
            .ok_or_else(|| exec_datafusion_err!("empty attribute: {:?}", &self.field_names))?;
        let new_field = Field::new(
            field_name,
            value_field.data_type().clone(),
            value_field.is_nullable(),
        );
        let data_type =
            Self::update_nested_field(struct_field.data_type(), &self.field_names, &new_field)?;
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            struct_field.is_nullable(),
        )))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Float32Array, Int32Array};
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_replace_field_uses_value_nullability() -> Result<()> {
        let metadata = HashMap::from([("field_id".to_string(), "1".to_string())]);
        let fields = vec![Arc::new(
            Field::new("a", DataType::Float32, false).with_metadata(metadata.clone()),
        )]
        .into();
        let struct_array = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0]))],
            None,
        )) as ArrayRef;
        let value_array = Arc::new(Int32Array::from(vec![Some(0), Some(2), None])) as ArrayRef;
        let arg_fields = vec![
            Arc::new(Field::new("x", struct_array.data_type().clone(), false)),
            Arc::new(Field::new("e", DataType::Int32, true)),
        ];
        let scalar_arguments = [None, None];
        let function = UpdateStructField::new(vec!["a".to_string()]);
        let return_field = function.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;

        assert!(!return_field.is_nullable());
        let DataType::Struct(fields) = return_field.data_type() else {
            return exec_err!("expected Struct return type");
        };
        let field = &fields[0];
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());
        assert_eq!(field.metadata(), &metadata);

        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(struct_array),
                ColumnarValue::Array(value_array),
            ],
            arg_fields,
            number_rows: 3,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let ColumnarValue::Array(result) = result else {
            return exec_err!("expected array result");
        };
        assert_eq!(result.data_type(), return_field.data_type());
        let result = as_struct_array(&result)?;
        let values = result
            .column_by_name("a")
            .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| exec_datafusion_err!("expected Int32 field `a`"))?;
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(0), Some(2), None]
        );
        Ok(())
    }

    #[test]
    fn test_replace_field_can_become_non_nullable() -> Result<()> {
        let struct_type =
            DataType::Struct(vec![Arc::new(Field::new("a", DataType::Int32, true))].into());
        let arg_fields = vec![
            Arc::new(Field::new("x", struct_type, true)),
            Arc::new(Field::new("e", DataType::Int32, false)),
        ];
        let scalar_arguments = [None, None];
        let function = UpdateStructField::new(vec!["a".to_string()]);
        let return_field = function.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;

        assert!(return_field.is_nullable());
        let DataType::Struct(fields) = return_field.data_type() else {
            return exec_err!("expected Struct return type");
        };
        assert!(!fields[0].is_nullable());
        Ok(())
    }
}
