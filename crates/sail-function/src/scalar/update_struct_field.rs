use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::utils::string::equals_ignore_case;

/// Matches a field name against the name a `withField` asked for, the way the analyzer resolver
/// does: it folds the case unless the analysis is case sensitive.
fn matches(name: &str, target: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        name == target
    } else {
        equals_ignore_case(name, target)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UpdateStructField {
    signature: Signature,
    field_names: Vec<String>,
    case_sensitive: bool,
}

impl UpdateStructField {
    pub fn new(field_names: Vec<String>, case_sensitive: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            field_names,
            case_sensitive,
        }
    }

    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    fn update_nested_field(
        data_type: &DataType,
        field_names: &[String],
        new_field: &Field,
        case_sensitive: bool,
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
                    // The field to replace is matched the way the analyzer resolver matches a
                    // name, so a name written in another case reaches it unless the analysis is
                    // case sensitive.
                    if matches(field.name(), current_field, case_sensitive) {
                        field_found = true;
                        if field_names.len() == 1 {
                            // The field is replaced rather than edited, so it takes the type,
                            // the nullability and the name of the value, and no metadata, which
                            // is what `StructField(name, dataType, nullable)` gives it.
                            new_fields
                                .push(Arc::new(new_field.clone().with_name(current_field.clone())));
                        } else {
                            let new_data_type = Self::update_nested_field(
                                field.data_type(),
                                &field_names[1..],
                                new_field,
                                case_sensitive,
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
        Self::update_nested_field(
            data_type,
            &self.field_names,
            &new_field,
            self.case_sensitive,
        )
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
        let data_type = Self::update_nested_field(
            struct_field.data_type(),
            &self.field_names,
            &new_field,
            self.case_sensitive,
        )?;
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
