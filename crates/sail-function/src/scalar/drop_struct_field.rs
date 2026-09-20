use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common::utils::string::equals_ignore_case;

use crate::error::field_not_found_plan_err;

/// Matches a field name against the name a `dropFields` asked for, the way the analyzer resolver
/// does: it folds the case unless the analysis is case sensitive.
fn matches(name: &str, target: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        name == target
    } else {
        equals_ignore_case(name, target)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DropStructField {
    signature: Signature,
    field_names: Vec<String>,
    case_sensitive: bool,
}

impl DropStructField {
    pub fn new(field_names: Vec<String>, case_sensitive: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            field_names,
            case_sensitive,
        }
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    fn drop_nested_field(
        data_type: &DataType,
        field_names: &[String],
        case_sensitive: bool,
    ) -> Result<DataType> {
        match data_type {
            DataType::Struct(fields) => {
                let Some(current_field) = field_names.first() else {
                    return plan_err!("Field name cannot be empty");
                };

                let mut new_fields = Vec::with_capacity(fields.len());
                let mut field_found = false;

                for field in fields.iter() {
                    if matches(field.name(), current_field, case_sensitive) {
                        field_found = true;
                        if field_names.len() == 1 {
                            continue;
                        }
                        let new_data_type = Self::drop_nested_field(
                            field.data_type(),
                            &field_names[1..],
                            case_sensitive,
                        )?;
                        // The path is rebuilt as a `WithField` per level, so the level takes the
                        // spelling that was asked for rather than the one the struct declares.
                        new_fields.push(Arc::new(Field::new(
                            current_field,
                            new_data_type,
                            field.is_nullable(),
                        )));
                    } else {
                        new_fields.push(Arc::clone(field));
                    }
                }

                // A level of a nested path is looked up before it is rebuilt, so a name that
                // matches nothing is a missing field. The last name is only ever dropped, and
                // dropping a field that is not there leaves the struct alone.
                if !field_found && field_names.len() > 1 {
                    return Err(field_not_found_plan_err(current_field, fields));
                }

                if new_fields.is_empty() {
                    plan_err!(
                        "[DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS] Cannot drop all fields in struct"
                    )
                } else {
                    Ok(DataType::Struct(new_fields.into()))
                }
            }
            _ => plan_err!("Expected Struct, found {data_type}"),
        }
    }

    fn drop_nested_field_from_array(
        array: &ArrayRef,
        field_names: &[String],
        case_sensitive: bool,
    ) -> Result<ArrayRef> {
        let Some(current_field) = field_names.first() else {
            return exec_err!("Field name cannot be empty");
        };

        let struct_array = as_struct_array(&array)?;
        let new_data_type =
            Self::drop_nested_field(struct_array.data_type(), field_names, case_sensitive)?;
        let DataType::Struct(new_fields) = new_data_type else {
            return exec_err!("drop_struct_field expected a struct type");
        };

        // The type above kept the input fields in order, dropping the ones that matched, so the
        // same walk over the input produces the arrays that go with it. Walking by name instead
        // would read the same column twice when two fields match the name.
        let mut new_arrays = Vec::with_capacity(new_fields.len());
        for (index, field) in struct_array.fields().iter().enumerate() {
            let column = struct_array.column(index);
            if matches(field.name(), current_field, case_sensitive) {
                if field_names.len() == 1 {
                    continue;
                }
                new_arrays.push(Self::drop_nested_field_from_array(
                    column,
                    &field_names[1..],
                    case_sensitive,
                )?);
            } else {
                new_arrays.push(Arc::clone(column));
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = arg_types else {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                arg_types.len()
            );
        };
        Self::drop_nested_field(arg_type, &self.field_names, self.case_sensitive)
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
        let new_array =
            Self::drop_nested_field_from_array(array, &self.field_names, self.case_sensitive)?;
        Ok(ColumnarValue::Array(new_array))
    }
}
