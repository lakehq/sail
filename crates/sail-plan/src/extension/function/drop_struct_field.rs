use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion_common::cast::as_struct_array;
use datafusion_common::{exec_err, plan_err, ExprSchema, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable, ScalarUDFImpl, Signature, Volatility};

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
}

impl ScalarUDFImpl for DropStructField {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "drop_struct_field"
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() != 1 {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                args.len()
            );
        }

        Ok(format!(
            "{}[{}]",
            args[0].display_name()?,
            &self.field_names.join(".")
        ))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        todo!()
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        if args.len() != 1 {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                args.len()
            );
        }

        // Just testing, this is not the right way to deal with field names
        let name = &self.field_names.join(".");
        let data_type = args[0].get_type(schema)?;

        match (data_type, name) {
            (DataType::Struct(fields), s) => {
                if s.is_empty() {
                    plan_err!(
                        "Struct based indexed access requires a non empty string"
                    )
                } else {
                    let remaining_fields: Vec<_> = fields
                        .iter()
                        .filter_map(|f| {
                            if f.name() != s {
                                Some(f.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    Ok(DataType::Struct(remaining_fields.into()))
                }
            }
            (DataType::Null, _) => Ok(DataType::Null),
            (other, _) => plan_err!("The expression to get an indexed field is only valid for `List`, `Struct`, or `Null` types, got {other}"),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "drop_struct_field function requires 1 argument, got {}",
                args.len()
            );
        }

        if args[0].data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        let arrays = ColumnarValue::values_to_arrays(args)?;
        let array = Arc::clone(&arrays[0]);
        // Just testing, this is not the right way to deal with field names
        let name = &self.field_names.join(".");

        match (array.data_type(), name) {
            (DataType::Struct(_), k) => {
                let struct_array = as_struct_array(&array)?;
                let drop_pos = struct_array.column_names().iter().position(|c| c == k);
                match drop_pos {
                    None => exec_err!("drop indexed field {k} not found in struct"),
                    Some(pos) => {
                        let filtered_columns: Vec<ArrayRef> = struct_array
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != pos)
                            .map(|(_, field)| Arc::clone(field))
                            .collect();
                        let filtered_fields: Fields = struct_array
                            .fields()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != pos)
                            .map(|(_, field)| Arc::clone(field))
                            .collect();
                        let new_struct_array =
                            Arc::new(StructArray::new(filtered_fields, filtered_columns, None));
                        Ok(ColumnarValue::Array(new_struct_array))
                    }
                }
            }
            (DataType::Null, _) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
            (dt, name) => exec_err!(
                "drop indexed field is only possible on lists with int64 indexes or struct \
                                         with utf8 indexes. Tried {dt:?} with {name:?} index"
            ),
        }
    }
}
