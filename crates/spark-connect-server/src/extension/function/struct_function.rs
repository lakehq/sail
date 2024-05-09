use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

// Not sure why the DataFusion code does not try to extract the field names from the input.
// So I'm just rewriting the code here.

fn to_array_struct(args: &[ArrayRef], field_names: &[String]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return exec_err!("struct requires at least one argument");
    }

    let vec: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            Ok((
                Arc::new(Field::new(
                    field_names[i].as_str(),
                    arg.data_type().clone(),
                    true,
                )),
                arg.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(StructArray::from(vec)))
}

fn to_struct_expr(args: &[ColumnarValue], field_names: &[String]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    Ok(ColumnarValue::Array(to_array_struct(
        arrays.as_slice(),
        field_names,
    )?))
}
#[derive(Debug, Clone)]
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let return_fields = arg_types
            .iter()
            .enumerate()
            .map(|(pos, dt)| Field::new(self.field_names[pos].clone(), dt.clone(), true))
            .collect::<Vec<Field>>();
        Ok(DataType::Struct(Fields::from(return_fields)))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        to_struct_expr(args, self.field_names.as_slice())
    }
}
