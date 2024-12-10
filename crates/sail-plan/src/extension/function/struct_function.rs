use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

fn to_struct_array(args: &[ArrayRef], field_names: &[String]) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("struct requires at least one argument");
    }

    let vec: Vec<_> = args
        .iter()
        .zip(field_names.iter())
        .map(|(arg, field_name)| {
            Ok((
                Arc::new(Field::new(
                    field_name.as_str(),
                    arg.data_type().clone(),
                    true,
                )),
                arg.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(StructArray::from(vec)))
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let fields = arg_types
            .iter()
            .zip(self.field_names.iter())
            .map(|(dt, field_name)| Ok(Field::new(field_name.clone(), dt.clone(), true)))
            .collect::<Result<Vec<Field>>>()?;
        Ok(DataType::Struct(Fields::from(fields)))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        Ok(ColumnarValue::Array(to_struct_array(
            arrays.as_slice(),
            self.field_names.as_slice(),
        )?))
    }
}
