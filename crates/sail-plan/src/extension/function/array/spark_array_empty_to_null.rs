use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::cast::{as_large_list_array, as_list_array};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct ArrayEmptyToNull {
    signature: Signature,
}

impl Default for ArrayEmptyToNull {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayEmptyToNull {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn general_array_empty_to_null<O: OffsetSizeTrait>(
        array: &GenericListArray<O>,
        field: &FieldRef,
    ) -> Result<GenericListArray<O>> {
        let non_nulls = array
            .iter()
            .map(|x| match x {
                Some(a) => a.len() != 0,
                None => false,
            })
            .collect::<Vec<_>>();
        Ok(GenericListArray::<O>::try_new(
            field.clone(),
            array.offsets().clone(),
            array.values().clone(),
            Some(NullBuffer::from(non_nulls)),
        )?)
    }
}

impl ScalarUDFImpl for ArrayEmptyToNull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_empty_to_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[x @ (DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _))] => {
                Ok(x.clone())
            }
            _ => Err(DataFusionError::Internal(format!(
                "{} should only be called with a list",
                self.name(),
            ))),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} should only be called with a single argument",
                self.name()
            )));
        }
        let arg = &args[0];
        let out = match arg.data_type() {
            DataType::List(field) => {
                let array = as_list_array(arg)?;
                Arc::new(Self::general_array_empty_to_null(array, field)?) as ArrayRef
            }
            DataType::LargeList(field) => {
                let array = as_large_list_array(arg)?;
                Arc::new(Self::general_array_empty_to_null(array, field)?) as ArrayRef
            }
            DataType::FixedSizeList(_, _) => {
                return Err(DataFusionError::NotImplemented(
                    "fixed size list".to_string(),
                ));
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} should only be called with a list",
                    self.name(),
                )));
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}
