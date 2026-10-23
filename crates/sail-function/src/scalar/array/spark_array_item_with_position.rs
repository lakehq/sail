use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, GenericListArray, OffsetSizeTrait, PrimitiveArray,
    StructArray,
};
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Fields, Int32Type, Int64Type,
};
use datafusion::common::cast::{as_large_list_array, as_list_array};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::exec_err;
use datafusion_expr::ScalarFunctionArgs;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayItemWithPosition {
    signature: Signature,
}

impl Default for ArrayItemWithPosition {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayItemWithPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn item_fields<P: ArrowPrimitiveType>(field: &FieldRef) -> Fields {
        sail_type_system::policies::array_item_with_position::item_fields(field, P::DATA_TYPE)
    }

    fn item_type<P: ArrowPrimitiveType>(field: &FieldRef) -> DataType {
        sail_type_system::policies::array_item_with_position::item_type(field, P::DATA_TYPE)
    }

    fn general_array_item_with_position<P: ArrowPrimitiveType>(
        array: &GenericListArray<P::Native>,
        field: &FieldRef,
    ) -> Result<ArrayRef>
    where
        P::Native: OffsetSizeTrait,
        PrimitiveArray<P>: From<Vec<P::Native>>,
    {
        let mut positions = vec![<P::Native as ArrowNativeType>::usize_as(0); array.values().len()];
        for i in 0..array.len() {
            let start = array.offsets()[i];
            let end = array.offsets()[i + 1];
            let mut p = start;
            while p < end {
                positions[p.as_usize()] = p - start;
                p += <P::Native as ArrowNativeType>::usize_as(1);
            }
        }
        let positions = PrimitiveArray::<P>::from(positions);
        let values = StructArray::try_new(
            Self::item_fields::<P>(field),
            vec![Arc::new(positions) as ArrayRef, array.values().clone()],
            None,
        )?;
        Ok(Arc::new(GenericListArray::<P::Native>::try_new(
            Arc::new(Field::new_list_field(
                Self::item_type::<P>(field),
                field.is_nullable(),
            )),
            array.offsets().clone(),
            Arc::new(values),
            array.nulls().cloned(),
        )?))
    }
}

impl ScalarUDFImpl for ArrayItemWithPosition {
    fn name(&self) -> &str {
        "array_item_with_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::array_item_with_position::return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let args = ColumnarValue::values_to_arrays(&args)?;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "{} should only be called with a single argument",
                self.name()
            );
        };
        let out = match arg.data_type() {
            DataType::List(f) => {
                let array = as_list_array(arg)?;
                Self::general_array_item_with_position::<Int32Type>(array, f)?
            }
            DataType::LargeList(f) => {
                let array = as_large_list_array(arg)?;
                Self::general_array_item_with_position::<Int64Type>(array, f)?
            }
            DataType::FixedSizeList(_, _) => {
                return Err(DataFusionError::NotImplemented(
                    "fixed size list".to_string(),
                ));
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} should only be called with a list",
                    self.name()
                )));
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}
