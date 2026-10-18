use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericListArray, GenericListViewArray,
    MutableArrayData, OffsetSizeTrait, PrimitiveArray, StructArray, make_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Fields, Int32Type, Int64Type,
};
use datafusion::common::cast::{as_large_list_array, as_list_array};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{exec_err, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};

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
        Fields::from(vec![
            Arc::new(Field::new("pos", P::DATA_TYPE, field.is_nullable())),
            Arc::new(field.as_ref().clone().with_name("col")),
        ])
    }

    fn item_type<P: ArrowPrimitiveType>(field: &FieldRef) -> DataType {
        DataType::Struct(Self::item_fields::<P>(field))
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

    fn list_view_item_with_position<P: ArrowPrimitiveType>(
        array: &GenericListViewArray<P::Native>,
        field: &FieldRef,
    ) -> Result<ArrayRef>
    where
        P::Native: OffsetSizeTrait,
        PrimitiveArray<P>: From<Vec<P::Native>>,
    {
        let mut offsets = Vec::with_capacity(array.len() + 1);
        offsets.push(P::Native::usize_as(0));
        let source = array.values().to_data();
        let mut values = MutableArrayData::new(vec![&source], false, 0);
        let mut positions = Vec::new();
        // Shared view elements can have different positions in different rows.
        for row in 0..array.len() {
            if array.is_valid(row) {
                let start = array.value_offsets()[row].as_usize();
                let size = array.value_sizes()[row].as_usize();
                if size > P::Native::MAX_OFFSET - positions.len() {
                    return exec_err!("array_item_with_position output offset overflow");
                }
                values.try_extend(0, start, start + size)?;
                positions.extend((0..size).map(P::Native::usize_as));
            }
            offsets.push(P::Native::usize_as(positions.len()));
        }
        let values = StructArray::try_new(
            Self::item_fields::<P>(field),
            vec![
                Arc::new(PrimitiveArray::<P>::from(positions)) as ArrayRef,
                make_array(values.freeze()),
            ],
            None,
        )?;
        Ok(Arc::new(GenericListArray::<P::Native>::try_new(
            Arc::new(Field::new_list_field(
                Self::item_type::<P>(field),
                field.is_nullable(),
            )),
            OffsetBuffer::new(offsets.into()),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return internal_err!("{} should only be called with a list", self.name());
        };
        let out = match field.data_type() {
            DataType::List(f) | DataType::ListView(f) => DataType::List(Arc::new(
                Field::new_list_field(Self::item_type::<Int32Type>(f), f.is_nullable()),
            )),
            DataType::LargeList(f) | DataType::LargeListView(f) => DataType::LargeList(Arc::new(
                Field::new_list_field(Self::item_type::<Int64Type>(f), f.is_nullable()),
            )),
            DataType::FixedSizeList(f, n) => DataType::FixedSizeList(
                Arc::new(Field::new_list_field(
                    Self::item_type::<Int32Type>(f),
                    f.is_nullable(),
                )),
                *n,
            ),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} should only be called with a list",
                    self.name()
                )));
            }
        };
        Ok(Arc::new(Field::new(
            self.name(),
            super::list_view_to_list_type(&out),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;
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
            DataType::ListView(f) => {
                Self::list_view_item_with_position::<Int32Type>(arg.as_list_view::<i32>(), f)?
            }
            DataType::LargeListView(f) => {
                Self::list_view_item_with_position::<Int64Type>(arg.as_list_view::<i64>(), f)?
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
        super::cast_list_result(
            ColumnarValue::Array(Arc::new(out)),
            return_field.data_type(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn positions_preserve_element_metadata() -> Result<()> {
        let element = Arc::new(
            Field::new("element", DataType::Int64, false)
                .with_metadata(HashMap::from([("logical_type".into(), "element".into())])),
        );
        for data_type in [
            DataType::List(Arc::clone(&element)),
            DataType::LargeList(Arc::clone(&element)),
            DataType::ListView(Arc::clone(&element)),
            DataType::LargeListView(Arc::clone(&element)),
            DataType::FixedSizeList(Arc::clone(&element), 2),
        ] {
            let result = ArrayItemWithPosition::new().return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::new(Field::new("input", data_type, true))],
                scalar_arguments: &[None],
            })?;
            let (DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)) = result.data_type()
            else {
                return internal_err!("expected a list");
            };
            let DataType::Struct(fields) = item.data_type() else {
                return internal_err!("expected a struct");
            };
            assert_eq!(
                fields[1].as_ref(),
                &element.as_ref().clone().with_name("col")
            );
            assert!(fields[0].metadata().is_empty());
        }
        Ok(())
    }
}
