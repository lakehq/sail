use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, GenericListArray, ListArray, OffsetSizeTrait,
    PrimitiveArray, StructArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Fields, Int32Type, Int64Type,
};
use datafusion::common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub(crate) struct ArrayItemWithPosition {
    signature: Signature,
}

impl ArrayItemWithPosition {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn item_fields<P: ArrowPrimitiveType>(field: &FieldRef) -> Fields {
        Fields::from(vec![
            Arc::new(Field::new("pos", P::DATA_TYPE, field.is_nullable())),
            Arc::new(Field::new(
                "col",
                field.data_type().clone(),
                field.is_nullable(),
            )),
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
}

impl ScalarUDFImpl for ArrayItemWithPosition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_item_with_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let out = match arg_types {
            [DataType::List(f)] => DataType::List(Arc::new(Field::new_list_field(
                Self::item_type::<Int32Type>(f),
                f.is_nullable(),
            ))),
            [DataType::LargeList(f)] => DataType::LargeList(Arc::new(Field::new_list_field(
                Self::item_type::<Int64Type>(f),
                f.is_nullable(),
            ))),
            [DataType::FixedSizeList(f, n)] => DataType::FixedSizeList(
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
        Ok(out)
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

#[derive(Debug)]
pub(crate) struct ArrayEmptyToNull {
    signature: Signature,
}

impl ArrayEmptyToNull {
    pub(crate) fn new() -> Self {
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

#[derive(Debug)]
pub(crate) struct MapToArray {
    signature: Signature,
    nullable: bool,
}

impl MapToArray {
    pub(crate) fn new(nullable: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            nullable,
        }
    }

    fn nullable_entry_fields(&self, field: &FieldRef) -> Result<Fields> {
        match field.data_type() {
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| f.as_ref().clone().with_nullable(self.nullable))
                    .collect::<Vec<_>>();
                Ok(Fields::from(fields))
            }
            _ => Err(DataFusionError::Internal(format!(
                "map entry should be a struct, found: {:?}",
                field.data_type(),
            ))),
        }
    }

    fn nullable_map_field(&self, field: &FieldRef) -> Result<FieldRef> {
        let data_type = DataType::Struct(self.nullable_entry_fields(field)?);
        Ok(Arc::new(Field::new(field.name(), data_type, true)))
    }
}

impl ScalarUDFImpl for MapToArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[DataType::Map(field, _)] => Ok(DataType::List(self.nullable_map_field(field)?)),
            _ => Err(DataFusionError::Internal(format!(
                "{} should only be called with a map",
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
            DataType::Map(field, _) => {
                let array = as_map_array(arg)?;
                let fields = self.nullable_entry_fields(field)?;
                let inner = StructArray::new(
                    fields,
                    array.entries().columns().to_vec(),
                    array.entries().nulls().cloned(),
                );
                ListArray::try_new(
                    self.nullable_map_field(field)?,
                    array.offsets().clone(),
                    Arc::new(inner),
                    array.nulls().cloned(),
                )?
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} should only be called with a map",
                    self.name(),
                )));
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}
