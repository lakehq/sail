use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait, StructArray, make_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{Result, exec_datafusion_err, exec_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayStructField {
    signature: Signature,
}

impl Default for ArrayStructField {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayStructField {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn list_field(data_type: &DataType) -> Result<&FieldRef> {
        sail_type_system::policies::array_struct_field_details::list_field(data_type)
    }

    fn list_item_field(field: &FieldRef, nullable: bool) -> FieldRef {
        sail_type_system::policies::array_struct_field_details::list_item_field(field, nullable)
    }

    fn struct_values(values: &ArrayRef, field_name: &str) -> Result<(ArrayRef, FieldRef)> {
        let Some(struct_array) = values.as_any().downcast_ref::<StructArray>() else {
            return exec_err!(
                "array_struct_field requires list values to be structs, got {}",
                values.data_type()
            );
        };
        let (index, field) = struct_array
            .fields()
            .find(field_name)
            .ok_or_else(|| exec_datafusion_err!("missing field: {field_name}"))?;
        let values = struct_array.column(index);
        let nulls = NullBuffer::union(struct_array.nulls(), values.nulls());
        let data = values.to_data().into_builder().nulls(nulls).build()?;
        Ok((make_array(data), Arc::clone(field)))
    }

    fn project_list<O>(array: &GenericListArray<O>, field_name: &str) -> Result<ArrayRef>
    where
        O: OffsetSizeTrait,
    {
        let list_field = Self::list_field(array.data_type())?;
        let (values, child) = Self::struct_values(array.values(), field_name)?;
        Ok(Arc::new(GenericListArray::<O>::try_new(
            Self::list_item_field(&child, list_field.is_nullable() || child.is_nullable()),
            array.offsets().clone(),
            values,
            array.nulls().cloned(),
        )?))
    }

    fn project_fixed_size_list(
        array: &FixedSizeListArray,
        size: i32,
        field_name: &str,
    ) -> Result<ArrayRef> {
        let list_field = Self::list_field(array.data_type())?;
        let (values, child) = Self::struct_values(array.values(), field_name)?;
        Ok(Arc::new(FixedSizeListArray::try_new(
            Self::list_item_field(&child, list_field.is_nullable() || child.is_nullable()),
            size,
            values,
            array.nulls().cloned(),
        )?))
    }
}

impl ScalarUDFImpl for ArrayStructField {
    fn name(&self) -> &str {
        "array_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_array_struct_field_typing::array_struct_field_return_type(
            _arg_types,
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::array_struct_field_details::return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, arg_fields, ..
        } = args;
        let field_name = arg_fields
            .get(1)
            .and_then(|_| args.get(1))
            .and_then(|arg| match arg {
                ColumnarValue::Scalar(value) => value.try_as_str().flatten(),
                ColumnarValue::Array(_) => None,
            })
            .filter(|x| !x.is_empty())
            .ok_or_else(|| {
                plan_datafusion_err!("{} requires a non-empty literal field name", self.name())
            })?;
        let arrays = ColumnarValue::values_to_arrays(&args[..1])?;
        let [array] = arrays.as_slice() else {
            return exec_err!("{} requires an array argument", self.name());
        };
        let out = match array.data_type() {
            DataType::List(_) => Self::project_list(as_list_array(array)?, field_name)?,
            DataType::LargeList(_) => Self::project_list(as_large_list_array(array)?, field_name)?,
            DataType::FixedSizeList(_, size) => {
                let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() else {
                    return exec_err!(
                        "array_struct_field expected FixedSizeListArray, got {}",
                        array.data_type()
                    );
                };
                Self::project_fixed_size_list(array, *size, field_name)?
            }
            _ => {
                return exec_err!(
                    "array_struct_field requires a list, got {}",
                    array.data_type()
                );
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}
