use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait, StructArray, make_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, internal_err, plan_datafusion_err,
};
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

    fn field_name<'a>(&self, value: Option<&'a ScalarValue>) -> Result<&'a str> {
        value
            .and_then(|x| x.try_as_str().flatten())
            .filter(|x| !x.is_empty())
            .ok_or_else(|| {
                plan_datafusion_err!("{} requires a non-empty literal field name", self.name())
            })
    }

    fn list_field(data_type: &DataType) -> Result<&FieldRef> {
        match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Ok(field),
            _ => exec_err!("array_struct_field requires a list, got {data_type}"),
        }
    }

    fn list_item_field(field: &FieldRef, nullable: bool) -> FieldRef {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(Field::LIST_FIELD_DEFAULT_NAME)
                .with_nullable(nullable),
        )
    }

    fn return_type_for_list(&self, data_type: &DataType, field_name: &str) -> Result<DataType> {
        let list_field = Self::list_field(data_type)?;
        let DataType::Struct(fields) = list_field.data_type() else {
            return exec_err!(
                "array_struct_field requires a list of structs, got {}",
                list_field.data_type()
            );
        };
        let (_, child) = fields
            .find(field_name)
            .ok_or_else(|| exec_datafusion_err!("missing field: {field_name}"))?;
        let nullable = list_field.is_nullable() || child.is_nullable();
        let field = Self::list_item_field(child, nullable);
        match data_type {
            DataType::List(_) => Ok(DataType::List(field)),
            DataType::LargeList(_) => Ok(DataType::LargeList(field)),
            DataType::FixedSizeList(_, size) => Ok(DataType::FixedSizeList(field, *size)),
            _ => unreachable!("list_field accepted only list data types"),
        }
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
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [array_field, _] = args.arg_fields else {
            return exec_err!(
                "{} requires 2 arguments, got {}",
                self.name(),
                args.arg_fields.len()
            );
        };
        let field_name = self.field_name(args.scalar_arguments.get(1).copied().flatten())?;
        let data_type = self.return_type_for_list(array_field.data_type(), field_name)?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
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
