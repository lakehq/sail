use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{plan_err, Result};

pub fn get_fields(data_types: &[DataType], names: &[String]) -> Result<Vec<Field>> {
    if data_types.len() != names.len() {
        return plan_err!("data types and names must have the same length for fields");
    }
    let fields = names
        .iter()
        .zip(data_types.iter())
        .map(|(n, t)| Field::new(n.clone(), t.clone(), false))
        .collect::<Vec<_>>();
    Ok(fields)
}

pub fn get_struct_array_type(data_types: &[DataType], names: &[String]) -> Result<DataType> {
    Ok(DataType::List(Arc::new(Field::new_list_field(
        DataType::Struct(get_fields(data_types, names)?.into()),
        false,
    ))))
}

pub fn get_list_field(data_type: &DataType) -> Result<FieldRef> {
    match data_type {
        DataType::List(field) => Ok(field.clone()),
        _ => plan_err!("invalid list data type: {data_type}"),
    }
}

pub fn build_singleton_list_array(array: ArrayRef) -> ArrayRef {
    let array = ListArray::new(
        Arc::new(Field::new_list_field(array.data_type().clone(), false)),
        OffsetBuffer::from_lengths(vec![array.len()]),
        array,
        None,
    );
    Arc::new(array)
}

pub fn build_list_array(arrays: &[ArrayRef], data_type: &DataType) -> Result<ArrayRef> {
    let field = get_list_field(data_type)?;
    let array = if arrays.is_empty() {
        ListArray::new_null(field, 0)
    } else {
        let lengths = arrays.iter().map(|x| x.len()).collect::<Vec<_>>();
        let arrays = arrays.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            concat(&arrays)?,
            None,
        )
    };
    Ok(Arc::new(array))
}
