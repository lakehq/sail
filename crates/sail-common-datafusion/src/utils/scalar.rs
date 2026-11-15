use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};

/// Extract a scalar without rebuilding list child fields and losing their metadata.
pub fn scalar_from_array(array: &ArrayRef, index: usize) -> Result<ScalarValue> {
    Ok(match array.data_type() {
        DataType::List(_) => ScalarValue::List(Arc::new(array.as_list::<i32>().slice(index, 1))),
        DataType::LargeList(_) => {
            ScalarValue::LargeList(Arc::new(array.as_list::<i64>().slice(index, 1)))
        }
        DataType::FixedSizeList(_, _) => {
            ScalarValue::FixedSizeList(Arc::new(array.as_fixed_size_list().slice(index, 1)))
        }
        _ => ScalarValue::try_from_array(array, index)?,
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeListArray, Int32Array, LargeListArray, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn list_scalars_preserve_fields_and_validity() -> Result<()> {
        let field = Arc::new(
            Field::new("child", DataType::Int32, true)
                .with_metadata([("owner".to_string(), "retained".to_string())].into()),
        );
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
        let validity = Some(NullBuffer::from(vec![true, true, false]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::new(vec![0, 2, 2, 2].into()),
                Arc::clone(&values),
                validity.clone(),
            )),
            Arc::new(LargeListArray::new(
                Arc::clone(&field),
                OffsetBuffer::new(vec![0_i64, 2, 2, 2].into()),
                values,
                validity,
            )),
            Arc::new(FixedSizeListArray::new(
                field,
                1,
                Arc::new(Int32Array::from(vec![Some(7), None, None])),
                Some(NullBuffer::from(vec![true, true, false])),
            )),
        ];
        for array in arrays {
            for index in 0..array.len() {
                let scalar = scalar_from_array(&array, index)?;
                assert_eq!(&scalar.data_type(), array.data_type());
                assert_eq!(scalar.is_null(), array.is_null(index));
                assert_eq!(
                    scalar.to_array()?.to_data(),
                    array.slice(index, 1).to_data()
                );
            }
        }
        Ok(())
    }
}
