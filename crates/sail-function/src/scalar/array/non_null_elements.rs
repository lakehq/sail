use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Declares that the elements of a list are never NULL, for elements the caller guarantees are
/// never NULL.
///
/// This is used for results such as `collect_set`, `array_intersect` and `array_except` that
/// cannot hold NULL elements, where Spark declares the elements as never NULL. The list is
/// returned as it is with the element field changed, so no data is copied. DataFusion does not
/// support a CAST to a list with non-nullable elements of a struct type, so a CAST cannot be used
/// for this.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NonNullElements {
    signature: Signature,
}

impl Default for NonNullElements {
    fn default() -> Self {
        Self::new()
    }
}

impl NonNullElements {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

fn non_null_element_type(data_type: &DataType) -> Result<DataType> {
    let field = |field: &FieldRef| Arc::new(field.as_ref().clone().with_nullable(false));
    match data_type {
        DataType::List(f) => Ok(DataType::List(field(f))),
        DataType::LargeList(f) => Ok(DataType::LargeList(field(f))),
        other => plan_err!("spark_non_null_elements expects a list, got {other}"),
    }
}

impl ScalarUDFImpl for NonNullElements {
    fn name(&self) -> &str {
        "spark_non_null_elements"
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
            return plan_err!("spark_non_null_elements expects one argument");
        };
        let data_type = non_null_element_type(field.data_type())?;
        Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("spark_non_null_elements expects one argument");
        };
        let data_type = args.return_field.data_type();
        match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(with_element_field(array, data_type)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = with_element_field(&scalar.to_array()?, data_type)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }
}

/// Returns the list with the element field of `data_type`.
fn with_element_field(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    match data_type {
        DataType::List(field) => list_with_element_field::<i32>(array, field),
        DataType::LargeList(field) => list_with_element_field::<i64>(array, field),
        other => exec_err!("spark_non_null_elements expects a list, got {other}"),
    }
}

fn list_with_element_field<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let Some(list) = array.as_list_opt::<O>() else {
        return exec_err!(
            "spark_non_null_elements expects a list of {}, got {}",
            field.data_type(),
            array.data_type()
        );
    };
    let values = if list.values().data_type() == field.data_type() {
        Arc::clone(list.values())
    } else {
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        cast_with_options(list.values(), field.data_type(), &options)?
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray, StructArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{Field, Fields};

    use super::*;

    #[test]
    fn test_non_null_elements_of_structs() -> Result<()> {
        let fields = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        let values = StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![None, Some(1)]))],
            None,
        );
        let field = Arc::new(Field::new_list_field(DataType::Struct(fields), true));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2, 0]),
            Arc::new(values),
            Some(vec![true, false].into()),
        ));
        let data_type = non_null_element_type(list.data_type())?;
        let result = with_element_field(&list, &data_type)?;
        assert_eq!(result.data_type(), &data_type);
        assert_eq!(result.len(), 2);
        assert!(result.is_null(1));
        assert_eq!(
            result.as_list::<i32>().values().to_data(),
            list.as_list::<i32>().values().to_data()
        );
        Ok(())
    }

    #[test]
    fn test_non_null_elements_rejects_null_elements() -> Result<()> {
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
            datafusion::arrow::datatypes::Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(1), None])]));
        let data_type = non_null_element_type(list.data_type())?;
        assert!(with_element_field(&list, &data_type).is_err());
        assert!(non_null_element_type(&DataType::Int32).is_err());
        Ok(())
    }
}
