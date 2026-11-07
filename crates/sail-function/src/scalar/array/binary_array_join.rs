use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericBinaryBuilder, GenericListArray, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_nested_utils::make_scalar_function;

/// Joins the non-null binary elements of each list with a binary delimiter.
///
/// This is how Spark's `listagg` joins binary values (`ByteArray.concatWS`), where the bytes are
/// joined as they are, so they do not have to be valid UTF-8. A NULL delimiter joins the values
/// without a delimiter, and a NULL list gives NULL.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/collect.scala#L572-L600>
///
/// The first argument is a list of `Binary` or `LargeBinary` values and the second argument is
/// `Binary`. The result has the type of the list values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBinaryArrayJoin {
    signature: Signature,
}

impl Default for SparkBinaryArrayJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBinaryArrayJoin {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBinaryArrayJoin {
    fn name(&self) -> &str {
        "spark_binary_array_join"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [
                DataType::List(field) | DataType::LargeList(field),
                DataType::Binary | DataType::Null,
            ] => match field.data_type() {
                DataType::Binary => Ok(DataType::Binary),
                DataType::LargeBinary => Ok(DataType::LargeBinary),
                other => plan_err!(
                    "spark_binary_array_join expects a list of binary values, got {other}"
                ),
            },
            _ => plan_err!(
                "spark_binary_array_join expects a list of binary values and a binary delimiter, got {arg_types:?}"
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(binary_array_join)(&args.args)
    }
}

fn binary_array_join(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [list, delimiter] = args else {
        return exec_err!("spark_binary_array_join expects 2 arguments");
    };
    match (list.data_type(), delimiter.data_type()) {
        (DataType::List(_), DataType::Binary | DataType::Null) => {
            join_list_values(list.as_list::<i32>(), delimiter)
        }
        (DataType::LargeList(_), DataType::Binary | DataType::Null) => {
            join_list_values(list.as_list::<i64>(), delimiter)
        }
        (list_type, delimiter_type) => exec_err!(
            "spark_binary_array_join expects a list of binary values and a binary delimiter, got {list_type} and {delimiter_type}"
        ),
    }
}

fn join_list_values<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    delimiter: &ArrayRef,
) -> Result<ArrayRef> {
    match list.values().data_type() {
        DataType::Binary => join::<O, i32>(list, delimiter),
        DataType::LargeBinary => join::<O, i64>(list, delimiter),
        other => exec_err!("spark_binary_array_join expects a list of binary values, got {other}"),
    }
}

fn join<O: OffsetSizeTrait, V: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    delimiter: &ArrayRef,
) -> Result<ArrayRef> {
    let values = list.values().as_binary::<V>();
    let delimiter = delimiter.as_binary_opt::<i32>();
    let mut builder = GenericBinaryBuilder::<V>::with_capacity(list.len(), values.values().len());
    let mut buffer = Vec::new();
    let offsets = list.value_offsets();
    for row in 0..list.len() {
        if list.is_null(row) {
            builder.append_null();
            continue;
        }
        let delimiter = match delimiter {
            Some(delimiter) if delimiter.is_valid(row) => delimiter.value(row),
            _ => &[][..],
        };
        buffer.clear();
        let mut first = true;
        for index in offsets[row].as_usize()..offsets[row + 1].as_usize() {
            if values.is_null(index) {
                continue;
            }
            if !first {
                buffer.extend_from_slice(delimiter);
            }
            first = false;
            buffer.extend_from_slice(values.value(index));
        }
        builder.append_value(&buffer);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BinaryArray, LargeBinaryArray, LargeListArray, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Field;

    use super::*;

    fn values() -> Vec<Option<&'static [u8]>> {
        vec![
            Some(b"\x80"),
            Some(b"\xc3"),
            None,
            Some(b"\x41\xff"),
            Some(b""),
            None,
        ]
    }

    #[test]
    fn test_binary_array_join() -> Result<()> {
        // Rows: [80, C3, NULL, 41FF], [], NULL, [NULL]
        let lengths = [4, 0, 0, 2];
        let nulls = Some(vec![true, true, false, true].into());
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Binary, true)),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(BinaryArray::from(values())),
            nulls.clone(),
        ));
        let large_list: ArrayRef = Arc::new(LargeListArray::new(
            Arc::new(Field::new_list_field(DataType::LargeBinary, true)),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(LargeBinaryArray::from(values())),
            nulls,
        ));
        let delimiter: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"\x00".as_slice()),
            Some(b"\x80"),
            Some(b"\x00"),
            None,
        ]));
        let expected = vec![
            Some(b"\x80\x00\xc3\x00\x41\xff".as_slice()),
            Some(b""),
            None,
            Some(b""),
        ];

        let result = binary_array_join(&[list, Arc::clone(&delimiter)])?;
        assert_eq!(
            result.as_binary::<i32>(),
            &BinaryArray::from(expected.clone())
        );
        let result = binary_array_join(&[large_list, delimiter])?;
        assert_eq!(result.as_binary::<i64>(), &LargeBinaryArray::from(expected));
        Ok(())
    }

    #[test]
    fn test_binary_array_join_null_delimiter() -> Result<()> {
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Binary, true)),
            OffsetBuffer::from_lengths([4]),
            Arc::new(BinaryArray::from(values()[..4].to_vec())),
            None,
        ));
        let delimiter: ArrayRef = Arc::new(datafusion::arrow::array::NullArray::new(1));
        let result = binary_array_join(&[list, delimiter])?;
        assert_eq!(
            result.as_binary::<i32>(),
            &BinaryArray::from(vec![Some(b"\x80\xc3\x41\xff".as_slice())])
        );
        Ok(())
    }

    #[test]
    fn test_return_type() {
        let udf = SparkBinaryArrayJoin::new();
        let list = |data_type| DataType::List(Arc::new(Field::new_list_field(data_type, true)));
        assert_eq!(
            udf.return_type(&[list(DataType::Binary), DataType::Binary])
                .ok(),
            Some(DataType::Binary)
        );
        assert_eq!(
            udf.return_type(&[list(DataType::LargeBinary), DataType::Binary])
                .ok(),
            Some(DataType::LargeBinary)
        );
        assert!(
            udf.return_type(&[list(DataType::Utf8), DataType::Binary])
                .is_err()
        );
    }
}
