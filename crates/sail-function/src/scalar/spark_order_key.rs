// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Encodes an array or a struct as bytes that compare like the value in Spark's
/// ordering, for a sort that compares values one by one, such as the sample and the
/// split points of a range repartitioning, which cannot compare nested values.
///
/// The bytes are in Arrow's row format. In Spark's ascending ordering, arrays and
/// structs compare element by element, a shorter array that is a prefix comes first,
/// and a null element or field comes first. The descending ordering reverses the
/// whole comparison. A null value stays null, so that the sort that reads the bytes
/// places it as asked.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ordering.scala>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkOrderKey {
    signature: Signature,
    descending: bool,
}

impl SparkOrderKey {
    pub fn new(descending: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            descending,
        }
    }

    pub fn descending(&self) -> bool {
        self.descending
    }

    /// Whether a sort that compares values one by one needs the key to order values of
    /// the type.
    pub fn is_needed(data_type: &DataType) -> bool {
        match data_type {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_) => true,
            DataType::Dictionary(_, value) => Self::is_needed(value),
            _ => false,
        }
    }

    fn encode(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let options = SortOptions {
            descending: self.descending,
            // A null element or field comes first in the ascending ordering.
            nulls_first: !self.descending,
        };
        let converter = RowConverter::new(vec![SortField::new_with_options(
            array.data_type().clone(),
            options,
        )])?;
        let rows = converter.convert_columns(&[Arc::clone(array)])?;
        let mut builder = BinaryBuilder::with_capacity(array.len(), rows.size());
        for (index, row) in rows.iter().enumerate() {
            if array.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(row.as_ref());
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarUDFImpl for SparkOrderKey {
    fn name(&self) -> &str {
        if self.descending {
            "spark_order_key_desc"
        } else {
            "spark_order_key"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return internal_err!("{} expects one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("{} expects one argument", self.name());
        };
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.encode(array)?)),
            ColumnarValue::Scalar(value) => {
                let array = self.encode(&value.to_array()?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use arrow::array::{AsArray, ListArray};
    use arrow::datatypes::Int64Type;

    use super::*;

    fn keys(values: Vec<Option<Vec<Option<i64>>>>, descending: bool) -> Vec<Option<Vec<u8>>> {
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(values));
        let encoded = SparkOrderKey::new(descending).encode(&array).unwrap();
        encoded
            .as_binary::<i32>()
            .iter()
            .map(|x| x.map(|x| x.to_vec()))
            .collect()
    }

    #[test]
    fn test_keys_compare_like_spark_arrays() {
        // In ascending order, a prefix comes first and a null element comes first:
        // [] < [null] < [0] < [0, 1] < [1].
        let values = vec![
            Some(vec![Some(1)]),
            Some(vec![Some(0), Some(1)]),
            Some(vec![None]),
            Some(vec![]),
            Some(vec![Some(0)]),
        ];
        let ascending = [3, 2, 4, 1, 0];
        for descending in [false, true] {
            let keys = keys(values.clone(), descending);
            let mut order = (0..keys.len()).collect::<Vec<_>>();
            order.sort_by(|a, b| keys[*a].cmp(&keys[*b]));
            let mut expected = ascending.to_vec();
            if descending {
                expected.reverse();
            }
            assert_eq!(order, expected, "descending: {descending}");
        }
    }

    #[test]
    fn test_null_value_stays_null() {
        let keys = keys(vec![None, Some(vec![Some(0)])], false);
        assert!(keys[0].is_none());
        assert!(keys[1].is_some());
    }
}
