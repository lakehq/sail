// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/core/least.rs

use arrow::array::{Array, BooleanArray, make_comparator};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::SortOptions;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::ordering::spark_nested_sort_options;

use crate::scalar::conditional::greatest_least_utils::GreatestLeastOperator;

// Having the smallest result first. Like Spark, NULL fields and elements of nested values are
// smaller than any other value; NULL values are handled separately, as they are greater than any
// other value.
const SORT_OPTIONS: SortOptions = spark_nested_sort_options(false);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LeastFunc {
    signature: Signature,
}

impl Default for LeastFunc {
    fn default() -> Self {
        LeastFunc::new()
    }
}

impl LeastFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl GreatestLeastOperator for LeastFunc {
    const NAME: &'static str = "least";

    fn keep_scalar<'a>(lhs: &'a ScalarValue, rhs: &'a ScalarValue) -> Result<&'a ScalarValue> {
        // Manual checking for nulls as:
        // 1. If we're going to use <=, in Rust None is smaller than Some(T), which we don't want
        // 2. And we can't use make_comparator as it has no natural order (Arrow error)
        if lhs.is_null() {
            return Ok(rhs);
        }

        if rhs.is_null() {
            return Ok(lhs);
        }

        if !lhs.data_type().is_nested() {
            return if lhs <= rhs { Ok(lhs) } else { Ok(rhs) };
        }

        // Not using <= as in Rust None is smaller than Some(T)

        // If complex type we can't compare directly as we want null values to be larger
        let cmp = make_comparator(
            lhs.to_array()?.as_ref(),
            rhs.to_array()?.as_ref(),
            SORT_OPTIONS,
        )?;

        if cmp(0, 0).is_le() { Ok(lhs) } else { Ok(rhs) }
    }

    /// Return boolean array where `arr[i] = lhs[i] <= rhs[i]` for all i, where `arr` is the result array
    /// Nulls are always considered larger than any other value
    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
        // Fast path:
        // If both arrays are not nested, have the same length and no nulls, we can use the faster vectorized kernel
        // - If both arrays are not nested: Nested types, such as lists, are not supported as the null semantics are not well-defined.
        // - both array does not have any nulls: cmp::lt_eq will return null if any of the input is null while we want to return false in that case
        if !lhs.data_type().is_nested()
            && lhs.logical_null_count() == 0
            && rhs.logical_null_count() == 0
        {
            return cmp::lt_eq(&lhs, &rhs).map_err(|e| e.into());
        }

        let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

        assert_eq_or_internal_err!(
            lhs.len(),
            rhs.len(),
            "All arrays should have the same length for least comparison"
        );

        let lhs_nulls = lhs.logical_nulls();
        let rhs_nulls = rhs.logical_nulls();
        let is_null =
            |nulls: &Option<NullBuffer>, i| nulls.as_ref().is_some_and(|nulls| nulls.is_null(i));
        let values = BooleanBuffer::collect_bool(lhs.len(), |i| {
            !is_null(&lhs_nulls, i) && (is_null(&rhs_nulls, i) || cmp(i, i).is_le())
        });

        // No nulls as we only want to keep the values that are smaller, its either true or false
        Ok(BooleanArray::new(values, None))
    }
}

impl ScalarUDFImpl for LeastFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "least"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        super::greatest_least_utils::execute_conditional::<Self>(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced_type = super::greatest_least_utils::find_coerced_type::<Self>(arg_types)?;

        Ok(vec![coerced_type; arg_types.len()])
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayData, ArrayRef, FixedSizeListArray, Int32Array, LargeListArray, ListArray,
        StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::scalar::conditional::greatest_least_utils::execute_conditional;
    use crate::scalar::conditional::least::LeastFunc;

    fn least(args: &[ColumnarValue]) -> ArrayData {
        match execute_conditional::<LeastFunc>(args).unwrap() {
            ColumnarValue::Array(array) => array.to_data(),
            ColumnarValue::Scalar(scalar) => scalar.to_array().unwrap().to_data(),
        }
    }

    fn array(array: impl Array + 'static) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(array))
    }

    fn struct_array(a: Vec<Option<i32>>, b: Vec<Option<i32>>, valid: Vec<bool>) -> StructArray {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        StructArray::try_new(
            fields,
            vec![
                Arc::new(Int32Array::from(a)) as ArrayRef,
                Arc::new(Int32Array::from(b)) as ArrayRef,
            ],
            Some(NullBuffer::from(valid)),
        )
        .unwrap()
    }

    #[test]
    fn test_least_struct_null_fields_are_smallest() {
        // Spark: a NULL field is smaller than any other value, and a NULL struct is greater.
        let lhs = struct_array(
            vec![None, Some(1), None, Some(2)],
            vec![Some(5), Some(1), None, Some(0)],
            vec![true, true, false, true],
        );
        let rhs = struct_array(
            vec![Some(1), None, Some(0), None],
            vec![Some(1), None, Some(0), None],
            vec![true, false, true, false],
        );
        let expected = struct_array(
            vec![None, Some(1), Some(0), Some(2)],
            vec![Some(5), Some(1), Some(0), Some(0)],
            vec![true, true, true, true],
        );
        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(
            least(&[array(lhs.clone()), array(rhs.clone())]),
            expected.to_data()
        );
        assert_eq!(least(&[array(rhs), array(lhs)]), expected.to_data());
    }

    #[test]
    fn test_least_struct_scalar_null_fields_are_smallest() {
        let null_field =
            ScalarValue::try_from_array(&struct_array(vec![None], vec![Some(1)], vec![true]), 0)
                .unwrap();
        let one = struct_array(vec![Some(1)], vec![Some(1)], vec![true]);
        let expected: ArrayRef = Arc::new(struct_array(vec![None], vec![Some(1)], vec![true]));
        assert_eq!(
            least(&[
                array(one.clone()),
                ColumnarValue::Scalar(null_field.clone())
            ]),
            expected.to_data()
        );
        let one = ScalarValue::try_from_array(&one, 0).unwrap();
        assert_eq!(
            least(&[
                ColumnarValue::Scalar(one),
                ColumnarValue::Scalar(null_field)
            ]),
            expected.to_data()
        );
    }

    #[test]
    fn test_least_list_null_elements_are_smallest() {
        // Spark: a NULL element is smaller than any other value, a shorter list with the same
        // prefix is smaller, and a NULL list is greater than any other value.
        let lhs = vec![
            Some(vec![None, Some(5)]),
            Some(vec![Some(1)]),
            None,
            Some(vec![Some(2)]),
        ];
        let rhs = vec![
            Some(vec![Some(1), Some(1)]),
            Some(vec![Some(1), None]),
            Some(vec![Some(0)]),
            None,
        ];
        let expected = vec![
            Some(vec![None, Some(5)]),
            Some(vec![Some(1)]),
            Some(vec![Some(0)]),
            Some(vec![Some(2)]),
        ];

        let list = |v: &Vec<Option<Vec<Option<i32>>>>| {
            ListArray::from_iter_primitive::<Int32Type, _, _>(v.clone())
        };
        let expected_list: ArrayRef = Arc::new(list(&expected));
        assert_eq!(
            least(&[array(list(&lhs)), array(list(&rhs))]),
            expected_list.to_data()
        );
        assert_eq!(
            least(&[array(list(&rhs)), array(list(&lhs))]),
            expected_list.to_data()
        );

        let large = |v: &Vec<Option<Vec<Option<i32>>>>| {
            LargeListArray::from_iter_primitive::<Int32Type, _, _>(v.clone())
        };
        let expected_large: ArrayRef = Arc::new(large(&expected));
        assert_eq!(
            least(&[array(large(&lhs)), array(large(&rhs))]),
            expected_large.to_data()
        );
        assert_eq!(
            least(&[array(large(&rhs)), array(large(&lhs))]),
            expected_large.to_data()
        );
    }

    #[test]
    fn test_least_fixed_size_list_null_elements_are_smallest() {
        let fixed = |v: Vec<Option<Vec<Option<i32>>>>| -> ArrayRef {
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                v, 2,
            ))
        };
        let lhs = fixed(vec![
            Some(vec![None, Some(5)]),
            Some(vec![Some(1), Some(1)]),
            None,
            Some(vec![Some(2), Some(0)]),
        ]);
        let rhs = fixed(vec![
            Some(vec![Some(1), Some(1)]),
            Some(vec![Some(1), None]),
            Some(vec![Some(0), Some(0)]),
            None,
        ]);
        let expected = fixed(vec![
            Some(vec![None, Some(5)]),
            Some(vec![Some(1), None]),
            Some(vec![Some(0), Some(0)]),
            Some(vec![Some(2), Some(0)]),
        ]);
        let args = |l: &ArrayRef, r: &ArrayRef| {
            [
                ColumnarValue::Array(Arc::clone(l)),
                ColumnarValue::Array(Arc::clone(r)),
            ]
        };
        assert_eq!(least(&args(&lhs, &rhs)), expected.to_data());
        assert_eq!(least(&args(&rhs, &lhs)), expected.to_data());
    }

    #[test]
    fn test_least_list_of_struct_null_fields_are_smallest() {
        let list_of_struct = |a: Option<i32>| -> ArrayRef {
            let field = Arc::new(Field::new("a", DataType::Int32, true));
            let values = StructArray::try_new(
                Fields::from(vec![Arc::clone(&field)]),
                vec![Arc::new(Int32Array::from(vec![a])) as ArrayRef],
                None,
            )
            .unwrap();
            let item = Arc::new(Field::new_list_field(values.data_type().clone(), true));
            Arc::new(ListArray::new(
                item,
                OffsetBuffer::from_lengths([1]),
                Arc::new(values),
                None,
            ))
        };
        let null_field = list_of_struct(None);
        let zero = list_of_struct(Some(0));
        let args = |l: &ArrayRef, r: &ArrayRef| {
            [
                ColumnarValue::Array(Arc::clone(l)),
                ColumnarValue::Array(Arc::clone(r)),
            ]
        };
        assert_eq!(least(&args(&null_field, &zero)), null_field.to_data());
        assert_eq!(least(&args(&zero, &null_field)), null_field.to_data());
    }

    #[test]
    fn test_least_return_types_without_common_supertype_in_arg_type() {
        let least = LeastFunc::new();
        let return_type = least
            .coerce_types(&[DataType::Decimal128(10, 3), DataType::Decimal128(10, 4)])
            .unwrap();
        assert_eq!(
            return_type,
            vec![DataType::Decimal128(11, 4), DataType::Decimal128(11, 4)]
        );
    }
}
