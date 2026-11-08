// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/core/greatest.rs

use arrow::array::{Array, BooleanArray, make_comparator};
use arrow::buffer::BooleanBuffer;
use arrow::compute::SortOptions;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::conditional::greatest_least_utils::GreatestLeastOperator;

const SORT_OPTIONS: SortOptions = SortOptions {
    // We want greatest first
    descending: false,

    // NULL will be less than any other value
    nulls_first: true,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GreatestFunc {
    signature: Signature,
}

impl Default for GreatestFunc {
    fn default() -> Self {
        GreatestFunc::new()
    }
}

impl GreatestFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl GreatestLeastOperator for GreatestFunc {
    const NAME: &'static str = "greatest";

    fn keep_scalar<'a>(lhs: &'a ScalarValue, rhs: &'a ScalarValue) -> Result<&'a ScalarValue> {
        if !lhs.data_type().is_nested() {
            return if lhs >= rhs { Ok(lhs) } else { Ok(rhs) };
        }

        // If complex type we can't compare directly as we want null values to be smaller
        let cmp = make_comparator(
            lhs.to_array()?.as_ref(),
            rhs.to_array()?.as_ref(),
            SORT_OPTIONS,
        )?;

        if cmp(0, 0).is_ge() { Ok(lhs) } else { Ok(rhs) }
    }

    /// Return boolean array where `arr[i] = lhs[i] >= rhs[i]` for all i, where `arr` is the result array
    /// Nulls are always considered smaller than any other value
    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
        // Fast path:
        // If both arrays are not nested, have the same length and no nulls, we can use the faster vectorized kernel
        // - If both arrays are not nested: Nested types, such as lists, are not supported as the null semantics are not well-defined.
        // - both array does not have any nulls: cmp::gt_eq will return null if any of the input is null while we want to return false in that case
        if !lhs.data_type().is_nested()
            && lhs.logical_null_count() == 0
            && rhs.logical_null_count() == 0
        {
            return cmp::gt_eq(&lhs, &rhs).map_err(|e| e.into());
        }

        let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

        assert_eq_or_internal_err!(
            lhs.len(),
            rhs.len(),
            "All arrays should have the same length for greatest comparison"
        );

        let values = BooleanBuffer::collect_bool(lhs.len(), |i| cmp(i, i).is_ge());

        // No nulls as we only want to keep the values that are larger, its either true or false
        Ok(BooleanArray::new(values, None))
    }
}

impl ScalarUDFImpl for GreatestFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "greatest"
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
    use arrow::datatypes::DataType;
    use datafusion_expr::ScalarUDFImpl;

    use crate::scalar::conditional::greatest::GreatestFunc;

    #[test]
    fn test_greatest_return_types_without_common_supertype_in_arg_type() {
        let greatest = GreatestFunc::new();
        let return_type = greatest
            .coerce_types(&[DataType::Decimal128(10, 3), DataType::Decimal128(10, 4)])
            .unwrap();
        assert_eq!(
            return_type,
            vec![DataType::Decimal128(11, 4), DataType::Decimal128(11, 4)]
        );
    }
}
