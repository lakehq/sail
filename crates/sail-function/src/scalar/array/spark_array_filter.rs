use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Comparison operator for array filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterOp {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

impl FilterOp {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            ">" => Some(FilterOp::GreaterThan),
            ">=" => Some(FilterOp::GreaterThanOrEqual),
            "<" => Some(FilterOp::LessThan),
            "<=" => Some(FilterOp::LessThanOrEqual),
            "=" | "==" => Some(FilterOp::Equal),
            "!=" | "<>" => Some(FilterOp::NotEqual),
            _ => None,
        }
    }

    fn apply_i32(&self, a: i32, b: i32) -> bool {
        match self {
            FilterOp::GreaterThan => a > b,
            FilterOp::GreaterThanOrEqual => a >= b,
            FilterOp::LessThan => a < b,
            FilterOp::LessThanOrEqual => a <= b,
            FilterOp::Equal => a == b,
            FilterOp::NotEqual => a != b,
        }
    }

    fn apply_i64(&self, a: i64, b: i64) -> bool {
        match self {
            FilterOp::GreaterThan => a > b,
            FilterOp::GreaterThanOrEqual => a >= b,
            FilterOp::LessThan => a < b,
            FilterOp::LessThanOrEqual => a <= b,
            FilterOp::Equal => a == b,
            FilterOp::NotEqual => a != b,
        }
    }

    fn apply_f64(&self, a: f64, b: f64) -> bool {
        match self {
            FilterOp::GreaterThan => a > b,
            FilterOp::GreaterThanOrEqual => a >= b,
            FilterOp::LessThan => a < b,
            FilterOp::LessThanOrEqual => a <= b,
            FilterOp::Equal => (a - b).abs() < f64::EPSILON,
            FilterOp::NotEqual => (a - b).abs() >= f64::EPSILON,
        }
    }
}

/// SparkArrayFilter filters array elements based on a simple comparison predicate.
///
/// This implementation supports predicates of the form: element <op> constant
/// where <op> is one of: >, >=, <, <=, =, !=
///
/// The UDF stores the operator and comparison value, which are set during construction
/// by the resolver when it parses the lambda expression.
///
/// Usage: Created by the resolver when processing filter(array, lambda x: x > value)
#[derive(Debug, Clone, PartialEq)]
pub struct SparkArrayFilter {
    signature: Signature,
    /// The comparison operator
    op: FilterOp,
    /// The value to compare against (as ScalarValue for flexibility)
    compare_value: ScalarValue,
    /// Whether the comparison is reversed (value <op> element instead of element <op> value)
    reversed: bool,
}

impl SparkArrayFilter {
    pub fn new(op: FilterOp, compare_value: ScalarValue, reversed: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            op,
            compare_value,
            reversed,
        }
    }

    /// Create a filter for: element > value
    pub fn greater_than(value: ScalarValue) -> Self {
        Self::new(FilterOp::GreaterThan, value, false)
    }

    /// Create a filter for: element >= value
    pub fn greater_than_or_equal(value: ScalarValue) -> Self {
        Self::new(FilterOp::GreaterThanOrEqual, value, false)
    }

    /// Create a filter for: element < value
    pub fn less_than(value: ScalarValue) -> Self {
        Self::new(FilterOp::LessThan, value, false)
    }

    /// Create a filter for: element <= value
    pub fn less_than_or_equal(value: ScalarValue) -> Self {
        Self::new(FilterOp::LessThanOrEqual, value, false)
    }

    /// Create a filter for: element == value
    pub fn equal(value: ScalarValue) -> Self {
        Self::new(FilterOp::Equal, value, false)
    }

    /// Create a filter for: element != value
    pub fn not_equal(value: ScalarValue) -> Self {
        Self::new(FilterOp::NotEqual, value, false)
    }
}

impl std::hash::Hash for SparkArrayFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        // ScalarValue doesn't implement Hash directly, use debug string as workaround
        format!("{:?}", self.compare_value).hash(state);
        self.reversed.hash(state);
    }
}

impl Eq for SparkArrayFilter {}

impl ScalarUDFImpl for SparkArrayFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_array_filter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(field) => Ok(DataType::List(field.clone())),
            DataType::LargeList(field) => Ok(DataType::LargeList(field.clone())),
            other => exec_err!("spark_array_filter expects List type, got {:?}", other),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let return_type = self.return_type(
            &args
                .arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() != 1 {
            return exec_err!("spark_array_filter requires exactly 1 argument (the array)");
        }

        let array_arg = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(1)?,
        };

        let result = self.filter_array(&array_arg)?;
        Ok(ColumnarValue::Array(result))
    }
}

impl SparkArrayFilter {
    fn filter_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "Argument must be a ListArray".to_string(),
            )
        })?;

        let num_rows = list_array.len();
        let mut new_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
        new_offsets.push(0);

        let values = list_array.values();
        let mut keep_indices: Vec<usize> = Vec::new();

        for row_idx in 0..num_rows {
            if list_array.is_null(row_idx) {
                new_offsets.push(*new_offsets.last().unwrap_or(&0));
                continue;
            }

            let start = list_array.value_offsets()[row_idx] as usize;
            let end = list_array.value_offsets()[row_idx + 1] as usize;

            let mut count = 0i32;
            for elem_idx in start..end {
                if self.element_matches(values.as_ref(), elem_idx)? {
                    keep_indices.push(elem_idx);
                    count += 1;
                }
            }
            new_offsets.push(new_offsets.last().unwrap_or(&0) + count);
        }

        // Build the new values array by taking elements at keep_indices
        let new_values = if keep_indices.is_empty() {
            datafusion::arrow::array::new_empty_array(values.data_type())
        } else {
            datafusion::arrow::compute::take(
                values.as_ref(),
                &datafusion::arrow::array::UInt64Array::from(
                    keep_indices.iter().map(|&i| i as u64).collect::<Vec<_>>(),
                ),
                None,
            )?
        };

        let field = list_array.value_type();
        let new_list = ListArray::try_new(
            Arc::new(Field::new_list_field(field, true)),
            OffsetBuffer::new(new_offsets.into()),
            new_values,
            list_array.nulls().cloned(),
        )?;

        Ok(Arc::new(new_list))
    }

    fn element_matches(&self, values: &dyn Array, idx: usize) -> Result<bool> {
        if values.is_null(idx) {
            return Ok(false); // NULL elements don't match any predicate
        }

        match (values.data_type(), &self.compare_value) {
            (DataType::Int32, ScalarValue::Int32(Some(v))) => {
                let arr = values.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution("Expected Int32Array".to_string())
                })?;
                let elem = arr.value(idx);
                Ok(if self.reversed {
                    self.op.apply_i32(*v, elem)
                } else {
                    self.op.apply_i32(elem, *v)
                })
            }
            (DataType::Int64, ScalarValue::Int64(Some(v))) => {
                let arr = values.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution("Expected Int64Array".to_string())
                })?;
                let elem = arr.value(idx);
                Ok(if self.reversed {
                    self.op.apply_i64(*v, elem)
                } else {
                    self.op.apply_i64(elem, *v)
                })
            }
            // Int64 array with Int32 scalar (common from PySpark)
            (DataType::Int64, ScalarValue::Int32(Some(v))) => {
                let arr = values.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution("Expected Int64Array".to_string())
                })?;
                let elem = arr.value(idx);
                let v64 = *v as i64;
                Ok(if self.reversed {
                    self.op.apply_i64(v64, elem)
                } else {
                    self.op.apply_i64(elem, v64)
                })
            }
            (DataType::Int8, ScalarValue::Int32(Some(v))) => {
                // Int8 arrays compared against Int32 scalar (common from PySpark)
                let arr = values
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int8Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected Int8Array".to_string(),
                        )
                    })?;
                let elem = arr.value(idx) as i32;
                Ok(if self.reversed {
                    self.op.apply_i32(*v, elem)
                } else {
                    self.op.apply_i32(elem, *v)
                })
            }
            (DataType::Float64, ScalarValue::Float64(Some(v))) => {
                let arr = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected Float64Array".to_string(),
                        )
                    })?;
                let elem = arr.value(idx);
                Ok(if self.reversed {
                    self.op.apply_f64(*v, elem)
                } else {
                    self.op.apply_f64(elem, *v)
                })
            }
            (DataType::Utf8, ScalarValue::Utf8(Some(v))) => {
                let arr = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected StringArray".to_string(),
                        )
                    })?;
                let elem = arr.value(idx);
                let matches = match self.op {
                    FilterOp::Equal => elem == v.as_str(),
                    FilterOp::NotEqual => elem != v.as_str(),
                    FilterOp::GreaterThan => elem > v.as_str(),
                    FilterOp::GreaterThanOrEqual => elem >= v.as_str(),
                    FilterOp::LessThan => elem < v.as_str(),
                    FilterOp::LessThanOrEqual => elem <= v.as_str(),
                };
                Ok(if self.reversed { !matches } else { matches })
            }
            (dt, sv) => exec_err!(
                "Unsupported type combination for array filter: {:?} vs {:?}",
                dt,
                sv
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Builder;
    use datafusion::arrow::array::ListBuilder;

    #[test]
    fn test_filter_array_greater_than() -> Result<()> {
        // Create array: [[1, 2, 3, 4, 5], [10, 20, 30]]
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.append(true);
        builder.values().append_value(10);
        builder.values().append_value(20);
        builder.values().append_value(30);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        // Filter: x > 2
        let filter = SparkArrayFilter::greater_than(ScalarValue::Int32(Some(2)));
        let result = filter.filter_array(&array)?;
        let result_list = result.as_any().downcast_ref::<ListArray>().unwrap();

        // Expected: [[3, 4, 5], [10, 20, 30]]
        assert_eq!(result_list.len(), 2);

        // First row: [3, 4, 5]
        let row0 = result_list.value(0);
        let row0_ints = row0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row0_ints.len(), 3);
        assert_eq!(row0_ints.value(0), 3);
        assert_eq!(row0_ints.value(1), 4);
        assert_eq!(row0_ints.value(2), 5);

        // Second row: [10, 20, 30]
        let row1 = result_list.value(1);
        let row1_ints = row1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row1_ints.len(), 3);
        assert_eq!(row1_ints.value(0), 10);
        assert_eq!(row1_ints.value(1), 20);
        assert_eq!(row1_ints.value(2), 30);

        Ok(())
    }

    #[test]
    fn test_filter_array_less_than_or_equal() -> Result<()> {
        // Create array: [[1, 2, 3, 4, 5]]
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        // Filter: x <= 3
        let filter = SparkArrayFilter::less_than_or_equal(ScalarValue::Int32(Some(3)));
        let result = filter.filter_array(&array)?;
        let result_list = result.as_any().downcast_ref::<ListArray>().unwrap();

        // Expected: [[1, 2, 3]]
        assert_eq!(result_list.len(), 1);

        let row0 = result_list.value(0);
        let row0_ints = row0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row0_ints.len(), 3);
        assert_eq!(row0_ints.value(0), 1);
        assert_eq!(row0_ints.value(1), 2);
        assert_eq!(row0_ints.value(2), 3);

        Ok(())
    }
}
