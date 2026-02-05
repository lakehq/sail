use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::{exec_err, DFSchema, Result};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// Default column name for the lambda variable (used for display/testing).
pub const LAMBDA_ELEMENT_COLUMN: &str = "__lambda_element__";

/// SparkArrayFilterExpr filters array elements using arbitrary DataFusion expressions.
///
/// Unlike SparkArrayFilter which only supports simple comparisons (x > literal),
/// this implementation evaluates full DataFusion expressions for each array element.
///
/// The lambda variable in the expression is represented as a column reference.
/// Optionally supports a second variable for the element index within each array.
/// At runtime:
/// 1. All array elements are flattened into a single column (with optional index column)
/// 2. The expression is evaluated vectorized over all elements
/// 3. Boolean results are used to filter and reconstruct the arrays
#[derive(Debug)]
pub struct SparkArrayFilterExpr {
    signature: Signature,
    /// The lambda expression to evaluate for each element.
    lambda_expr: Expr,
    /// The data type of array elements (needed to create the schema for evaluation).
    element_type: DataType,
    /// The column name used in the expression for the lambda element variable.
    column_name: String,
    /// Optional column name for the index variable (for two-argument lambdas).
    index_column_name: Option<String>,
}

impl SparkArrayFilterExpr {
    pub fn new(lambda_expr: Expr, element_type: DataType) -> Self {
        Self::with_column_name(lambda_expr, element_type, LAMBDA_ELEMENT_COLUMN.to_string())
    }

    pub fn with_column_name(lambda_expr: Expr, element_type: DataType, column_name: String) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            lambda_expr,
            element_type,
            column_name,
            index_column_name: None,
        }
    }

    pub fn with_index_column(
        lambda_expr: Expr,
        element_type: DataType,
        column_name: String,
        index_column_name: String,
    ) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            lambda_expr,
            element_type,
            column_name,
            index_column_name: Some(index_column_name),
        }
    }
}

impl Clone for SparkArrayFilterExpr {
    fn clone(&self) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            lambda_expr: self.lambda_expr.clone(),
            element_type: self.element_type.clone(),
            column_name: self.column_name.clone(),
            index_column_name: self.index_column_name.clone(),
        }
    }
}

impl PartialEq for SparkArrayFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.lambda_expr == other.lambda_expr
            && self.element_type == other.element_type
            && self.column_name == other.column_name
            && self.index_column_name == other.index_column_name
    }
}

impl std::hash::Hash for SparkArrayFilterExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        format!("{:?}", self.lambda_expr).hash(state);
        self.element_type.hash(state);
        self.column_name.hash(state);
        self.index_column_name.hash(state);
    }
}

impl Eq for SparkArrayFilterExpr {}

impl ScalarUDFImpl for SparkArrayFilterExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_array_filter_expr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(field) => Ok(DataType::List(field.clone())),
            DataType::LargeList(field) => Ok(DataType::LargeList(field.clone())),
            other => exec_err!("spark_array_filter_expr expects List type, got {:?}", other),
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
            return exec_err!("spark_array_filter_expr requires exactly 1 argument (the array)");
        }

        let array_arg = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(1)?,
        };

        let result = self.filter_array(&array_arg)?;
        Ok(ColumnarValue::Array(result))
    }
}

impl SparkArrayFilterExpr {
    fn filter_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "Argument must be a ListArray".to_string(),
            )
        })?;

        let num_rows = list_array.len();
        let values = list_array.values();

        // If empty, return as-is
        if values.is_empty() {
            return Ok(array.clone());
        }

        // Build schema fields - always include element column
        let mut fields = vec![Field::new(
            &self.column_name,
            self.element_type.clone(),
            true,
        )];

        // Optionally add index column (Int32 to match common array element types)
        if let Some(ref index_col) = self.index_column_name {
            fields.push(Field::new(index_col, DataType::Int32, false));
        }

        let arrow_schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(arrow_schema.clone())?;

        // Build columns for RecordBatch
        let mut columns: Vec<ArrayRef> = vec![values.clone()];

        // If index column is needed, generate indices for each element within its array
        if self.index_column_name.is_some() {
            let mut indices: Vec<i32> = Vec::with_capacity(values.len());
            for row_idx in 0..num_rows {
                if list_array.is_null(row_idx) {
                    continue;
                }
                let start = list_array.value_offsets()[row_idx] as usize;
                let end = list_array.value_offsets()[row_idx + 1] as usize;
                for i in 0..(end - start) {
                    indices.push(i as i32);
                }
            }
            columns.push(Arc::new(Int32Array::from(indices)));
        }

        // Create RecordBatch with all array elements (and optional indices)
        let batch = RecordBatch::try_new(arrow_schema, columns)?;

        // Create PhysicalExpr and evaluate
        let props = ExecutionProps::new();
        let physical_expr = create_physical_expr(&self.lambda_expr, &df_schema, &props)?;
        let result = physical_expr.evaluate(&batch)?;

        // Extract boolean mask
        let mask = match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "Lambda expression must return boolean".to_string(),
                    )
                })?
                .clone(),
            ColumnarValue::Scalar(s) => {
                let bool_val = matches!(s, datafusion_common::ScalarValue::Boolean(Some(true)));
                BooleanArray::from(vec![bool_val; values.len()])
            }
        };

        // Build filtered arrays for each row
        let mut new_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
        new_offsets.push(0);
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
                // Check if mask value is true (and not null)
                if mask.is_valid(elem_idx) && mask.value(elem_idx) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int32Builder, ListBuilder};
    use datafusion_expr::{col, lit, Operator};

    #[test]
    fn test_filter_with_gt_expr() -> Result<()> {
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

        // Filter: __lambda_element__ > 2
        let lambda_expr = Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
            left: Box::new(col(LAMBDA_ELEMENT_COLUMN)),
            op: Operator::Gt,
            right: Box::new(lit(2i32)),
        });

        let filter = SparkArrayFilterExpr::new(lambda_expr, DataType::Int32);
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

        Ok(())
    }

    #[test]
    fn test_filter_with_and_expr() -> Result<()> {
        // Create array: [[1, 2, 3, 4, 5]]
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        // Filter: __lambda_element__ > 1 AND __lambda_element__ < 5
        let lambda_expr = Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
                left: Box::new(col(LAMBDA_ELEMENT_COLUMN)),
                op: Operator::Gt,
                right: Box::new(lit(1i32)),
            })),
            op: Operator::And,
            right: Box::new(Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
                left: Box::new(col(LAMBDA_ELEMENT_COLUMN)),
                op: Operator::Lt,
                right: Box::new(lit(5i32)),
            })),
        });

        let filter = SparkArrayFilterExpr::new(lambda_expr, DataType::Int32);
        let result = filter.filter_array(&array)?;
        let result_list = result.as_any().downcast_ref::<ListArray>().unwrap();

        // Expected: [[2, 3, 4]]
        assert_eq!(result_list.len(), 1);

        let row0 = result_list.value(0);
        let row0_ints = row0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row0_ints.len(), 3);
        assert_eq!(row0_ints.value(0), 2);
        assert_eq!(row0_ints.value(1), 3);
        assert_eq!(row0_ints.value(2), 4);

        Ok(())
    }
}
