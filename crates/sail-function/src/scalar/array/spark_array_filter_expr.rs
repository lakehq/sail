use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_common::{exec_err, DFSchema, Result};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
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
/// Supports external column references from the outer query context.
/// At runtime:
/// 1. All array elements are flattened into a single column (with optional index column)
/// 2. External columns are broadcast to match the number of elements per row
/// 3. The expression is evaluated vectorized over all elements
/// 4. Boolean results are used to filter and reconstruct the arrays
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
    /// External columns referenced in the lambda expression.
    /// Each entry is (column_name, data_type).
    outer_columns: Vec<(String, DataType)>,
}

impl SparkArrayFilterExpr {
    pub fn new(lambda_expr: Expr, element_type: DataType) -> Self {
        Self::with_column_name(lambda_expr, element_type, LAMBDA_ELEMENT_COLUMN.to_string())
    }

    pub fn with_column_name(
        lambda_expr: Expr,
        element_type: DataType,
        column_name: String,
    ) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            lambda_expr,
            element_type,
            column_name,
            index_column_name: None,
            outer_columns: Vec::new(),
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
            outer_columns: Vec::new(),
        }
    }

    /// Create a filter with external column references.
    /// The outer_columns are passed as additional arguments after the array.
    pub fn with_outer_columns(
        lambda_expr: Expr,
        element_type: DataType,
        column_name: String,
        index_column_name: Option<String>,
        outer_columns: Vec<(String, DataType)>,
    ) -> Self {
        // Signature: array + N outer columns
        let num_args = 1 + outer_columns.len();
        Self {
            signature: Signature::any(num_args, Volatility::Immutable),
            lambda_expr,
            element_type,
            column_name,
            index_column_name,
            outer_columns,
        }
    }

    /// Returns a reference to the lambda expression.
    pub fn lambda_expr(&self) -> &Expr {
        &self.lambda_expr
    }

    /// Returns a reference to the element data type.
    pub fn element_type(&self) -> &DataType {
        &self.element_type
    }

    /// Returns a reference to the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Returns a reference to the optional index column name.
    pub fn index_column_name(&self) -> Option<&str> {
        self.index_column_name.as_deref()
    }

    /// Returns a reference to the outer columns.
    pub fn outer_columns(&self) -> &[(String, DataType)] {
        &self.outer_columns
    }
}

impl Clone for SparkArrayFilterExpr {
    fn clone(&self) -> Self {
        let num_args = 1 + self.outer_columns.len();
        Self {
            signature: Signature::any(num_args, Volatility::Immutable),
            lambda_expr: self.lambda_expr.clone(),
            element_type: self.element_type.clone(),
            column_name: self.column_name.clone(),
            index_column_name: self.index_column_name.clone(),
            outer_columns: self.outer_columns.clone(),
        }
    }
}

impl PartialEq for SparkArrayFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.lambda_expr == other.lambda_expr
            && self.element_type == other.element_type
            && self.column_name == other.column_name
            && self.index_column_name == other.index_column_name
            && self.outer_columns == other.outer_columns
    }
}

impl std::hash::Hash for SparkArrayFilterExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        format!("{:?}", self.lambda_expr).hash(state);
        self.element_type.hash(state);
        self.column_name.hash(state);
        self.index_column_name.hash(state);
        self.outer_columns.hash(state);
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

        let expected_args = 1 + self.outer_columns.len();
        if args.len() != expected_args {
            return exec_err!(
                "spark_array_filter_expr requires {} arguments (array + {} outer columns), got {}",
                expected_args,
                self.outer_columns.len(),
                args.len()
            );
        }

        let array_arg = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(1)?,
        };

        // Extract outer column arrays
        let outer_arrays: Vec<ArrayRef> = args[1..]
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(arr) => Ok(arr.clone()),
                ColumnarValue::Scalar(s) => s.to_array_of_size(array_arg.len()),
            })
            .collect::<Result<Vec<_>>>()?;

        let result = self.filter_array_with_outer(&array_arg, &outer_arrays)?;
        Ok(ColumnarValue::Array(result))
    }
}

impl SparkArrayFilterExpr {
    /// Filter array without external columns (backwards compatible).
    #[cfg(test)]
    fn filter_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        self.filter_array_with_outer(array, &[])
    }

    /// Filter array with optional external column values.
    /// External columns are broadcast so each array element sees its row's value.
    fn filter_array_with_outer(
        &self,
        array: &ArrayRef,
        outer_arrays: &[ArrayRef],
    ) -> Result<ArrayRef> {
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

        // Add outer column fields
        for (col_name, col_type) in &self.outer_columns {
            fields.push(Field::new(col_name, col_type.clone(), true));
        }

        let arrow_schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(arrow_schema.clone())?;

        // Build columns for RecordBatch
        let mut columns: Vec<ArrayRef> = vec![values.clone()];

        // Build row-to-element mapping for broadcasting outer columns
        // For each element, we need to know which row it belongs to
        let mut element_to_row: Vec<usize> = Vec::with_capacity(values.len());
        let mut indices_for_index_col: Vec<i32> = Vec::with_capacity(values.len());

        for row_idx in 0..num_rows {
            if list_array.is_null(row_idx) {
                continue;
            }
            let start = list_array.value_offsets()[row_idx] as usize;
            let end = list_array.value_offsets()[row_idx + 1] as usize;
            for i in 0..(end - start) {
                element_to_row.push(row_idx);
                indices_for_index_col.push(i as i32);
            }
        }

        // If index column is needed, add it
        if self.index_column_name.is_some() {
            columns.push(Arc::new(Int32Array::from(indices_for_index_col)));
        }

        // Broadcast outer columns: for each element, take the value from its row
        for outer_arr in outer_arrays {
            let take_indices = datafusion::arrow::array::UInt64Array::from(
                element_to_row.iter().map(|&i| i as u64).collect::<Vec<_>>(),
            );
            let broadcast_arr =
                datafusion::arrow::compute::take(outer_arr.as_ref(), &take_indices, None)?;
            columns.push(broadcast_arr);
        }

        // Create RecordBatch with all array elements (and optional indices and outer columns)
        let batch = RecordBatch::try_new(arrow_schema, columns)?;

        // Create PhysicalExpr with type coercion and evaluate
        let physical_expr =
            SessionContext::new().create_physical_expr(self.lambda_expr.clone(), &df_schema)?;
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
#[allow(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Int32Array, Int32Builder, ListBuilder};
    use datafusion_expr::{col, lit, Operator};

    use super::*;

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
        let result_list = result
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("downcast failed");

        // Expected: [[3, 4, 5], [10, 20, 30]]
        assert_eq!(result_list.len(), 2);

        // First row: [3, 4, 5]
        let row0 = result_list.value(0);
        let row0_ints = row0
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("downcast failed");
        assert_eq!(row0_ints.len(), 3);
        assert_eq!(row0_ints.value(0), 3);
        assert_eq!(row0_ints.value(1), 4);
        assert_eq!(row0_ints.value(2), 5);

        // Second row: [10, 20, 30]
        let row1 = result_list.value(1);
        let row1_ints = row1
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("downcast failed");
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
        let result_list = result
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("downcast failed");

        // Expected: [[2, 3, 4]]
        assert_eq!(result_list.len(), 1);

        let row0 = result_list.value(0);
        let row0_ints = row0
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("downcast failed");
        assert_eq!(row0_ints.len(), 3);
        assert_eq!(row0_ints.value(0), 2);
        assert_eq!(row0_ints.value(1), 3);
        assert_eq!(row0_ints.value(2), 4);

        Ok(())
    }
}
