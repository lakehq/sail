use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::Result;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion::scalar::ScalarValue;
use datafusion_physical_expr::expressions::Literal;

pub fn spark_ntile_udwf() -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(SparkNtile::new()))
}

/// Spark-compatible NTILE window function.
///
/// Distributes rows into `n` buckets, with extra rows going to the first buckets.
/// For example, 10 rows with ntile(4) gives buckets of sizes (3, 3, 2, 2),
/// not (3, 2, 3, 2) like the default DataFusion implementation.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNtile {
    signature: Signature,
}

impl SparkNtile {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::UInt64,
                    DataType::UInt32,
                    DataType::UInt16,
                    DataType::UInt8,
                    DataType::Int64,
                    DataType::Int32,
                    DataType::Int16,
                    DataType::Int8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for SparkNtile {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for SparkNtile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ntile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let scalar_n = get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 0)?
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "NTILE requires a positive integer".to_string(),
                )
            })?;

        if scalar_n.is_null() {
            return datafusion::common::exec_err!(
                "NTILE requires a positive integer, but finds NULL"
            );
        }

        let n = get_integer_value(&scalar_n)?;
        if n <= 0 {
            return datafusion::common::exec_err!("NTILE requires a positive integer");
        }

        Ok(Box::new(SparkNtileEvaluator { n: n as u64 }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::UInt64, false).into())
    }
}

#[derive(Debug)]
struct SparkNtileEvaluator {
    n: u64,
}

impl PartitionEvaluator for SparkNtileEvaluator {
    fn evaluate_all(&mut self, _values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let num_rows = num_rows as u64;
        let n = u64::min(self.n, num_rows);

        if n == 0 {
            return datafusion::common::exec_err!("NTILE requires a positive integer");
        }

        // Spark distributes extra rows to the first buckets
        // For 10 rows with n=4: base=2, extra=2
        // Bucket sizes: (3, 3, 2, 2) - first 2 buckets get the extra rows
        let base = num_rows / n;
        let extra = num_rows % n;

        let mut vec: Vec<u64> = Vec::with_capacity(num_rows as usize);
        for i in 0..num_rows {
            // First `extra` buckets have `base + 1` rows each
            // Remaining buckets have `base` rows each
            let threshold = extra * (base + 1);
            let bucket = if i < threshold {
                i / (base + 1)
            } else {
                extra + (i - threshold) / base
            };
            vec.push(bucket + 1);
        }

        Ok(Arc::new(UInt64Array::from(vec)))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

/// Helper to extract scalar value from input expressions
fn get_scalar_value_from_args(
    input_exprs: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
    index: usize,
) -> Result<Option<ScalarValue>> {
    if let Some(expr) = input_exprs.get(index) {
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            return Ok(Some(literal.value().clone()));
        }
    }
    Ok(None)
}

/// Helper to extract integer value from ScalarValue
fn get_integer_value(scalar: &ScalarValue) -> Result<i64> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(*v as i64),
        ScalarValue::Int16(Some(v)) => Ok(*v as i64),
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt64(Some(v)) => {
            if *v > i64::MAX as u64 {
                datafusion::common::exec_err!("NTILE argument too large")
            } else {
                Ok(*v as i64)
            }
        }
        ScalarValue::Null => datafusion::common::exec_err!("NTILE requires a non-null integer"),
        _ => datafusion::common::exec_err!("NTILE requires an integer argument"),
    }
}
