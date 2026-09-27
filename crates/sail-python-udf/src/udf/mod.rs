use datafusion_common::Result;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::{AggregateUDF, Expr, WindowFunctionDefinition};

use self::pyspark_batch_collector::PySparkBatchCollectorUDF;
use self::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use self::pyspark_group_map_udf::PySparkGroupMapUDF;
use self::pyspark_udaf::PySparkGroupAggregateUDF;
use self::pyspark_udf::PySparkUDF;
use self::pyspark_unresolved_udf::PySparkUnresolvedUDF;

pub mod pyspark_batch_collector;
pub mod pyspark_cogroup_map_udf;
pub mod pyspark_group_map_udf;
pub mod pyspark_map_iter_udf;
pub mod pyspark_scalar_iter_udf;
pub mod pyspark_udaf;
pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod pyspark_unresolved_udf;

/// Detects Python UDFs in an expression, including nested scalar, aggregate, and
/// window calls. Stream UDFs and resolved UDTFs are plan nodes, not expressions.
pub fn expr_contains_python_udf(body: &Expr) -> Result<bool> {
    let is_python_aggregate = |function: &AggregateUDF| {
        let f = function.inner();
        f.is::<PySparkGroupAggregateUDF>()
            || f.is::<PySparkGroupMapUDF>()
            || f.is::<PySparkBatchCollectorUDF>()
    };
    body.exists(|expression| {
        Ok(match expression {
            Expr::ScalarFunction(function) => {
                let f = function.func.inner();
                f.is::<PySparkUDF>()
                    || f.is::<PySparkUnresolvedUDF>()
                    || f.is::<PySparkCoGroupMapUDF>()
            }
            Expr::AggregateFunction(function) => is_python_aggregate(&function.func),
            Expr::WindowFunction(window) => matches!(
                &window.fun,
                WindowFunctionDefinition::AggregateUDF(udf)
                    if is_python_aggregate(udf)
            ),
            _ => false,
        })
    })
}

#[cfg(test)]
mod tests;
