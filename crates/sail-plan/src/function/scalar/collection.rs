use arrow::datatypes::DataType;
use datafusion_expr::{cast, expr, ExprSchemable};
use datafusion_functions_nested::expr_fn;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::collection::spark_concat::SparkConcat;
use crate::extension::function::collection::spark_reverse::SparkReverse;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::utils::ItemTaker;

fn size(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;

    match value.get_type(input.function_context.schema)? {
        arrow::datatypes::DataType::List(_)
        | arrow::datatypes::DataType::ListView(_)
        | arrow::datatypes::DataType::FixedSizeList(..)
        | arrow::datatypes::DataType::LargeList(_)
        | arrow::datatypes::DataType::LargeListView(_) => {
            Ok(cast(expr_fn::array_length(value), DataType::Int32))
        }
        arrow::datatypes::DataType::Map(..) => {
            Ok(cast(expr_fn::cardinality(value), DataType::Int32))
        }
        wrong_type => Err(PlanError::InvalidArgument(format!(
            "type should be List or Map, got {wrong_type:?}"
        ))),
    }
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        // TODO: coalesce(result, -1)
        // if spark.sql.ansi.enabled is false and spark.sql.legacy.sizeOfNull is true
        // https://spark.apache.org/docs/latest/api/sql/index.html#cardinality
        ("cardinality", F::custom(size)),
        ("deep_size", F::unary(expr_fn::cardinality)),
        ("size", F::custom(size)),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
    ]
}
