use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, Operator, ScalarUDF};

use crate::catalog::CatalogManager;
use crate::error::{PlanError, PlanResult};
use crate::extension::function::raise_error::RaiseError;
use crate::function::common::{Function, FunctionContext};
use crate::utils::ItemTaker;

fn assert_true(
    args: Vec<expr::Expr>,
    _function_context: &FunctionContext,
) -> PlanResult<expr::Expr> {
    let (err_msg, col) = if args.len() == 1 {
        let col = args.one()?;
        (
            // Need to do this order to avoid the "value used after being moved" error.
            lit(ScalarValue::Utf8(Some(format!("'{}' is not true!", &col)))),
            col,
        )
    } else if args.len() == 2 {
        let (col, err_msg) = args.two()?;
        (err_msg, col)
    } else {
        return Err(PlanError::invalid(format!(
            "assert_true expects at most two arguments, got {}",
            args.len()
        )));
    };

    // TODO: Add PySpark tests once we have the pytest setup for the library.
    //  Ref link: https://github.com/lakehq/sail/pull/122#discussion_r1716235731
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(expr::Expr::Not(Box::new(col.clone()))),
            Box::new(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: Arc::new(ScalarUDF::from(RaiseError::new())),
                args: vec![err_msg],
            })),
        )],
        else_expr: Some(Box::new(lit(ScalarValue::Null))),
    }))
}

fn current_catalog(
    args: Vec<expr::Expr>,
    function_context: &FunctionContext,
) -> PlanResult<expr::Expr> {
    args.zero()?;
    let catalog_manager = CatalogManager::new(
        function_context.session_context(),
        function_context.plan_config().clone(),
    );
    Ok(lit(catalog_manager.default_catalog()?))
}

fn current_database(
    args: Vec<expr::Expr>,
    function_context: &FunctionContext,
) -> PlanResult<expr::Expr> {
    args.zero()?;
    let catalog_manager = CatalogManager::new(
        function_context.session_context(),
        function_context.plan_config().clone(),
    );
    Ok(lit(catalog_manager.default_database()?))
}

fn current_user(
    args: Vec<expr::Expr>,
    function_context: &FunctionContext,
) -> PlanResult<expr::Expr> {
    args.zero()?;
    Ok(lit(function_context.plan_config().session_user_id.clone()))
}

pub(super) fn list_built_in_misc_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("aes_decrypt", F::unknown("aes_decrypt")),
        ("aes_encrypt", F::unknown("aes_encrypt")),
        ("assert_true", F::custom(assert_true)),
        ("bitmap_bit_position", F::unknown("bitmap_bit_position")),
        ("bitmap_bucket_number", F::unknown("bitmap_bucket_number")),
        ("bitmap_count", F::unknown("bitmap_count")),
        ("current_catalog", F::custom(current_catalog)),
        ("current_database", F::custom(current_database)),
        ("current_schema", F::custom(current_database)),
        ("current_user", F::custom(current_user)),
        ("equal_null", F::binary_op(Operator::IsNotDistinctFrom)),
        ("hll_sketch_estimate", F::unknown("hll_sketch_estimate")),
        ("hll_union", F::unknown("hll_union")),
        (
            "input_file_block_length",
            F::unknown("input_file_block_length"),
        ),
        (
            "input_file_block_start",
            F::unknown("input_file_block_start"),
        ),
        ("input_file_name", F::unknown("input_file_name")),
        ("java_method", F::unknown("java_method")),
        (
            "monotonically_increasing_id",
            F::unknown("monotonically_increasing_id"),
        ),
        ("raise_error", F::udf(RaiseError::new())),
        ("reflect", F::unknown("reflect")),
        ("spark_partition_id", F::unknown("spark_partition_id")),
        ("try_aes_decrypt", F::unknown("try_aes_decrypt")),
        ("typeof", F::unknown("typeof")),
        ("user", F::unknown("user")),
        ("uuid", F::nullary(expr_fn::uuid)),
        ("version", F::unknown("version")),
    ]
}
