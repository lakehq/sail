use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, ExprSchemable, Operator, ScalarUDF};

use crate::catalog::CatalogManager;
use crate::error::{PlanError, PlanResult};
use crate::extension::function::raise_error::RaiseError;
use crate::extension::function::spark_aes::{
    SparkAESDecrypt, SparkAESEncrypt, SparkTryAESDecrypt, SparkTryAESEncrypt,
};
use crate::function::common::{Function, FunctionInput};
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

fn assert_true(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (err_msg, col) = if arguments.len() == 1 {
        let col = arguments.one()?;
        (
            // Need to do this order to avoid the "value used after being moved" error.
            lit(ScalarValue::Utf8(Some(format!("'{}' is not true!", &col)))),
            col,
        )
    } else if arguments.len() == 2 {
        let (col, err_msg) = arguments.two()?;
        (err_msg, col)
    } else {
        return Err(PlanError::invalid(format!(
            "assert_true expects at most two arguments, got {}",
            arguments.len()
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

fn current_catalog(input: FunctionInput) -> PlanResult<expr::Expr> {
    input.arguments.zero()?;
    let catalog_manager = CatalogManager::new(input.session_context, input.plan_config.clone());
    Ok(lit(catalog_manager.default_catalog()?))
}

fn current_database(input: FunctionInput) -> PlanResult<expr::Expr> {
    input.arguments.zero()?;
    let catalog_manager = CatalogManager::new(input.session_context, input.plan_config.clone());
    Ok(lit(catalog_manager.default_database()?))
}

fn current_user(input: FunctionInput) -> PlanResult<expr::Expr> {
    input.arguments.zero()?;
    Ok(lit(input.plan_config.session_user_id.clone()))
}

fn type_of(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput {
        arguments,
        plan_config,
        schema,
        ..
    } = input;
    let expr = arguments.one()?;
    let data_type = expr.get_type(schema)?;
    let type_of = plan_config
        .plan_formatter
        .data_type_to_simple_string(&PlanResolver::unresolve_data_type(&data_type)?)?;
    Ok(lit(type_of))
}

pub(super) fn list_built_in_misc_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("aes_decrypt", F::udf(SparkAESDecrypt::new())),
        ("aes_encrypt", F::udf(SparkAESEncrypt::new())),
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
        ("try_aes_encrypt", F::udf(SparkTryAESEncrypt::new())),
        ("try_aes_decrypt", F::udf(SparkTryAESDecrypt::new())),
        ("typeof", F::custom(type_of)),
        ("user", F::unknown("user")),
        ("uuid", F::nullary(expr_fn::uuid)),
        ("version", F::unknown("version")),
    ]
}
