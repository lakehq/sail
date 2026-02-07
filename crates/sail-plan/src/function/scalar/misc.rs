use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, when, ExprSchemable, Operator, ScalarUDF};
use datafusion_spark::function::bitmap::expr_fn as bitmap_fn;
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_aes::{
    SparkAESDecrypt, SparkAESEncrypt, SparkTryAESDecrypt, SparkTryAESEncrypt,
};
use sail_function::scalar::misc::version::SparkVersion;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn assert_true(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
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

fn current_catalog(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    input.arguments.zero()?;
    let catalog_manager = input
        .function_context
        .session_context
        .extension::<CatalogManager>()?;
    Ok(lit(catalog_manager.default_catalog()?.to_string()))
}

fn current_database(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    input.arguments.zero()?;
    let catalog_manager = input
        .function_context
        .session_context
        .extension::<CatalogManager>()?;
    Ok(lit(quote_namespace_if_needed(
        &catalog_manager.default_database()?,
    )))
}

fn current_user(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    input.arguments.zero()?;
    Ok(lit(input
        .function_context
        .plan_config
        .session_user_id
        .clone()))
}

fn type_of(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let expr = arguments.one()?;
    let data_type = expr.get_type(function_context.schema)?;
    let service = function_context
        .session_context
        .extension::<PlanService>()?;
    let type_of = service
        .plan_formatter()
        .data_type_to_simple_string(&data_type)?;
    Ok(lit(type_of))
}

fn bitmap_bit_position(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let value = arguments.one()?;
    let num_bits = 8 * 4 * 1024;
    Ok(when(
        value.clone().gt(lit(0)),
        (value.clone() - lit(1)) % lit(num_bits),
    )
    .when(lit(true), (-value) % lit(num_bits))
    .end()?)
}

fn bitmap_bucket_number(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let value = arguments.one()?;
    let num_bits = 8 * 4 * 1024;
    Ok(when(
        value.clone().gt(lit(0)),
        lit(1) + (value.clone() - lit(1)) / lit(num_bits),
    )
    .when(lit(true), value / lit(num_bits))
    .end()?)
}

pub(super) fn list_built_in_misc_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aes_decrypt", F::udf(SparkAESDecrypt::new())),
        ("aes_encrypt", F::udf(SparkAESEncrypt::new())),
        ("assert_true", F::custom(assert_true)),
        ("bitmap_bit_position", F::custom(bitmap_bit_position)),
        ("bitmap_bucket_number", F::custom(bitmap_bucket_number)),
        ("bitmap_count", F::unary(bitmap_fn::bitmap_count)),
        ("current_catalog", F::custom(current_catalog)),
        ("current_database", F::custom(current_database)),
        ("current_schema", F::custom(current_database)),
        ("current_user", F::custom(current_user)),
        ("from_avro", F::unknown("from_avro")),
        ("from_protobuf", F::unknown("from_protobuf")),
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
        ("schema_of_avro", F::unknown("schema_of_avro")),
        ("session_user", F::custom(current_user)),
        ("spark_partition_id", F::unknown("spark_partition_id")),
        ("to_avro", F::unknown("to_avro")),
        ("to_protobuf", F::unknown("to_protobuf")),
        ("try_aes_encrypt", F::udf(SparkTryAESEncrypt::new())),
        ("try_aes_decrypt", F::udf(SparkTryAESDecrypt::new())),
        ("try_reflect", F::unknown("try_reflect")),
        ("typeof", F::custom(type_of)),
        ("user", F::custom(current_user)),
        ("uuid", F::nullary(expr_fn::uuid)),
        ("version", F::udf(SparkVersion::new())),
    ]
}
