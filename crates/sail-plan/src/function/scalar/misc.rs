use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions::{core, string};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::hll_sketch::{HllSketchEstimateFunction, HllUnionFunction};
use sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_aes::{
    SparkAESDecrypt, SparkAESEncrypt, SparkTryAESDecrypt, SparkTryAESEncrypt,
};
use sail_function::scalar::misc::spark_partition_id::SparkPartitionId;
use sail_function::scalar::misc::theta_sketch::{
    ThetaDifferenceFunction, ThetaIntersectionFunction, ThetaSketchEstimateFunction,
    ThetaUnionFunction,
};
use sail_function::scalar::misc::version::SparkVersion;
use sail_function::sketch::DEFAULT_THETA_LG_NOM_ENTRIES;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn assert_true(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let names = input.argument_display_names;

    let (value, error) = match values.len() {
        1 => {
            let message = format!(
                "'{}' is not true!",
                names.first().map(String::as_str).unwrap_or("")
            );
            (
                values.one()?,
                context.literal(ScalarValue::Utf8(Some(message))),
            )
        }
        2 => values.two()?,
        n => {
            return Err(PlanError::invalid(format!(
                "assert_true expects at most two arguments, got {n}"
            )));
        }
    };
    let condition = context.not(value)?;
    let error = context.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![error])?;
    let null = context.literal(ScalarValue::Null);
    context.case(vec![(condition, error)], Some(null))
}

fn current_catalog(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    values.zero()?;
    let value = context
        .session
        .extension::<CatalogManager>()?
        .default_catalog()?
        .to_string();
    Ok(context.literal(ScalarValue::Utf8(Some(value))))
}

fn current_database(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    values.zero()?;
    let value = quote_namespace_if_needed(
        &context
            .session
            .extension::<CatalogManager>()?
            .default_database()?,
    );
    Ok(context.literal(ScalarValue::Utf8(Some(value))))
}

fn current_user(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    values.zero()?;
    Ok(context.literal(ScalarValue::Utf8(Some(
        context.config.session_user_id.clone(),
    ))))
}

fn input_file_name(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    values.zero()?;
    let value = context.scalar_udf(core::input_file_name(), vec![])?;
    let empty = context.literal(ScalarValue::Utf8(Some("".into())));
    context.scalar_udf(core::coalesce(), vec![value, empty])
}

fn type_of(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let data_type = &value.result_type.resolved("typeof argument")?.data_type;
    let name = context
        .session
        .extension::<PlanService>()?
        .plan_formatter()
        .data_type_to_simple_string(data_type)?;
    Ok(context.literal(ScalarValue::Utf8(Some(name))))
}

fn bitmap_bit_position(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let copy = context.copy_expression(&value)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let positive = context.binary(copy, Operator::Gt, zero)?;
    let copy = context.copy_expression(&value)?;
    let one = context.literal(ScalarValue::Int32(Some(1)));
    let offset = context.binary(copy, Operator::Minus, one)?;
    let bits = context.literal(ScalarValue::Int32(Some(32768)));
    let operator = if name == "bitmap_bit_position" {
        Operator::Modulo
    } else {
        Operator::Divide
    };
    let mut a = context.binary(offset, operator, bits)?;
    let b = if name == "bitmap_bit_position" {
        raw_negative(context, value)
    } else {
        let one = context.literal(ScalarValue::Int32(Some(1)));
        a = context.binary(one, Operator::Plus, a)?;
        value
    };
    let bits = context.literal(ScalarValue::Int32(Some(32768)));
    let b = context.binary(b, operator, bits)?;
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    context.case(vec![(positive, a), (yes, b)], None)
}

fn theta_union(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let mut values = values;
    match values.len() {
        2 => values.push(context.literal(if name == "hll_union" {
            ScalarValue::Boolean(Some(false))
        } else {
            ScalarValue::Int32(Some(DEFAULT_THETA_LG_NOM_ENTRIES))
        })),
        3 => {
            let value = values
                .pop()
                .ok_or_else(|| PlanError::internal("sketch option missing"))?;
            values.push(context.cast(
                value,
                if name == "hll_union" {
                    DataType::Boolean
                } else {
                    DataType::Int32
                },
            )?);
        }
        n => {
            return Err(PlanError::invalid(format!(
                "{name} requires 2 or 3 arguments, got {n}"
            )));
        }
    }
    let function = if name == "hll_union" {
        ScalarUDF::from(HllUnionFunction::new())
    } else {
        ScalarUDF::from(ThetaUnionFunction::new())
    };
    context.scalar_udf(Arc::new(function), values)
}

pub(crate) fn raw_negative(context: &mut FunctionContextInput<'_>, value: Expr) -> Expr {
    Expr {
        id: context.ids.expression(),
        result_type: value.result_type.clone(),
        kind: Deferred::Resolved(ExprKind::Negative(Box::new(value))),
    }
}

pub(super) fn list_built_in_misc_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aes_decrypt", F::udf(SparkAESDecrypt::new())),
        ("aes_encrypt", F::udf(SparkAESEncrypt::new())),
        ("assert_true", F::custom(assert_true)),
        (
            "bitmap_bit_position",
            F::custom(|input| bitmap_bit_position(input, "bitmap_bit_position")),
        ),
        (
            "bitmap_bucket_number",
            F::custom(|input| bitmap_bit_position(input, "bitmap_bucket_number")),
        ),
        (
            "bitmap_count",
            F::scalar_udf(datafusion_spark::function::bitmap::bitmap_count),
        ),
        ("current_catalog", F::custom(current_catalog)),
        ("current_database", F::custom(current_database)),
        ("current_path", F::unknown("current_path")),
        ("current_schema", F::custom(current_database)),
        ("current_user", F::custom(current_user)),
        ("from_avro", F::unknown("from_avro")),
        ("from_protobuf", F::unknown("from_protobuf")),
        ("equal_null", F::binary_op(Operator::IsNotDistinctFrom)),
        (
            "hll_sketch_estimate",
            F::udf(HllSketchEstimateFunction::new()),
        ),
        (
            "hll_union",
            F::custom(|input| theta_union(input, "hll_union")),
        ),
        ("theta_difference", F::udf(ThetaDifferenceFunction::new())),
        (
            "theta_intersection",
            F::udf(ThetaIntersectionFunction::new()),
        ),
        (
            "theta_sketch_estimate",
            F::udf(ThetaSketchEstimateFunction::new()),
        ),
        (
            "theta_union",
            F::custom(|input| theta_union(input, "theta_union")),
        ),
        (
            "tuple_difference_double",
            F::unknown("tuple_difference_double"),
        ),
        (
            "tuple_difference_integer",
            F::unknown("tuple_difference_integer"),
        ),
        (
            "tuple_difference_theta_double",
            F::unknown("tuple_difference_theta_double"),
        ),
        (
            "tuple_difference_theta_integer",
            F::unknown("tuple_difference_theta_integer"),
        ),
        (
            "tuple_intersection_double",
            F::unknown("tuple_intersection_double"),
        ),
        (
            "tuple_intersection_integer",
            F::unknown("tuple_intersection_integer"),
        ),
        (
            "tuple_intersection_theta_double",
            F::unknown("tuple_intersection_theta_double"),
        ),
        (
            "tuple_intersection_theta_integer",
            F::unknown("tuple_intersection_theta_integer"),
        ),
        (
            "tuple_sketch_estimate_double",
            F::unknown("tuple_sketch_estimate_double"),
        ),
        (
            "tuple_sketch_estimate_integer",
            F::unknown("tuple_sketch_estimate_integer"),
        ),
        (
            "tuple_sketch_summary_double",
            F::unknown("tuple_sketch_summary_double"),
        ),
        (
            "tuple_sketch_summary_integer",
            F::unknown("tuple_sketch_summary_integer"),
        ),
        (
            "tuple_sketch_theta_double",
            F::unknown("tuple_sketch_theta_double"),
        ),
        (
            "tuple_sketch_theta_integer",
            F::unknown("tuple_sketch_theta_integer"),
        ),
        ("tuple_union_double", F::unknown("tuple_union_double")),
        ("tuple_union_integer", F::unknown("tuple_union_integer")),
        (
            "tuple_union_theta_double",
            F::unknown("tuple_union_theta_double"),
        ),
        (
            "tuple_union_theta_integer",
            F::unknown("tuple_union_theta_integer"),
        ),
        (
            "input_file_block_length",
            F::udf(InputFileBlockLengthFunc::new()),
        ),
        (
            "input_file_block_start",
            F::udf(InputFileBlockStartFunc::new()),
        ),
        ("input_file_name", F::custom(input_file_name)),
        ("java_method", F::unknown("java_method")),
        (
            "monotonically_increasing_id",
            F::udf(SparkMonotonicallyIncreasingId::new()),
        ),
        ("raise_error", F::udf(RaiseError::new())),
        ("reflect", F::unknown("reflect")),
        ("schema_of_avro", F::unknown("schema_of_avro")),
        ("session_user", F::custom(current_user)),
        ("spark_partition_id", F::udf(SparkPartitionId::new())),
        ("to_avro", F::unknown("to_avro")),
        ("to_protobuf", F::unknown("to_protobuf")),
        ("try_aes_encrypt", F::udf(SparkTryAESEncrypt::new())),
        ("try_aes_decrypt", F::udf(SparkTryAESDecrypt::new())),
        ("try_reflect", F::unknown("try_reflect")),
        ("typeof", F::custom(type_of)),
        ("user", F::custom(current_user)),
        ("uuid", F::scalar_udf(string::uuid)),
        ("version", F::udf(SparkVersion::new())),
    ]
}
