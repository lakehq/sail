use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::hll_sketch::HllUnionFunction;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::theta_sketch::ThetaUnionFunction;
use sail_function::sketch::DEFAULT_THETA_LG_NOM_ENTRIES;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_assert_true(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let names = input.argument_display_names;

    Ok({
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
        let error =
            context.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![error])?;
        let null = context.literal(ScalarValue::Null);
        context.case(vec![(condition, error)], Some(null))?
    })
}

fn resolve_current_catalog(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        values.zero()?;
        let value = context
            .session
            .extension::<CatalogManager>()?
            .default_catalog()?
            .to_string();
        context.literal(ScalarValue::Utf8(Some(value)))
    })
}

fn resolve_current_database(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        values.zero()?;
        let value = quote_namespace_if_needed(
            &context
                .session
                .extension::<CatalogManager>()?
                .default_database()?,
        );
        context.literal(ScalarValue::Utf8(Some(value)))
    })
}

fn resolve_current_user(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        values.zero()?;
        context.literal(ScalarValue::Utf8(Some(
            context.config.session_user_id.clone(),
        )))
    })
}

fn resolve_input_file_name(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        values.zero()?;
        let value = context.scalar("input_file_name", vec![])?;
        let empty = context.literal(ScalarValue::Utf8(Some("".into())));
        context.scalar("coalesce", vec![value, empty])?
    })
}

fn resolve_typeof(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = values.one()?;
        let data_type = &value.result_type.resolved("typeof argument")?.data_type;
        let name = context
            .session
            .extension::<PlanService>()?
            .plan_formatter()
            .data_type_to_simple_string(data_type)?;
        context.literal(ScalarValue::Utf8(Some(name)))
    })
}

fn resolve_bitmap_bit_position(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
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
        context.case(vec![(positive, a), (yes, b)], None)?
    })
}

fn resolve_theta_union(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
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
        context.scalar_udf(Arc::new(function), values)?
    })
}

fn resolve_sha2(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (value, bits) = values.two()?;
        let function = match bits.kind.resolved("sha2 bit length")? {
            ExprKind::Literal(ScalarValue::Int32(Some(0 | 256))) => "sha256",
            ExprKind::Literal(ScalarValue::Int32(Some(224))) => "sha224",
            ExprKind::Literal(ScalarValue::Int32(Some(384))) => "sha384",
            ExprKind::Literal(ScalarValue::Int32(Some(512))) => "sha512",
            _ => {
                return Err(PlanError::invalid(
                    "The second argument of sha2 must be a literal integer.",
                ));
            }
        };
        let value = context.scalar(function, vec![value])?;
        let value = context.scalar_udf(datafusion_spark::function::math::hex(), vec![value])?;
        context.scalar("lower", vec![value])?
    })
}

fn resolve_shiftrightunsigned(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (value, shift) = values.two()?;
        let target = value
            .result_type
            .resolved("unsigned shift input")?
            .data_type
            .clone();
        let (unsigned, max) = match target {
            DataType::Int32 => (DataType::UInt32, u64::from(u32::MAX)),
            DataType::Int64 => (DataType::UInt64, u64::MAX),
            _ => {
                return Err(PlanError::invalid(format!(
                    "`shiftrightunsigned`: unsupported input type {target:?}"
                )));
            }
        };
        let copy = context.copy_expression(&value)?;
        let zero = context.literal(ScalarValue::Int32(Some(0)));
        let negative = context.binary(copy, Operator::Lt, zero)?;
        let copy = context.copy_expression(&value)?;
        let absolute = context.scalar("abs", vec![copy])?;
        let one = context.literal(ScalarValue::Int32(Some(1)));
        let distance = context.binary(absolute, Operator::Minus, one)?;
        let maximum = context.literal(ScalarValue::UInt64(Some(max)));
        let wrapped = context.binary(maximum, Operator::Minus, distance)?;
        let value = context.case(vec![(negative, wrapped)], Some(value))?;
        let value = context.cast(value, unsigned)?;
        let value = context.binary(value, Operator::BitwiseShiftRight, shift)?;
        context.cast(value, target)?
    })
}

fn resolve_bitwise_not(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = values.one()?;
        let negative = raw_negative(context, value);
        let one = context.literal(ScalarValue::Int32(Some(1)));
        context.binary(negative, Operator::Minus, one)?
    })
}

pub(crate) fn raw_negative(context: &mut FunctionContextInput<'_>, value: Expr) -> Expr {
    Expr {
        id: context.ids.expression(),
        result_type: value.result_type.clone(),
        kind: Deferred::Resolved(ExprKind::Negative(Box::new(value))),
    }
}

pub(super) fn list_built_in_misc_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
("aes_decrypt", F::udf(sail_function::scalar::misc::spark_aes::SparkAESDecrypt::new())),
("aes_encrypt", F::udf(sail_function::scalar::misc::spark_aes::SparkAESEncrypt::new())),
("bitmap_count", F::scalar_udf(datafusion_spark::function::bitmap::bitmap_count)),
("current_path", F::kernel("current_path")),
("from_avro", F::kernel("from_avro")),
("from_protobuf", F::kernel("from_protobuf")),
("hll_sketch_estimate", F::udf(
            sail_function::scalar::misc::hll_sketch::HllSketchEstimateFunction::new(),
        )),
("input_file_block_length", F::udf(sail_common_datafusion::input_file::InputFileBlockLengthFunc::new())),
("input_file_block_start", F::udf(sail_common_datafusion::input_file::InputFileBlockStartFunc::new())),
("java_method", F::kernel("java_method")),
("monotonically_increasing_id", F::udf(sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId::new())),
("raise_error", F::udf(sail_function::scalar::misc::raise_error::RaiseError::new())),
("reflect", F::kernel("reflect")),
("schema_of_avro", F::kernel("schema_of_avro")),
("spark_partition_id", F::udf(sail_function::scalar::misc::spark_partition_id::SparkPartitionId::new())),
("theta_difference", F::udf(
            sail_function::scalar::misc::theta_sketch::ThetaDifferenceFunction::new(),
        )),
("theta_intersection", F::udf(
            sail_function::scalar::misc::theta_sketch::ThetaIntersectionFunction::new(),
        )),
("theta_sketch_estimate", F::udf(
            sail_function::scalar::misc::theta_sketch::ThetaSketchEstimateFunction::new(),
        )),
("to_avro", F::kernel("to_avro")),
("to_protobuf", F::kernel("to_protobuf")),
("try_aes_decrypt", F::udf(sail_function::scalar::misc::spark_aes::SparkTryAESDecrypt::new())),
("try_aes_encrypt", F::udf(sail_function::scalar::misc::spark_aes::SparkTryAESEncrypt::new())),
("try_reflect", F::kernel("try_reflect")),
("tuple_difference_double", F::kernel("tuple_difference_double")),
("tuple_difference_integer", F::kernel("tuple_difference_integer")),
("tuple_difference_theta_double", F::kernel("tuple_difference_theta_double")),
("tuple_difference_theta_integer", F::kernel("tuple_difference_theta_integer")),
("tuple_intersection_double", F::kernel("tuple_intersection_double")),
("tuple_intersection_integer", F::kernel("tuple_intersection_integer")),
("tuple_intersection_theta_double", F::kernel("tuple_intersection_theta_double")),
("tuple_intersection_theta_integer", F::kernel("tuple_intersection_theta_integer")),
("tuple_sketch_estimate_double", F::kernel("tuple_sketch_estimate_double")),
("tuple_sketch_estimate_integer", F::kernel("tuple_sketch_estimate_integer")),
("tuple_sketch_summary_double", F::kernel("tuple_sketch_summary_double")),
("tuple_sketch_summary_integer", F::kernel("tuple_sketch_summary_integer")),
("tuple_sketch_theta_double", F::kernel("tuple_sketch_theta_double")),
("tuple_sketch_theta_integer", F::kernel("tuple_sketch_theta_integer")),
("tuple_union_double", F::kernel("tuple_union_double")),
("tuple_union_integer", F::kernel("tuple_union_integer")),
("tuple_union_theta_double", F::kernel("tuple_union_theta_double")),
("tuple_union_theta_integer", F::kernel("tuple_union_theta_integer")),
("uuid", F::kernel("uuid")),
("version", F::udf(sail_function::scalar::misc::version::SparkVersion::new())),

        ("assert_true", F::custom(resolve_assert_true)),
        ("current_catalog", F::custom(resolve_current_catalog)),
        ("current_database", F::custom(resolve_current_database)),
        ("current_schema", F::custom(resolve_current_database)),
        ("current_user", F::custom(resolve_current_user)),
        ("session_user", F::custom(resolve_current_user)),
        ("user", F::custom(resolve_current_user)),
        ("input_file_name", F::custom(resolve_input_file_name)),
        ("typeof", F::custom(resolve_typeof)),
        (
            "bitmap_bit_position",
            F::custom(|input| resolve_bitmap_bit_position(input, "bitmap_bit_position")),
        ),
        (
            "bitmap_bucket_number",
            F::custom(|input| resolve_bitmap_bit_position(input, "bitmap_bucket_number")),
        ),
        (
            "theta_union",
            F::custom(|input| resolve_theta_union(input, "theta_union")),
        ),
        (
            "hll_union",
            F::custom(|input| resolve_theta_union(input, "hll_union")),
        ),
        ("sha2", F::custom(resolve_sha2)),
        ("shiftrightunsigned", F::custom(resolve_shiftrightunsigned)),
        (">>>", F::custom(resolve_shiftrightunsigned)),
        ("~", F::custom(resolve_bitwise_not)),
    ]
}
