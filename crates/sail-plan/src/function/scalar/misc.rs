use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, Operator, ScalarUDF, cast, expr, lit, try_cast, when};
use datafusion_spark::function::bitmap::expr_fn as bitmap_fn;
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
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

use crate::coercion::{SAIL_DATE_DIFFERENCE_METADATA_KEY, spark_interval_metadata_for_expression};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    ScalarFunction, ScalarFunctionInput, is_spark_udt_field, spark_field_type_name,
};

fn assert_true(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (err_msg, col) = if arguments.len() == 1 {
        let col = arguments.one()?;
        (
            // Need to do this order to avoid the "value used after being moved" error.
            lit(ScalarValue::Utf8(Some(format!("'{}' is not true!", col)))),
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
    // `DATE - DATE` stays physically INT while interval ranges are not available to every
    // consumer: Spark reads an interval DAY as days in numeric casts, whereas Arrow reads a
    // Duration as seconds. The arithmetic resolver marks that temporary representation so its
    // later operands are still checked as Spark's interval. `typeof` is observational and can
    // report the declared Spark type without changing that physical value.
    let field = expr.to_field(function_context.schema)?.1;
    if field
        .metadata()
        .get(SAIL_DATE_DIFFERENCE_METADATA_KEY)
        .is_some_and(|value| value == "true")
    {
        return Ok(lit("interval day"));
    }
    // Arrow's interval types carry only their physical family; Spark also keeps the declared
    // start and end fields. Literals and casts put that range in Sail metadata, and a binary
    // expression may need to widen ranges from both children (for example DAY + HOUR is DAY TO
    // HOUR). Read the expression rather than only its result field so `typeof` follows Spark's
    // `DataType.typeName` for every interval range.
    if let Some(metadata) = spark_interval_metadata_for_expression(&expr, function_context.schema)?
    {
        use sail_common::spec::SparkIntervalMetadata;
        let type_of = match metadata {
            SparkIntervalMetadata::YearMonth {
                start_field,
                end_field,
            } => interval_type_name(start_field, end_field),
            SparkIntervalMetadata::DayTime {
                start_field,
                end_field,
            } => interval_type_name(start_field, end_field),
        };
        return Ok(lit(type_of));
    }
    let data_type = expr.get_type(function_context.schema)?;
    let service = function_context
        .session_context
        .extension::<PlanService>()?;
    let type_of = service
        .plan_formatter()
        .data_type_to_simple_string(&data_type)?;
    Ok(lit(type_of))
}

fn interval_type_name(
    start_field: impl std::fmt::Debug,
    end_field: impl std::fmt::Debug,
) -> String {
    let start_field = format!("{start_field:?}").to_lowercase();
    let end_field = format!("{end_field:?}").to_lowercase();
    if start_field == end_field {
        format!("interval {start_field}")
    } else {
        format!("interval {start_field} to {end_field}")
    }
}

/// The BIGINT a `bitmap_*` position function reads. Its `inputTypes` is `Seq(LongType)`
/// (`bitmapExpressions.scala`), and implicit casting reaches a BIGINT only from a NULL, a number or
/// a STRING, so any other argument is refused at analysis instead of being cast.
fn bitmap_position_argument(name: &str, input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let value = arguments.one()?;
    let (_, field) = value.to_field(function_context.schema)?;
    let data_type = field.data_type();
    let accepted = !is_spark_udt_field(&field)
        && (data_type.is_null()
            || data_type.is_numeric()
            || matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ));
    if !accepted {
        return Err(PlanError::analysis(format!(
            "cannot resolve {name} due to data type mismatch: the argument requires BIGINT, got {}",
            spark_field_type_name(&field)
        )));
    }
    // The implicit cast follows the ANSI flag like any `Cast` (`Cast.scala:886-905`): with it off a
    // malformed string is NULL, never an error.
    // TODO: with ANSI off Spark wraps an overflowing DECIMAL; `try_cast` reads it as NULL.
    if function_context.plan_config.ansi_mode {
        Ok(cast(value, DataType::Int64))
    } else if matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        // Spark's non-ANSI Numeric.toLong uses the JVM floating-point conversion:
        // NaN becomes zero, out-of-range values saturate (Cast.scala:903-905).
        let value = cast(value, DataType::Float64);
        Ok(when(expr_fn::isnan(value.clone()), lit(0_i64))
            .when(value.clone().gt_eq(lit(i64::MAX as f64)), lit(i64::MAX))
            .when(value.clone().lt_eq(lit(i64::MIN as f64)), lit(i64::MIN))
            .otherwise(try_cast(value, DataType::Int64))?)
    } else {
        Ok(try_cast(value, DataType::Int64))
    }
}

fn bitmap_bit_position(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    // `inputTypes = Seq(LongType)` and `dataType = LongType` (`bitmapExpressions.scala`). As an INT
    // it was a different arithmetic operand than Spark's: `DATE + bitmap_bit_position(1)` resolved here
    // and is refused there, since `DateAdd` takes no BIGINT.
    let value = bitmap_position_argument("bitmap_bit_position", input)?;
    let num_bits = 8 * 4 * 1024;
    Ok(when(
        value.clone().gt(lit(0)),
        (value.clone() - lit(1)) % lit(num_bits),
    )
    // `(-value) % NUM_BITS` on a Java long (`BitmapExpressionUtils.java:37-43`); negating after the
    // remainder gives the same answer without overflowing on the BIGINT minimum.
    .when(lit(true), -(value % lit(num_bits)))
    .end()?)
}

fn bitmap_bucket_number(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    // `inputTypes = Seq(LongType)` and `dataType = LongType` (`bitmapExpressions.scala`). As an INT
    // it was a different arithmetic operand than Spark's: `DATE + bitmap_bucket_number(1)` resolved here
    // and is refused there, since `DateAdd` takes no BIGINT.
    let value = bitmap_position_argument("bitmap_bucket_number", input)?;
    let num_bits = 8 * 4 * 1024;
    Ok(when(
        value.clone().gt(lit(0)),
        lit(1) + (value.clone() - lit(1)) / lit(num_bits),
    )
    .when(lit(true), value / lit(num_bits))
    .end()?)
}

fn theta_union(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let arguments = match arguments.len() {
        2 => {
            let (first, second) = arguments.two()?;
            vec![first, second, lit(DEFAULT_THETA_LG_NOM_ENTRIES)]
        }
        3 => {
            let (first, second, lg_nom_entries) = arguments.three()?;
            vec![first, second, cast(lg_nom_entries, DataType::Int32)]
        }
        count => {
            return Err(PlanError::invalid(format!(
                "theta_union requires 2 or 3 arguments, got {count}"
            )));
        }
    };
    Ok(ScalarUDF::from(ThetaUnionFunction::new()).call(arguments))
}

fn hll_union(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let arguments = match arguments.len() {
        2 => {
            let (first, second) = arguments.two()?;
            vec![first, second, lit(false)]
        }
        3 => {
            let (first, second, allow_different_lg_config_k) = arguments.three()?;
            vec![
                first,
                second,
                cast(allow_different_lg_config_k, DataType::Boolean),
            ]
        }
        count => {
            return Err(PlanError::invalid(format!(
                "hll_union requires 2 or 3 arguments, got {count}"
            )));
        }
    };
    Ok(ScalarUDF::from(HllUnionFunction::new()).call(arguments))
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
        ("hll_union", F::custom(hll_union)),
        ("theta_difference", F::udf(ThetaDifferenceFunction::new())),
        (
            "theta_intersection",
            F::udf(ThetaIntersectionFunction::new()),
        ),
        (
            "theta_sketch_estimate",
            F::udf(ThetaSketchEstimateFunction::new()),
        ),
        ("theta_union", F::custom(theta_union)),
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
            F::unknown("input_file_block_length"),
        ),
        (
            "input_file_block_start",
            F::unknown("input_file_block_start"),
        ),
        // TODO: Map this to DataFusion's file-name UDF after preserving Spark's empty-string
        // behavior for non-file inputs and covering driver-to-worker file scans.
        ("input_file_name", F::unknown("input_file_name")),
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
        ("uuid", F::nullary(expr_fn::uuid)),
        ("version", F::udf(SparkVersion::new())),
    ]
}
