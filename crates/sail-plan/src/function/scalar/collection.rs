use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, when};
use datafusion_functions::math::expr_fn::abs;
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::type_coercion::nested::widen_list_nullability;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::conditional::coalesce::CoalesceFunc;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn size(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;

    let result = match value.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => Ok(cast(expr_fn::array_length(value), DataType::Int32)),
        DataType::Map(..) => Ok(cast(expr_fn::cardinality(value), DataType::Int32)),
        wrong_type => Err(PlanError::InvalidArgument(format!(
            "size expects List or Map as argument, got {wrong_type:?}"
        ))),
    }?;

    let config = input.function_context.plan_config;
    if config.legacy_size_of_null && !config.ansi_mode {
        Ok(ScalarUDF::from(CoalesceFunc::new()).call(vec![result, lit(-1_i32)]))
    } else {
        // TODO: Preserve input nullability in nonlegacy mode; see the size/cardinality schema tests.
        Ok(result)
    }
}

fn element_at(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    let (collection, element) = input.arguments.two()?;
    let name = if is_try {
        "try_element_at"
    } else {
        "element_at"
    };
    let null_or_out_of_bounds = if is_try || !input.function_context.plan_config.ansi_mode {
        lit(ScalarValue::Null)
    } else {
        ScalarUDF::from(RaiseError::new()).call(vec![lit("element_at: the index is out of bounds")])
    };

    Ok(match collection.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => {
            when(element.clone().eq(lit(0)), ScalarUDF::from(RaiseError::new()).call(
            vec![lit(format!("{name}: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"))]
                )).when( abs(element.clone()).not_between(lit(1), expr_fn::array_length(collection.clone())),
                null_or_out_of_bounds
                )
                .when(lit(true), expr_fn::array_element(collection, element)).end()?
        }
        DataType::Map(entries, sorted) => {
            let (collection, element) = cast_map_key_to_widened_nested_nullability(
                collection,
                &entries,
                sorted,
                element,
                input.function_context.schema,
            )?;
            expr_fn::array_element(expr_fn::map_extract(collection, element), lit(1))
        }
        wrong_type => ScalarUDF::from(RaiseError::new()).call(vec![lit(format!(
            "{name} expects List or Map type as first argument, got {wrong_type:?}",
        ))]),
    })
}

/// Casts a map and a key that differ only in the nullability of nested fields of the key type
/// to their common key type, since DataFusion casts the key to the map key type.
fn cast_map_key_to_widened_nested_nullability(
    map: expr::Expr,
    entries: &FieldRef,
    sorted: bool,
    key: expr::Expr,
    schema: &DFSchemaRef,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let DataType::Struct(fields) = entries.data_type() else {
        return Ok((map, key));
    };
    let (Some(key_field), Some(value_field), 2) = (fields.first(), fields.get(1), fields.len())
    else {
        return Ok((map, key));
    };
    let key_type = key.get_type(schema.as_ref())?;
    if !key_type.is_nested() || &key_type == key_field.data_type() {
        return Ok((map, key));
    }
    let Some(widened) = widen_list_nullability(&[key_field.data_type().clone(), key_type.clone()])
    else {
        return Ok((map, key));
    };
    let map = if &widened == key_field.data_type() {
        map
    } else {
        let fields = vec![
            Arc::new(key_field.as_ref().clone().with_data_type(widened.clone())),
            Arc::clone(value_field),
        ];
        let entries = entries
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(fields.into()));
        cast(map, DataType::Map(Arc::new(entries), sorted))
    };
    let key = if key_type == widened {
        key
    } else {
        cast(key, widened)
    };
    Ok((map, key))
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("cardinality", F::custom(size)),
        ("deep_size", F::unary(expr_fn::cardinality)),
        ("element_at", F::custom(|input| element_at(input, false))),
        ("size", F::custom(size)),
        ("array_concat", F::udf(SparkConcat::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
        ("try_element_at", F::custom(|input| element_at(input, true))),
    ]
}
