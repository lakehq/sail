use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion::functions_nested::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, expr, lit, ExprSchemable};
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::map::str_to_map::StrToMap;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    if !input.arguments.len().is_multiple_of(2) {
        return Err(PlanError::InvalidArgument(format!(
            "map(k1, v1, k2, v2, ...): expect number of args to be multiple of 2, got {}",
            input.arguments.len()
        )));
    }

    let schema = input.function_context.schema;
    let (keys, values): (Vec<_>, Vec<_>) = input
        .arguments
        .chunks(2)
        .map(|key_value| (key_value[0].clone(), key_value[1].clone()))
        .unzip();
    let value_contains_null = values.iter().try_fold(false, |nullable, value| {
        Ok::<_, PlanError>(nullable || value.nullable(schema.as_ref())?)
    })?;

    let keys = expr_fn::make_array(keys);
    let values = expr_fn::make_array(values);
    let values = cast_list_value_nullability(values, schema, true)?;
    let expr = F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })?;
    cast_map_value_nullability(expr, schema, value_contains_null)
}

fn map_from_arrays(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let schema = input.function_context.schema;
    let (keys, values) = input.arguments.two()?;
    let value_contains_null = match values.get_type(schema.as_ref())? {
        DataType::List(field) | DataType::LargeList(field) => field.is_nullable(),
        _ => true,
    };
    let values = cast_list_value_nullability(values, schema, true)?;
    let expr = F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })?;
    cast_map_value_nullability(expr, schema, value_contains_null)
}

fn map_from_entries(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let schema = input.function_context.schema;
    let entries = input.arguments.one()?;
    let value_contains_null = map_entries_value_contains_null(&entries.get_type(schema.as_ref())?);
    let entries = cast_map_entries_value_nullability(entries, schema, true)?;
    let expr = F::udf(MapFromEntries::new())(ScalarFunctionInput {
        arguments: vec![entries],
        function_context: input.function_context,
    })?;
    cast_map_value_nullability(expr, schema, value_contains_null)
}

fn map_entries(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let map = input.arguments.one()?;
    let value_contains_null = map_value_contains_null(&map.get_type(schema.as_ref())?);
    let map = cast_map_value_nullability(map, schema, true)?;
    let expr = expr_fn::map_entries(map);
    cast_map_entries_value_nullability(expr, schema, value_contains_null)
}

fn map_values(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let map = input.arguments.one()?;
    let value_contains_null = map_value_contains_null(&map.get_type(schema.as_ref())?);
    let map = cast_map_value_nullability(map, schema, true)?;
    let expr = expr_fn::map_values(map);
    cast_list_value_nullability(expr, schema, value_contains_null)
}

fn map_value_contains_null(data_type: &DataType) -> bool {
    let DataType::Map(entries, _) = data_type else {
        return true;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return true;
    };
    fields
        .get(1)
        .map(|field| field.is_nullable())
        .unwrap_or(true)
}

fn map_entries_value_contains_null(data_type: &DataType) -> bool {
    let (DataType::List(field) | DataType::LargeList(field)) = data_type else {
        return true;
    };
    let DataType::Struct(fields) = field.data_type() else {
        return true;
    };
    fields
        .get(1)
        .map(|field| field.is_nullable())
        .unwrap_or(true)
}

fn cast_list_value_nullability(
    expr: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
    nullable: bool,
) -> PlanResult<expr::Expr> {
    let data_type = expr.get_type(schema.as_ref())?;
    let target_type = match data_type {
        DataType::List(field) if field.is_nullable() != nullable => {
            DataType::List(with_nullable(&field, nullable))
        }
        DataType::LargeList(field) if field.is_nullable() != nullable => {
            DataType::LargeList(with_nullable(&field, nullable))
        }
        _ => return Ok(expr),
    };
    Ok(cast(expr, target_type))
}

fn cast_map_entries_value_nullability(
    expr: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
    nullable: bool,
) -> PlanResult<expr::Expr> {
    let data_type = expr.get_type(schema.as_ref())?;
    let (target_type, changed) = match data_type {
        DataType::List(field) => {
            let target_field = cast_map_entry_field_value_nullability(&field, nullable)?;
            let changed = !Arc::ptr_eq(&field, &target_field);
            (DataType::List(target_field), changed)
        }
        DataType::LargeList(field) => {
            let target_field = cast_map_entry_field_value_nullability(&field, nullable)?;
            let changed = !Arc::ptr_eq(&field, &target_field);
            (DataType::LargeList(target_field), changed)
        }
        _ => return Ok(expr),
    };
    if changed {
        Ok(cast(expr, target_type))
    } else {
        Ok(expr)
    }
}

fn cast_map_entry_field_value_nullability(
    field: &FieldRef,
    nullable: bool,
) -> PlanResult<FieldRef> {
    let DataType::Struct(fields) = field.data_type() else {
        return Ok(field.clone());
    };
    let fields_vec = fields.iter().collect::<Vec<_>>();
    let [key_field, value_field] = fields_vec.as_slice() else {
        return Ok(field.clone());
    };
    if value_field.is_nullable() == nullable {
        return Ok(field.clone());
    }
    Ok(Arc::new(field.as_ref().clone().with_data_type(
        DataType::Struct(vec![(*key_field).clone(), with_nullable(value_field, nullable)].into()),
    )))
}

fn cast_map_value_nullability(
    expr: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
    value_contains_null: bool,
) -> PlanResult<expr::Expr> {
    let data_type = expr.get_type(schema.as_ref())?;
    let DataType::Map(entries_field, keys_sorted) = data_type else {
        return Ok(expr);
    };
    let DataType::Struct(fields) = entries_field.data_type() else {
        return Ok(expr);
    };
    let fields_vec = fields.iter().collect::<Vec<_>>();
    let [key_field, value_field] = fields_vec.as_slice() else {
        return Ok(expr);
    };
    if value_field.is_nullable() == value_contains_null {
        return Ok(expr);
    }
    let fields = Fields::from(vec![
        (*key_field).clone(),
        with_nullable(value_field, value_contains_null),
    ]);
    let entries_field = Arc::new(
        entries_field
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(fields)),
    );
    Ok(cast(expr, DataType::Map(entries_field, keys_sorted)))
}

fn with_nullable(field: &FieldRef, nullable: bool) -> FieldRef {
    Arc::new(field.as_ref().clone().with_nullable(nullable))
}

fn map_concat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use datafusion_expr::Expr;

    use crate::function::common::ScalarFunctionBuilder as F;

    // If any input is NULL, return NULL
    // This is done by creating a CASE expression that checks each input for NULL
    if input.arguments.is_empty() {
        return Ok(lit(ScalarValue::Null));
    }

    // Build the result expression
    let (keys, values) = input
        .arguments
        .iter()
        .map(|map| {
            (
                expr_fn::map_keys(map.clone()),
                expr_fn::map_values(map.clone()),
            )
        })
        .unzip();

    let keys = expr_fn::array_concat(keys);
    let values = expr_fn::array_concat(values);
    let result = F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })?;

    // Wrap the result with CASE to handle NULLs:
    // CASE WHEN arg1 IS NULL OR arg2 IS NULL OR ... THEN NULL ELSE result END
    // We already checked that arguments is not empty, so reduce will always return Some
    if let Some(null_check) = input
        .arguments
        .iter()
        .map(|arg| arg.clone().is_null())
        .reduce(|a, b| a.or(b))
    {
        Ok(Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(null_check), Box::new(lit(ScalarValue::Null)))],
            else_expr: Some(Box::new(result)),
        }))
    } else {
        // This should never happen because we checked arguments is not empty
        Ok(result)
    }
}

fn map_contains_key(map: expr::Expr, key: expr::Expr) -> expr::Expr {
    expr_fn::array_has(expr_fn::map_keys(map), key)
}

fn str_to_map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let (strs, delims) = input.arguments.at_least_one()?;

    let pair_delims = delims.first().cloned().unwrap_or(lit(","));
    let key_value_delims = delims.get(1).cloned().unwrap_or(lit(":"));

    F::udf(StrToMap::new())(ScalarFunctionInput {
        arguments: vec![strs, pair_delims, key_value_delims],
        function_context: input.function_context,
    })
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("map", F::custom(map)),
        ("map_concat", F::custom(map_concat)),
        ("map_contains_key", F::binary(map_contains_key)),
        ("map_entries", F::custom(map_entries)),
        ("map_from_arrays", F::custom(map_from_arrays)),
        ("map_from_entries", F::custom(map_from_entries)),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::custom(map_values)),
        ("str_to_map", F::custom(str_to_map)),
    ]
}
