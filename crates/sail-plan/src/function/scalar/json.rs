use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{cast, expr, lit, when, ScalarUDF};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions::unicode::expr_fn as unicode_fn;
use sail_function::scalar::json::{
    json_as_text_udf, json_length_udf, json_object_keys_udf, to_json_udf, SparkJsonTuple,
};
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn get_json_object(expr: expr::Expr, path: expr::Expr) -> PlanResult<expr::Expr> {
    let paths: Vec<expr::Expr> = match path {
        expr::Expr::Literal(ScalarValue::Utf8(Some(value)), _metadata)
            if value.starts_with("$.") =>
        {
            Ok::<_, DataFusionError>(value.replacen("$.", "", 1).split(".").map(lit).collect())
        }
        // FIXME: json_as_text_udf for array of paths with subpaths is not implemented, so only top level keys supported
        _ => Ok(vec![when(
            path.clone().like(lit("$.%")),
            unicode_fn::substr(path, lit(3)),
        )
        .when(lit(true), lit(""))
        .end()?]),
    }?;
    let mut args = Vec::with_capacity(1 + paths.len());
    args.push(expr);
    args.extend(paths);
    Ok(json_as_text_udf().call(args))
}

fn json_array_length(json_data: expr::Expr) -> expr::Expr {
    cast(json_length_udf().call(vec![json_data]), DataType::Int32)
}

fn json_object_keys(json_data: expr::Expr) -> expr::Expr {
    json_object_keys_udf().call(vec![json_data])
}

fn json_tuple(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context: _,
    } = input;
    if arguments.len() < 2 {
        return Err(crate::error::PlanError::invalid(
            "json_tuple requires at least 2 arguments: json string and at least one key",
        ));
    }

    let num_keys = arguments.len() - 1;
    let func = ScalarUDF::from(SparkJsonTuple::new());
    let struct_expr = func.call(arguments);

    // json_tuple only expands columns (not rows), so we can build
    // the MultiExpr directly without needing a separate rewriter.
    // Always use MultiExpr so rewrite_multi_expr extracts the alias names.
    let field_exprs: Vec<expr::Expr> = (0..num_keys)
        .map(|i| {
            let field_name = format!("c{i}");
            struct_expr.clone().field(&field_name).alias(field_name)
        })
        .collect();

    Ok(ScalarUDF::from(MultiExpr::new()).call(field_exprs))
}

fn to_json(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    // to_json accepts 1 or 2 arguments:
    // - to_json(expr) - convert expr to JSON string
    // - to_json(expr, options) - convert expr to JSON string with options
    match args.len() {
        1 | 2 => Ok(to_json_udf().call(args)),
        n => Err(PlanError::invalid(format!(
            "to_json expects 1 or 2 arguments, got {n}"
        ))),
    }
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_json", F::unknown("from_json")),
        ("get_json_object", F::binary(get_json_object)),
        ("json_array_length", F::unary(json_array_length)),
        ("json_object_keys", F::unary(json_object_keys)),
        ("json_tuple", F::custom(json_tuple)),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::var_arg(to_json)),
    ]
}
