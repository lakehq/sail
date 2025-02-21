use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit};
use datafusion_functions_json::udfs;

use crate::error::PlanResult;
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

fn get_json_object(input: FunctionInput) -> PlanResult<expr::Expr> {
    // > 1 path means nested access e.g. json_as_text(json, p1, p2) => json.p1.p2
    let FunctionInput { arguments, .. } = input;
    let (expr, paths) = arguments.at_least_one()?;
    let paths: Vec<expr::Expr> = paths
        .into_iter()
        .map(|path| match &path {
            expr::Expr::Literal(ScalarValue::Utf8(Some(value))) => {
                if value.starts_with("$") {
                    let nth = if value.starts_with("$.") { 2 } else { 1 };
                    let index = value
                        .char_indices()
                        .nth(nth)
                        .map(|(idx, _)| idx)
                        .unwrap_or(value.len());
                    lit(ScalarValue::Utf8(Some(value[index..].to_string())))
                } else {
                    path
                }
            }
            _ => path,
        })
        .collect();
    let mut args = Vec::with_capacity(1 + paths.len());
    args.push(expr);
    args.extend(paths);
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: udfs::json_as_text_udf(),
        args,
    }))
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("from_json", F::unknown("from_json")),
        ("get_json_object", F::custom(get_json_object)),
        ("json_array_length", F::scalar_udf(udfs::json_length_udf)),
        ("json_object_keys", F::unknown("json_object_keys")),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::unknown("to_json")),
    ]
}
