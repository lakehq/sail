use datafusion_common::ScalarValue;
use datafusion_functions_json::functions;
use datafusion_expr::{expr, lit};

use crate::function::common::Function;

fn get_json_object(expr: expr::Expr, path: expr::Expr) -> expr::Expr {
    println!("CHECK HERE: expr: {expr:?}\npath: {path:?}");
    let path = match &path {
        expr::Expr::Literal(ScalarValue::Utf8(Some(value))) => {
            if value.starts_with("$") {
                lit(ScalarValue::Utf8(Some(value[2..].trim().to_string())))
            } else if value.starts_with("$.") {
                lit(ScalarValue::Utf8(Some(value[3..].trim().to_string())))
            } else {
                path
            }
        }
        _ => path,
    };
    println!("CHECK HERE: expr: {expr:?}\npath: {path:?}");
    functions::json_get_json(expr, path)
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("from_json", F::unknown("from_json")),
        ("get_json_object", F::binary(get_json_object)),
        ("json_array_length", F::unknown("json_array_length")),
        ("json_object_keys", F::unknown("json_object_keys")),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::unknown("to_json")),
    ]
}
