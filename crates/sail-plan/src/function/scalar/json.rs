use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, expr, lit, when, Expr, ScalarUDF};
use datafusion_functions::unicode::expr_fn as unicode_fn;
use datafusion_spark::expr_fn::json_tuple as df_json_tuple;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::json::{
    json_as_text_udf, json_length_udf, json_object_keys_udf, to_json_udf, SparkFromJson,
    SparkSchemaOfJson,
};

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F, ScalarFunctionInput};

/// Parse a Spark `get_json_object` JSONPath into the literal arguments for
/// `json_as_text`: a `Utf8` literal per object key and an `Int64` literal per
/// array index (which `json_as_text` then reads as `JsonPath::Key` /
/// `JsonPath::Index` respectively).
///
/// Supports the subset Spark accepts: a leading `$`, dot notation (`.key`),
/// single-quoted bracket notation (`['key']`, which allows keys containing
/// dots), and array indexing (`[0]`). A bare `$` selects the whole document
/// (empty key list). Wildcards (`[*]`) and double-quoted brackets are not
/// supported — Spark returns NULL for the latter — so they parse as `None`,
/// letting the caller emit a NULL result like Spark.
fn parse_json_path(path: &str) -> Option<Vec<expr::Expr>> {
    let mut chars = path.chars().peekable();
    if chars.next()? != '$' {
        return None;
    }
    let mut keys = Vec::new();
    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next();
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return None;
                }
                keys.push(lit(key));
            }
            '[' => {
                chars.next();
                match chars.peek() {
                    Some('\'') => {
                        chars.next();
                        let mut key = String::new();
                        loop {
                            match chars.next() {
                                Some('\'') => break,
                                Some(c) => key.push(c),
                                None => return None,
                            }
                        }
                        if chars.next()? != ']' {
                            return None;
                        }
                        keys.push(lit(key));
                    }
                    Some(c) if c.is_ascii_digit() => {
                        let mut index = String::new();
                        while let Some(&c) = chars.peek() {
                            if c == ']' {
                                break;
                            }
                            index.push(c);
                            chars.next();
                        }
                        if chars.next()? != ']' {
                            return None;
                        }
                        keys.push(lit(index.parse::<i64>().ok()?));
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
    Some(keys)
}

fn get_json_object(expr: expr::Expr, path: expr::Expr) -> PlanResult<expr::Expr> {
    let paths: Vec<expr::Expr> = match path {
        expr::Expr::Literal(ScalarValue::Utf8(Some(value)), _metadata) => {
            match parse_json_path(&value) {
                // Spark returns NULL for paths it cannot parse (including the empty string and paths not anchored at `$`).
                Some(keys) => keys,
                None => return Ok(lit(ScalarValue::Utf8(None))),
            }
        }
        // FIXME: json_as_text_udf for array of paths with subpaths is not implemented, so only top level keys supported
        _ => vec![when(
            path.clone().like(lit("$.%")),
            unicode_fn::substr(path, lit(3)),
        )
        .when(lit(true), lit(""))
        .end()?],
    };
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

fn to_json(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    // to_json accepts 1 or 2 arguments:
    // - to_json(expr) - convert expr to JSON string
    // - to_json(expr, options) - convert expr to JSON string with options
    // Note: the SparkToJson UDF detects Variant inputs and delegates to variant_to_json,
    // which ignores any options provided.
    // See: https://docs.databricks.com/en/sql/language-manual/functions/to_json.html
    match args.len() {
        1 | 2 => Ok(to_json_udf().call(args)),
        n => Err(PlanError::invalid(format!(
            "to_json expects 1 or 2 arguments, got {n}"
        ))),
    }
}

fn from_json(
    ScalarFunctionInput {
        mut arguments,
        function_context,
    }: ScalarFunctionInput,
) -> PlanResult<expr::Expr> {
    let tz = function_context.plan_config.session_timezone.clone();
    // Try to constant-fold the schema argument (index 1) if it's not already a literal.
    // This handles cases like `from_json(col, schema_of_json(lit(...)))` where the schema
    // is a constant expression that can be evaluated at planning time.
    if arguments.len() >= 2 && !matches!(&arguments[1], expr::Expr::Literal(_, _)) {
        let evaluator = LiteralEvaluator::new();
        if let Ok(scalar) = evaluator.evaluate(&arguments[1]) {
            arguments[1] = expr::Expr::Literal(scalar, None);
        }
    }
    let udf = ScalarUDF::from(SparkFromJson::new(tz));
    Ok(udf.call(arguments))
}

fn json_tuple(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput { arguments, .. } = input;

    // Split into (json_expr, field_name_exprs)
    let (_json_expr, field_names) = arguments.split_first().ok_or_else(|| {
        PlanError::invalid(
            "json_tuple requires at least 2 arguments (json string and at least one field name)",
        )
    })?;

    // Validate field names are string literals
    for expr in field_names {
        match expr {
            Expr::Literal(ScalarValue::Utf8(Some(_)), _) => (),
            _ => {
                return Err(PlanError::invalid(
                    "json_tuple field names must be string literals",
                ))
            }
        }
    }

    // Build the json_tuple call with all field names
    let json_tuple_expr = df_json_tuple(arguments);

    // Wrap in array and explode with Inline
    let array_expr = ScalarUDF::from(SparkArray::new()).call(vec![json_tuple_expr]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Inline)).call(vec![array_expr]))
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        ("from_json", F::custom(from_json)),
        ("get_json_object", F::binary(get_json_object)),
        ("json_array_length", F::unary(json_array_length)),
        ("json_object_keys", F::unary(json_object_keys)),
        ("json_tuple", F::custom(json_tuple)),
        ("schema_of_json", F::udf(SparkSchemaOfJson::new())),
        ("to_json", F::var_arg(to_json)),
    ]
}
