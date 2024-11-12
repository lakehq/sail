use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions;
use datafusion::functions::expr_fn;
use datafusion::functions::string::contains::ContainsFunc;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::levenshtein::Levenshtein;
use crate::function::common::{Function, FunctionContext};
use crate::utils::ItemTaker;

fn regexp_replace(
    mut args: Vec<expr::Expr>,
    _function_context: &FunctionContext,
) -> PlanResult<expr::Expr> {
    if args.len() != 3 {
        return Err(PlanError::invalid("regexp_replace requires 3 arguments"));
    }
    // Spark replaces all occurrences of the pattern.
    args.push(expr::Expr::Literal(ScalarValue::Utf8(Some(
        "g".to_string(),
    ))));
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(
            functions::regex::regexpreplace::RegexpReplaceFunc::new(),
        )),
        args,
    }))
}

fn substr(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    if args.len() == 2 {
        let (first, second) = args.two()?;
        return Ok(expr_fn::substr(first, second));
    }
    if args.len() == 3 {
        let (first, second, third) = args.three()?;
        return Ok(expr_fn::substring(first, second, third));
    }
    Err(PlanError::invalid("substr requires 2 or 3 arguments"))
}

fn concat_ws(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    let (delimiter, args) = args.at_least_one()?;
    if args.is_empty() {
        return Ok(expr::Expr::Literal(ScalarValue::Utf8(Some("".to_string()))));
    }
    Ok(expr_fn::concat_ws(delimiter, args))
}

fn to_binary(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    let hex_format = expr::Expr::Literal(ScalarValue::Utf8(Some("hex".to_string())));
    if args.len() == 1 {
        let expr = args.one()?;
        let format = hex_format;
        return Ok(expr_fn::decode(expr, format));
    }
    if args.len() == 2 {
        let (expr, format) = args.two()?;
        return match format {
            expr::Expr::Literal(ScalarValue::Utf8(Some(ref s)))
                if s.to_lowercase() == "utf-8" || s.to_lowercase() == "utf8" =>
            {
                Ok(expr::Expr::Cast(expr::Cast {
                    expr: Box::new(expr),
                    data_type: DataType::Binary,
                }))
            }
            _ => Ok(expr_fn::decode(expr, format)),
        };
    }
    Err(PlanError::invalid("to_binary requires 1 or 2 arguments"))
}

fn base64(expr: expr::Expr) -> expr::Expr {
    let format = expr::Expr::Literal(ScalarValue::Utf8(Some("base64".to_string())));
    expr_fn::encode(expr, format)
}

fn unbase64(expr: expr::Expr) -> expr::Expr {
    let format = expr::Expr::Literal(ScalarValue::Utf8(Some("base64".to_string())));
    expr_fn::decode(expr, format)
}

fn overlay(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    if args.len() == 3 {
        return Ok(expr_fn::overlay(args));
    }
    if args.len() == 4 {
        let (str, substr, pos, count) = args.four()?;
        return match count {
            expr::Expr::Literal(ScalarValue::Int64(Some(-1)))
            | expr::Expr::Literal(ScalarValue::Int32(Some(-1))) => {
                Ok(expr_fn::overlay(vec![str, substr, pos]))
            }
            _ => Ok(expr_fn::overlay(vec![str, substr, pos, count])),
        };
    }
    Err(PlanError::invalid("overlay requires 3 or 4 arguments"))
}

fn position(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    if args.len() == 2 {
        let (substr, str) = args.two()?;
        return Ok(expr_fn::strpos(str, substr));
    }
    if args.len() == 3 {
        // TODO: optional third argument
        return Err(PlanError::todo(
            "position with 3 arguments is not supported yet",
        ));
    }
    Err(PlanError::invalid("position requires 2 arguments"))
}

fn space(n: expr::Expr) -> expr::Expr {
    expr_fn::repeat(lit(" "), n)
}

fn replace(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    if args.len() == 2 {
        let (str, substr) = args.two()?;
        return Ok(expr_fn::replace(str, substr, lit("")));
    }
    if args.len() == 3 {
        let (str, substr, replacement) = args.three()?;
        return Ok(expr_fn::replace(str, substr, replacement));
    }
    Err(PlanError::invalid("replace requires 2 or 3 arguments"))
}

fn upper(expr: expr::Expr) -> expr::Expr {
    // FIXME: Create UDF for upper to properly determine datatype
    let expr = match expr {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => expr,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr),
            data_type: DataType::Utf8,
        }),
    };
    expr_fn::upper(expr)
}

fn startswith(str: expr::Expr, substr: expr::Expr) -> expr::Expr {
    // FIXME: DataFusion 43.0.0 suddenly doesn't support casting to Utf8.
    //  Looks like many issues have been opened for this. Revert once fixed.
    let str = match str {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(str),
            data_type: DataType::Utf8,
        }),
    };
    let substr = match substr {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => substr,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(substr),
            data_type: DataType::Utf8,
        }),
    };
    expr_fn::starts_with(str, substr)
}

fn endswith(str: expr::Expr, substr: expr::Expr) -> expr::Expr {
    // FIXME: DataFusion 43.0.0 suddenly doesn't support casting to Utf8.
    //  Looks like many issues have been opened for this. Revert once fixed.
    let str = match str {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(str),
            data_type: DataType::Utf8,
        }),
    };
    let substr = match substr {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => substr,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(substr),
            data_type: DataType::Utf8,
        }),
    };
    expr_fn::ends_with(str, substr)
}

fn bit_length(expr: expr::Expr) -> expr::Expr {
    // FIXME: DataFusion 43.0.0 suddenly doesn't support casting to Utf8.
    //  Looks like many issues have been opened for this. Revert once fixed.
    let expr = match expr {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => expr,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr),
            data_type: DataType::Utf8,
        }),
    };
    expr_fn::bit_length(expr)
}

fn octet_length(expr: expr::Expr) -> expr::Expr {
    // FIXME: DataFusion 43.0.0 suddenly doesn't support casting to Utf8.
    //  Looks like many issues have been opened for this. Revert once fixed.
    let expr = match expr {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => expr,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr),
            data_type: DataType::Utf8,
        }),
    };
    expr_fn::octet_length(expr)
}

fn ascii(expr: expr::Expr) -> expr::Expr {
    // FIXME: DataFusion 43.0.0 suddenly doesn't support casting to Utf8.
    //  Looks like many issues have been opened for this. Revert once fixed.
    let expr = match expr {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => expr,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr),
            data_type: DataType::Utf8,
        }),
    };
    expr_fn::ascii(expr)
}

fn contains(str: expr::Expr, search_str: expr::Expr) -> expr::Expr {
    // FIXME: DataFusion 43.0.0 suddenly doesn't support casting to Utf8.
    //  Looks like many issues have been opened for this. Revert once fixed.
    let str = match str {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(str),
            data_type: DataType::Utf8,
        }),
    };
    let search_str = match search_str {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_)) => search_str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(search_str),
            data_type: DataType::Utf8,
        }),
    };
    expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(ContainsFunc::new())),
        args: vec![str, search_str],
    })
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("ascii", F::unary(ascii)),
        ("base64", F::unary(base64)),
        ("bit_length", F::unary(bit_length)),
        ("btrim", F::var_arg(expr_fn::btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(expr_fn::chr)),
        ("concat_ws", F::custom(concat_ws)),
        ("contains", F::binary(contains)),
        ("decode", F::unknown("decode")),
        ("elt", F::unknown("elt")),
        ("encode", F::unknown("encode")),
        ("endswith", F::binary(endswith)),
        ("find_in_set", F::binary(expr_fn::find_in_set)),
        ("format_number", F::unknown("format_number")),
        ("format_string", F::unknown("format_string")),
        ("initcap", F::unary(expr_fn::initcap)),
        ("instr", F::binary(expr_fn::instr)),
        ("lcase", F::unary(expr_fn::lower)),
        ("left", F::binary(expr_fn::left)),
        ("len", F::unary(expr_fn::length)),
        ("length", F::unary(expr_fn::length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::unary(expr_fn::lower)),
        ("lpad", F::var_arg(expr_fn::lpad)),
        ("ltrim", F::var_arg(expr_fn::ltrim)),
        ("luhn_check", F::unknown("luhn_check")),
        ("mask", F::unknown("mask")),
        ("octet_length", F::unary(octet_length)),
        ("overlay", F::custom(overlay)),
        ("position", F::custom(position)),
        ("printf", F::unknown("printf")),
        ("regexp_count", F::unknown("regexp_count")),
        ("regexp_extract", F::unknown("regexp_extract")),
        ("regexp_extract_all", F::unknown("regexp_extract_all")),
        ("regexp_instr", F::unknown("regexp_instr")),
        ("regexp_replace", F::custom(regexp_replace)),
        ("regexp_substr", F::unknown("regexp_substr")),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::custom(replace)),
        ("right", F::binary(expr_fn::right)),
        ("rpad", F::var_arg(expr_fn::rpad)),
        ("rtrim", F::var_arg(expr_fn::rtrim)),
        ("sentences", F::unknown("sentences")),
        ("soundex", F::unknown("soundex")),
        ("space", F::unary(space)),
        ("split", F::unknown("split")),
        ("split_part", F::ternary(expr_fn::split_part)),
        ("startswith", F::binary(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::custom(to_binary)),
        ("to_char", F::unknown("to_char")),
        ("to_number", F::unknown("to_number")),
        ("to_varchar", F::unknown("to_varchar")),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", F::var_arg(expr_fn::trim)),
        ("try_to_binary", F::unknown("try_to_binary")),
        ("try_to_number", F::unknown("try_to_number")),
        ("ucase", F::unary(upper)),
        ("unbase64", F::unary(unbase64)),
        ("upper", F::unary(upper)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}
