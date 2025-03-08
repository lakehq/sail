use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::string::contains::ContainsFunc;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::string::levenshtein::Levenshtein;
use crate::extension::function::string::spark_base64::{SparkBase64, SparkUnbase64};
use crate::extension::function::string::spark_encode_decode::{SparkDecode, SparkEncode};
use crate::extension::function::string::spark_mask::SparkMask;
use crate::extension::function::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

fn regexp_replace(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { mut arguments, .. } = input;
    if arguments.len() != 3 {
        return Err(PlanError::invalid("regexp_replace requires 3 arguments"));
    }
    // Spark replaces all occurrences of the pattern.
    arguments.push(expr::Expr::Literal(ScalarValue::Utf8(Some(
        "g".to_string(),
    ))));
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(
            functions::regex::regexpreplace::RegexpReplaceFunc::new(),
        )),
        args: arguments,
    }))
}

fn substr(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    if arguments.len() == 2 {
        let (first, second) = arguments.two()?;
        let first = match first {
            expr::Expr::Literal(ScalarValue::Utf8(_))
            | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
            | expr::Expr::Literal(ScalarValue::Utf8View(_)) => first,
            _ => expr::Expr::Cast(expr::Cast {
                expr: Box::new(first),
                data_type: DataType::Utf8,
            }),
        };
        return Ok(expr_fn::substr(first, second));
    }
    if arguments.len() == 3 {
        let (first, second, third) = arguments.three()?;
        let first = match first {
            expr::Expr::Literal(ScalarValue::Utf8(_))
            | expr::Expr::Literal(ScalarValue::LargeUtf8(_))
            | expr::Expr::Literal(ScalarValue::Utf8View(_)) => first,
            _ => expr::Expr::Cast(expr::Cast {
                expr: Box::new(first),
                data_type: DataType::Utf8,
            }),
        };
        return Ok(expr_fn::substring(first, second, third));
    }
    Err(PlanError::invalid("substr requires 2 or 3 arguments"))
}

fn concat_ws(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (delimiter, args) = arguments.at_least_one()?;
    if args.is_empty() {
        return Ok(expr::Expr::Literal(ScalarValue::Utf8(Some("".to_string()))));
    }
    Ok(expr_fn::concat_ws(delimiter, args))
}

fn overlay(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    if arguments.len() == 3 {
        return Ok(expr_fn::overlay(arguments));
    }
    if arguments.len() == 4 {
        let (str, substr, pos, count) = arguments.four()?;
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

fn position(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    if arguments.len() == 2 {
        let (substr, str) = arguments.two()?;
        return Ok(expr_fn::strpos(str, substr));
    }
    if arguments.len() == 3 {
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

fn replace(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    if arguments.len() == 2 {
        let (str, substr) = arguments.two()?;
        return Ok(expr_fn::replace(str, substr, lit("")));
    }
    if arguments.len() == 3 {
        let (str, substr, replacement) = arguments.three()?;
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
        ("base64", F::udf(SparkBase64::new())),
        ("bit_length", F::unary(bit_length)),
        ("btrim", F::var_arg(expr_fn::btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(expr_fn::chr)),
        ("concat_ws", F::custom(concat_ws)),
        ("contains", F::binary(contains)),
        ("decode", F::udf(SparkDecode::new())),
        ("elt", F::unknown("elt")),
        ("encode", F::udf(SparkEncode::new())),
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
        ("mask", F::udf(SparkMask::new())),
        ("octet_length", F::unary(octet_length)),
        ("overlay", F::custom(overlay)),
        ("position", F::custom(position)),
        ("printf", F::unknown("printf")),
        ("regexp_count", F::udf(RegexpCountFunc::new())),
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
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::unknown("to_char")),
        ("to_number", F::unknown("to_number")),
        ("to_varchar", F::unknown("to_varchar")),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", F::var_arg(expr_fn::trim)),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::unknown("try_to_number")),
        ("ucase", F::unary(upper)),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::unary(upper)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}
