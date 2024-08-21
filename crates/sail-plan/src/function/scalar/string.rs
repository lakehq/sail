use std::sync::Arc;

use datafusion::functions;
use datafusion::functions::expr_fn;
use datafusion::functions::string::contains::ContainsFunc;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::levenshtein::Levenshtein;
use crate::function::common::Function;
use crate::utils::ItemTaker;

fn regexp_replace(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
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

fn substr(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
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

fn concat_ws(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let (delimiter, args) = args.at_least_one()?;
    if args.is_empty() {
        return Ok(expr::Expr::Literal(ScalarValue::Utf8(Some("".to_string()))));
    }
    Ok(expr_fn::concat_ws(delimiter, args))
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("ascii", F::unary(expr_fn::ascii)),
        ("base64", F::unknown("base64")),
        ("bit_length", F::unary(expr_fn::bit_length)),
        ("btrim", F::var_arg(expr_fn::btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(expr_fn::chr)),
        ("concat_ws", F::custom(concat_ws)),
        ("contains", F::udf(ContainsFunc::new())),
        ("decode", F::unknown("decode")),
        ("elt", F::unknown("elt")),
        ("encode", F::unknown("encode")),
        ("endswith", F::binary(expr_fn::ends_with)),
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
        ("locate", F::unknown("locate")),
        ("lower", F::unary(expr_fn::lower)),
        ("lpad", F::var_arg(expr_fn::lpad)),
        ("ltrim", F::var_arg(expr_fn::ltrim)),
        ("luhn_check", F::unknown("luhn_check")),
        ("mask", F::unknown("mask")),
        ("octet_length", F::unary(expr_fn::octet_length)),
        ("overlay", F::unknown("overlay")),
        ("position", F::unknown("position")),
        ("printf", F::unknown("printf")),
        ("regexp_count", F::unknown("regexp_count")),
        ("regexp_extract", F::unknown("regexp_extract")),
        ("regexp_extract_all", F::unknown("regexp_extract_all")),
        ("regexp_instr", F::unknown("regexp_instr")),
        ("regexp_replace", F::custom(regexp_replace)),
        ("regexp_substr", F::unknown("regexp_substr")),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::unknown("replace")),
        ("right", F::binary(expr_fn::right)),
        ("rpad", F::var_arg(expr_fn::rpad)),
        ("rtrim", F::var_arg(expr_fn::rtrim)),
        ("sentences", F::unknown("sentences")),
        ("soundex", F::unknown("soundex")),
        ("space", F::unknown("space")),
        ("split", F::unknown("split")),
        ("split_part", F::unknown("split_part")),
        ("startswith", F::binary(expr_fn::starts_with)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::unknown("to_binary")),
        ("to_char", F::unknown("to_char")),
        ("to_number", F::unknown("to_number")),
        ("to_varchar", F::unknown("to_varchar")),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", F::var_arg(expr_fn::trim)),
        ("try_to_binary", F::unknown("try_to_binary")),
        ("try_to_number", F::unknown("try_to_number")),
        ("ucase", F::unary(expr_fn::upper)),
        ("unbase64", F::unknown("unbase64")),
        ("upper", F::unary(expr_fn::upper)),
    ]
}
