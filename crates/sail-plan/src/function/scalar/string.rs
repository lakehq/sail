use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{cast, expr, lit, try_cast, when, ExprSchemable};
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_number::SparkToNumber;
use sail_function::scalar::string::spark_try_to_number::SparkTryToNumber;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn regexp_replace(string: expr::Expr, pattern: expr::Expr, replacement: expr::Expr) -> expr::Expr {
    regex_fn::regexp_replace(string, pattern, replacement, Some(lit("g")))
}

fn substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let length_opt = (arguments.len() == 3).then(|| arguments.pop()).flatten();
    let (string, position) = arguments
        .two()
        .map_err(|_| PlanError::invalid("substr requires 2 or 3 arguments"))?;
    let string = cast_to_logical_string_or_try(string, function_context.schema, false)?;
    let substr_res = match length_opt {
        Some(length) => expr_fn::substring(string, position, length),
        None => expr_fn::substr(string, position),
    };
    // TODO: Spark client throws "UNEXPECTED EXCEPTION: ArrowInvalid('Unrecognized type: 24')"
    //  when the return type is Utf8View.
    Ok(cast(substr_res, DataType::Utf8))
}

fn concat_ws(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let (delimiter, args) = args.at_least_one()?;
    if args.is_empty() {
        return Ok(lit(""));
    }
    Ok(expr_fn::concat_ws(delimiter, args))
}

fn overlay(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    if args.len() == 4
        && matches!(
            args[3],
            expr::Expr::Literal(ScalarValue::Int64(Some(-1)), _)
                | expr::Expr::Literal(ScalarValue::Int32(Some(-1)), _)
        )
    {
        args.pop();
    }
    Ok(expr_fn::overlay(args))
}

fn position(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let start_opt = (arguments.len() == 3).then(|| arguments.pop()).flatten();
    let (substr, str) = arguments
        .into_iter()
        .map(|expr| cast_to_logical_string_or_try(expr, function_context.schema, false))
        .collect::<PlanResult<Vec<_>>>()?
        .two()
        .map_err(|_| PlanError::invalid("position requires 2 or 3 arguments"))?;
    Ok(match start_opt {
        Some(start) => {
            let str_from_pos = expr_fn::substr(str, start.clone());
            let pos = expr_fn::strpos(str_from_pos, substr);
            when(pos.clone().eq(lit(0)), lit(0))
                .when(pos.clone().gt(lit(0)), start + pos - lit(1))
                .end()?
        }
        None => expr_fn::strpos(str, substr),
    })
}

fn space(n: expr::Expr) -> expr::Expr {
    expr_fn::repeat(lit(" "), n)
}

fn replace(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let replacement = (args.len() == 3)
        .then(|| args.pop())
        .flatten()
        .unwrap_or_else(|| lit(""));
    let (str, substr) = args
        .two()
        .map_err(|_| PlanError::invalid("replace requires 2 or 3 arguments"))?;
    Ok(expr_fn::replace(str, substr, replacement))
}

fn lower(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr_fn::lower(validate_utf8(input)?))
}

fn upper(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr_fn::upper(validate_utf8(input)?))
}

fn startswith(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::starts_with)(input)
}

fn endswith(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::ends_with)(input)
}

fn contains(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::contains)(input)
}

fn bit_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::bit_length)(input)
}

fn octet_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::octet_length)(input)
}

fn ascii(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::ascii)(input)
}

fn cast_to_logical_string_or_try(
    arg: expr::Expr,
    schema: &DFSchema,
    is_try: bool,
) -> PlanResult<expr::Expr> {
    let data_type = match arg.get_type(schema)? {
        DataType::LargeBinary | DataType::LargeUtf8 => DataType::LargeUtf8,
        DataType::Utf8View => DataType::Utf8View,
        _ => DataType::Utf8,
    };
    Ok(if is_try {
        try_cast(arg, data_type)
    } else {
        cast(arg, data_type)
    })
}

fn validate_utf8_or_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    cast_to_logical_string_or_try(
        input.arguments.one()?,
        input.function_context.schema,
        is_try,
    )
}

fn validate_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    validate_utf8_or_try(input, false)
}

fn try_validate_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    validate_utf8_or_try(input, true)
}

fn is_valid_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(try_validate_utf8(input)?.is_not_null())
}

fn in_str_str_out_bool(
    func: impl Fn(expr::Expr, expr::Expr) -> expr::Expr,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> {
    move |input: ScalarFunctionInput| {
        let (arg1, arg2) = input
            .arguments
            .into_iter()
            .map(|expr| cast_to_logical_string_or_try(expr, input.function_context.schema, false))
            .collect::<PlanResult<Vec<_>>>()?
            .two()?;
        Ok(func(arg1, arg2))
    }
}

fn in_str_out_i32(
    func: impl Fn(expr::Expr) -> expr::Expr,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> {
    move |input: ScalarFunctionInput| Ok(cast(func(validate_utf8(input)?), DataType::Int32))
}

fn rev_args(
    func: impl Fn(Vec<expr::Expr>) -> expr::Expr,
) -> impl Fn(Vec<expr::Expr>) -> expr::Expr {
    move |args: Vec<expr::Expr>| func(args.into_iter().rev().collect())
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("ascii", F::custom(ascii)),
        ("base64", F::udf(SparkBase64::new())),
        ("bit_length", F::custom(bit_length)),
        ("btrim", F::var_arg(expr_fn::btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(expr_fn::chr)),
        ("collate", F::unknown("collate")),
        ("collation", F::unknown("collation")),
        ("concat_ws", F::var_arg(concat_ws)),
        ("contains", F::custom(contains)),
        ("decode", F::udf(SparkDecode::new())),
        ("elt", F::udf(SparkElt::new())),
        ("encode", F::udf(SparkEncode::new())),
        ("endswith", F::custom(endswith)),
        ("find_in_set", F::binary(expr_fn::find_in_set)),
        ("format_number", F::unknown("format_number")),
        ("format_string", F::binary(string_fn::format_string)),
        ("initcap", F::unary(expr_fn::initcap)),
        ("instr", F::binary(expr_fn::instr)),
        ("is_valid_utf8", F::custom(is_valid_utf8)),
        ("lcase", F::custom(lower)),
        ("left", F::binary(expr_fn::left)),
        ("len", F::unary(expr_fn::length)),
        ("length", F::unary(expr_fn::length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(lower)),
        ("lpad", F::var_arg(expr_fn::lpad)),
        ("ltrim", F::var_arg(rev_args(expr_fn::ltrim))),
        ("luhn_check", F::unary(string_fn::luhn_check)),
        ("make_valid_utf8", F::udf(MakeValidUtf8::new())),
        ("mask", F::udf(SparkMask::new())),
        ("octet_length", F::custom(octet_length)),
        ("overlay", F::var_arg(overlay)),
        ("position", F::custom(position)),
        ("printf", F::unknown("printf")),
        ("randstr", F::udf(Randstr::new())),
        ("regexp_count", F::udf(RegexpCountFunc::new())),
        ("regexp_extract", F::unknown("regexp_extract")),
        ("regexp_extract_all", F::unknown("regexp_extract_all")),
        ("regexp_instr", F::udf(RegexpInstrFunc::new())),
        ("regexp_replace", F::ternary(regexp_replace)),
        ("regexp_substr", F::unknown("regexp_substr")),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::var_arg(replace)),
        ("right", F::binary(expr_fn::right)),
        ("rpad", F::var_arg(expr_fn::rpad)),
        ("rtrim", F::var_arg(rev_args(expr_fn::rtrim))),
        ("sentences", F::unknown("sentences")),
        ("soundex", F::udf(Soundex::new())),
        ("space", F::unary(space)),
        ("split", F::udf(SparkSplit::new())),
        ("split_part", F::ternary(expr_fn::split_part)),
        ("startswith", F::custom(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::unknown("to_char")),
        ("to_number", F::udf(SparkToNumber::new())),
        ("to_varchar", F::unknown("to_varchar")),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", F::var_arg(rev_args(expr_fn::trim))),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::udf(SparkTryToNumber::new())),
        ("try_validate_utf8", F::custom(try_validate_utf8)),
        ("ucase", F::custom(upper)),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::custom(upper)),
        ("validate_utf8", F::custom(validate_utf8)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}
