use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{cast, expr, lit, try_cast, when, ExprSchemable, ScalarUDF};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::math::expr_fn as math_fn;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::string::format_number::FormatNumber;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_quote::SparkQuote;
use sail_function::scalar::string::spark_regexp_extract_all::{
    SparkRegexpExtract, SparkRegexpExtractAll,
};
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_substr_binary::SparkSubstrBinary;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_char::SparkToChar;
use sail_function::scalar::string::spark_to_number::SparkToNumber;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::function::scalar::datetime::date_format;

fn regexp_replace(string: expr::Expr, pattern: expr::Expr, replacement: expr::Expr) -> expr::Expr {
    regex_fn::regexp_replace(string, pattern, replacement, Some(lit("g")))
}

fn regexp_substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (string, pattern) = input
        .arguments
        .two()
        .map_err(|_| PlanError::invalid("regexp_substr requires 2 arguments"))?;
    let wrapped_pattern = expr_fn::concat_ws(lit(""), vec![lit("("), pattern, lit(")")]);
    let matches = regex_fn::regexp_match(string, wrapped_pattern, None);
    Ok(array_element(matches, lit(1i64)))
}

/// Extracts the integer value from a non-null integer literal expression.
fn literal_as_i64(e: &expr::Expr) -> Option<i64> {
    match e {
        expr::Expr::Literal(ScalarValue::Int8(Some(n)), _) => Some(i64::from(*n)),
        expr::Expr::Literal(ScalarValue::Int16(Some(n)), _) => Some(i64::from(*n)),
        expr::Expr::Literal(ScalarValue::Int32(Some(n)), _) => Some(i64::from(*n)),
        expr::Expr::Literal(ScalarValue::Int64(Some(n)), _) => Some(*n),
        _ => None,
    }
}

fn adjust_substr_position(
    string_len: expr::Expr,
    position: expr::Expr,
    length_opt: Option<expr::Expr>,
) -> PlanResult<(expr::Expr, Option<expr::Expr>)> {
    match &position {
        expr::Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n > 0 => Ok((position, length_opt)),
        expr::Expr::Literal(ScalarValue::Int32(Some(n)), _) if *n > 0 => Ok((position, length_opt)),
        expr::Expr::Literal(ScalarValue::Int64(Some(0)), _)
        | expr::Expr::Literal(ScalarValue::Int32(Some(0)), _) => Ok((lit(1i64), length_opt)),
        _ => {
            let effective_start = when(position.clone().gt(lit(0i64)), position.clone())
                .when(position.clone().eq(lit(0i64)), lit(1i64))
                .otherwise(cast(string_len, DataType::Int64) + position.clone() + lit(1i64))?;
            let clamped = expr_fn::greatest(vec![effective_start.clone(), lit(1i64)]);
            let length_opt = match length_opt {
                Some(length) => Some(
                    when(
                        effective_start.clone().lt(lit(1i64)),
                        expr_fn::greatest(vec![
                            length.clone() + effective_start - lit(1i64),
                            lit(0i64),
                        ]),
                    )
                    .otherwise(length)?,
                ),
                None => None,
            };
            Ok((clamped, length_opt))
        }
    }
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

    let string_type = string.get_type(function_context.schema)?;
    if matches!(string_type, DataType::Binary | DataType::LargeBinary) {
        // Cast position to Int64: coerces FLOAT/DOUBLE/STRING and all integer subtypes.
        let pos_i64 = cast(position, DataType::Int64);
        // SparkSubstrBinary handles all Spark position semantics internally (1-based, pos=0,
        // negative pos from end, overshoot adjustment, negative length, NULL propagation).
        let binary = cast(string, DataType::Binary);
        let length_opt = length_opt.map(|l| cast(l, DataType::Int64));
        let udf = ScalarUDF::from(SparkSubstrBinary::new());
        let args = match length_opt {
            Some(length) => vec![binary, pos_i64, length],
            None => vec![binary, pos_i64],
        };
        return Ok(udf.call(args));
    }

    let string = cast_to_logical_string_or_try(string, function_context.schema, false)?;

    // Fast path: constant non-null positive position (and non-negative length if present).
    // adjust_substr_position is a no-op for pos >= 1, negative length can't happen with a
    // literal >= 0, and a non-null literal never needs a NULL guard. Avoids generating a
    // CASE WHEN tree for the common case (e.g. substr(col, 1, 30) in TPC-DS queries).
    if let Some(pos_val) = literal_as_i64(&position) {
        if pos_val >= 1 {
            match length_opt {
                None => return Ok(cast(expr_fn::substr(string, position), DataType::Utf8)),
                Some(ref len_expr) if literal_as_i64(len_expr).map_or(false, |l| l >= 0) => {
                    return Ok(cast(
                        expr_fn::substring(string, position, length_opt.unwrap()),
                        DataType::Utf8,
                    ));
                }
                _ => {}
            }
        }
    }

    // Save original position before casting, for the NULL-propagation guard below.
    let original_position = position.clone();

    // Cast position to Int64: coerces FLOAT/DOUBLE/STRING (truncating toward zero) and
    // narrows all integer subtypes. BIGINT overflow (value > INT32_MAX) is a separate
    // known limitation; DataFusion does not raise CAST_OVERFLOW on the Int64 cast path.
    let position = cast(position, DataType::Int64);

    // Clamp negative length to 0 — Spark returns empty string, DataFusion raises an error.
    // Cast to Int64 first to ensure consistent types in the CASE WHEN expression.
    let length_opt = length_opt.map(|l| {
        let l_i64 = cast(l, DataType::Int64);
        when(l_i64.clone().lt(lit(0i64)), lit(0i64))
            .otherwise(l_i64)
            .unwrap_or_else(|_| lit(0i64))
    });

    // Spark uses 1-based indexing, treating pos=0 as pos=1 and negative pos as
    // counting from the end. For the 3-arg form, when pos overshoots the start
    // (effective_start < 1), the length must be reduced by the overshoot so the
    // endpoint stays correct — otherwise we'd return too many characters.
    let string_len = expr_fn::char_length(string.clone());
    let (position, length_opt) = adjust_substr_position(string_len, position, length_opt)?;
    let substr_res = match length_opt {
        Some(length) => expr_fn::substring(string, position, length),
        None => expr_fn::substr(string, position),
    };

    // NULL position must propagate to NULL result. greatest(NULL, 1) returns 1 in DataFusion
    // (matches Spark's greatest semantics), so we must guard explicitly.
    let result =
        when(original_position.is_null(), lit(ScalarValue::Utf8(None))).otherwise(substr_res)?;

    // TODO: Spark client throws "UNEXPECTED EXCEPTION: ArrowInvalid('Unrecognized type: 24')"
    //  when the return type is Utf8View.
    Ok(cast(result, DataType::Utf8))
}

/// Spark `left(string, n)`: returns the leftmost n characters.
/// For negative n, Spark returns empty string. DataFusion removes from the end.
fn spark_left(string: expr::Expr, length: expr::Expr) -> expr::Expr {
    let len = cast(length, DataType::Int64);
    when(len.clone().lt(lit(0i64)), lit(""))
        .otherwise(expr_fn::left(string, len))
        .unwrap_or_else(|_| lit(""))
}

/// Spark `right(string, n)`: returns the rightmost n characters.
/// For negative n, Spark returns empty string. DataFusion removes from the start.
fn spark_right(string: expr::Expr, length: expr::Expr) -> expr::Expr {
    let len = cast(length, DataType::Int64);
    when(len.clone().lt(lit(0i64)), lit(""))
        .otherwise(expr_fn::right(string, len))
        .unwrap_or_else(|_| lit(""))
}

/// Spark `char(n)` / `chr(n)`: returns the character for the given codepoint.
/// For invalid codepoints (negative or > max unicode), Spark returns empty string.
/// DataFusion throws an error for invalid codepoints.
fn spark_chr(codepoint: expr::Expr) -> expr::Expr {
    let cp = cast(codepoint, DataType::Int64);
    when(
        cp.clone().lt(lit(1i64)).or(cp.clone().gt(lit(0x10FFFFi64))),
        lit(""),
    )
    .otherwise(expr_fn::chr(cp))
    .unwrap_or_else(|_| lit(""))
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
            when(start.clone().lt_eq(lit(0)), lit(0))
                .when(pos.clone().eq(lit(0)), lit(0))
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

/// Dispatch for `to_char(expr, format)` and its alias `to_varchar`, following Spark's
/// `ToCharacterBuilder`: datetime input formats like `date_format`, binary input is
/// converted to a base64, hexadecimal, or UTF-8 string, and any other input is
/// formatted as a decimal value according to a number format.
fn to_char(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, format) = arguments
        .two()
        .map_err(|_| PlanError::invalid("to_char requires 2 arguments"))?;
    match value.get_type(function_context.schema)? {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            Ok(date_format(value, format))
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            // Spark requires a foldable format for binary input since the format
            // determines the conversion function.
            let expr::Expr::Literal(scalar, _) = &format else {
                return Err(PlanError::invalid(
                    "to_char: the `format` parameter must be a string literal for binary input",
                ));
            };
            match scalar.try_as_str() {
                Some(Some(name)) => match name.trim().to_lowercase().as_str() {
                    "base64" => Ok(ScalarUDF::from(SparkBase64::new()).call(vec![value])),
                    "hex" => Ok(math_fn::hex(value)),
                    "utf-8" => Ok(ScalarUDF::from(SparkDecode::new()).call(vec![value, format])),
                    invalid => Err(PlanError::invalid(format!(
                        "to_char: the value of the `format` parameter expects one of binary formats 'base64', 'hex', 'utf-8', but got '{invalid}'"
                    ))),
                },
                Some(None) => Err(PlanError::invalid(
                    "to_char: the `format` parameter expects a non-NULL value for binary input",
                )),
                None => Err(PlanError::invalid(
                    "to_char: the `format` parameter must be a string literal for binary input",
                )),
            }
        }
        _ => {
            let ansi_mode = function_context.plan_config.ansi_mode;
            Ok(ScalarUDF::from(SparkToChar::new(ansi_mode)).call(vec![value, format]))
        }
    }
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("ascii", F::custom(ascii)),
        ("base64", F::udf(SparkBase64::new())),
        ("bit_length", F::custom(bit_length)),
        ("btrim", F::var_arg(expr_fn::btrim)),
        ("char", F::unary(spark_chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(spark_chr)),
        ("collate", F::unknown("collate")),
        ("collation", F::unknown("collation")),
        ("concat_ws", F::udf(SparkConcatWs::new())),
        ("contains", F::custom(contains)),
        ("decode", F::udf(SparkDecode::new())),
        ("elt", F::udf(SparkElt::new())),
        ("encode", F::udf(SparkEncode::new())),
        ("endswith", F::custom(endswith)),
        ("find_in_set", F::binary(expr_fn::find_in_set)),
        ("format_number", F::udf(FormatNumber::new())),
        ("format_string", F::udf(FormatStringFunc::new())),
        ("initcap", F::unary(expr_fn::initcap)),
        ("instr", F::binary(expr_fn::instr)),
        ("is_valid_utf8", F::custom(is_valid_utf8)),
        ("lcase", F::custom(lower)),
        ("left", F::binary(spark_left)),
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
        ("printf", F::udf(FormatStringFunc::new())),
        ("quote", F::udf(SparkQuote::new())),
        ("randstr", F::udf(Randstr::new())),
        ("regexp_count", F::udf(RegexpCountFunc::new())),
        ("regexp_extract", F::udf(SparkRegexpExtract::new())),
        ("regexp_extract_all", F::udf(SparkRegexpExtractAll::new())),
        ("regexp_instr", F::udf(RegexpInstrFunc::new())),
        ("regexp_replace", F::ternary(regexp_replace)),
        ("regexp_substr", F::custom(regexp_substr)),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::var_arg(replace)),
        ("right", F::binary(spark_right)),
        ("rpad", F::var_arg(expr_fn::rpad)),
        ("rtrim", F::var_arg(rev_args(expr_fn::rtrim))),
        ("sentences", F::udf(SparkSentences::new())),
        ("soundex", F::udf(Soundex::new())),
        ("space", F::unary(space)),
        ("split", F::udf(SparkSplit::new())),
        ("split_part", F::ternary(expr_fn::split_part)),
        ("startswith", F::custom(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::custom(to_char)),
        ("to_number", F::udf(SparkToNumber::new(false))),
        ("to_varchar", F::custom(to_char)),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", F::var_arg(rev_args(expr_fn::trim))),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::udf(SparkToNumber::new(true))),
        ("try_validate_utf8", F::custom(try_validate_utf8)),
        ("ucase", F::custom(upper)),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::custom(upper)),
        ("validate_utf8", F::custom(validate_utf8)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}
