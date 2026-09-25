use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{ExprSchemable, HigherOrderUDF, ScalarUDF, cast, expr, lit, try_cast, when};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::math::expr_fn as math_fn;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::length::SparkLengthFunc;
use regex_syntax::hir::Look;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::string::format_number::FormatNumber;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_binary_substring::{
    SparkBinaryOverlay, SparkBinarySubstring,
};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_length::{SparkBitLength, SparkOctetLength};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_quote::SparkQuote;
use sail_function::scalar::string::spark_regexp_extract_all::{
    SparkRegexpExtract, SparkRegexpExtractAll,
};
use sail_function::scalar::string::spark_regexp_instr::SparkRegexpInstr;
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_char::SparkToChar;
use sail_function::scalar::string::spark_to_number::SparkToNumber;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::function::scalar::datetime::date_format;
use crate::function::scalar::lambda::lambda_with_fresh_parameter;

fn is_single_capture_extract(pattern: &expr::Expr, replacement: &expr::Expr) -> bool {
    let (expr::Expr::Literal(pattern, _), expr::Expr::Literal(replacement, _)) =
        (pattern, replacement)
    else {
        return false;
    };
    let (Some(pattern), Some("$1")) = (
        pattern.try_as_str().flatten(),
        replacement.try_as_str().flatten(),
    ) else {
        return false;
    };
    if !pattern.starts_with('^') {
        return false;
    }
    let Some(short_pattern) = pattern.strip_suffix(".*$") else {
        return false;
    };

    // Absolute start anchoring makes global and single replacement equivalent.
    regex_syntax::parse(short_pattern).is_ok_and(|pattern| {
        let properties = pattern.properties();
        properties.look_set_prefix().contains(Look::Start)
            && properties.explicit_captures_len() == 1
    })
}

fn regexp_replace(string: expr::Expr, pattern: expr::Expr, replacement: expr::Expr) -> expr::Expr {
    if is_single_capture_extract(&pattern, &replacement) {
        regex_fn::regexp_replace(string, pattern, lit("${1}"), None)
    } else {
        regex_fn::regexp_replace(string, pattern, replacement, Some(lit("g")))
    }
}

/// `RegExpCount.dataType` is `IntegerType` (`regexpExpressions.scala:1106`) and DataFusion's
/// `regexp_count` returns `Int64`. The width is not cosmetic: a BIGINT is a different arithmetic
/// operand, and `DATE + regexp_count(...)` is a date offset only as an INT -- Spark's `DateAdd`
/// refuses a BIGINT. A count of matches cannot leave `Int32`.
fn regexp_count(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let udf = ScalarUDF::from(RegexpCountFunc::new());
    Ok(cast(udf.call(input.arguments), DataType::Int32))
}

/// `RegExpInStr.dataType` is `IntegerType` (`regexpExpressions.scala:1193`), for the same reason as
/// [`regexp_count`]: a position in a string cannot leave `Int32`.
fn regexp_instr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let udf = ScalarUDF::from(RegexpInstrFunc::new());
    Ok(cast(udf.call(input.arguments), DataType::Int32))
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

fn regexp_instr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (string, pattern, index) = match input.arguments.len() {
        2 => {
            let (string, pattern) = input.arguments.two()?;
            (string, pattern, lit(0i32))
        }
        3 => input.arguments.three()?,
        _ => {
            return Err(PlanError::analysis(
                "regexp_instr requires 2 or 3 arguments",
            ));
        }
    };
    let schema = input.function_context.schema;
    for arg in [&string, &pattern] {
        if arg.get_type(schema)?.is_nested() {
            return Err(PlanError::analysis(
                "regexp_instr requires string arguments",
            ));
        }
    }
    let index_type = index.get_type(schema)?;
    if !(index_type.is_numeric() || index_type.is_string() || index_type.is_null()) {
        return Err(PlanError::analysis(
            "regexp_instr requires an integer index",
        ));
    }
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let index = if index_type.is_string() && !ansi_mode {
        ScalarUDF::from(SparkCastStringToInt32::new()).call(vec![index])
    } else {
        // ANSI conversion is deferred until the search arguments are evaluated.
        // Non-ANSI numeric narrowing preserves NULLs, and the converted value is unused.
        index
    };
    Ok(expr::Expr::HigherOrderFunction(
        expr::HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(SparkRegexpInstr::new(
                ansi_mode,
            ))),
            vec![
                cast(string, DataType::Utf8),
                cast(pattern, DataType::Utf8),
                lambda_with_fresh_parameter(index, "_regexp_instr")?,
            ],
        ),
    ))
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
    if matches!(string.get_type(function_context.schema)?, DataType::Binary) {
        let arguments = match length_opt {
            Some(length) => vec![
                string,
                cast(position, DataType::Int64),
                cast(length, DataType::Int64),
            ],
            None => vec![string, cast(position, DataType::Int64)],
        };
        return Ok(ScalarUDF::from(SparkBinarySubstring::new()).call(arguments));
    }
    let string = cast_to_logical_string_or_try(string, function_context.schema, false)?;
    // Spark uses 1-based indexing, but treats pos=0 the same as pos=1 (start of string).
    // For negative positions, Spark counts from the end of the string.
    // DataFusion follows the SQL standard where pos=0 reduces the effective length by 1,
    // and pos<0 reduces even more. We convert Spark's semantics to DataFusion's:
    // - pos > 0: use as-is (1-based from start)
    // - pos = 0: use 1 (same behavior as pos=1 in Spark)
    // - pos < 0: use greatest(char_length(str) + pos + 1, 1) (absolute position from end)
    // For literal positive positions (the common case), we skip the CASE WHEN to keep plans clean.
    let position = match &position {
        expr::Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n > 0 => position,
        expr::Expr::Literal(ScalarValue::Int32(Some(n)), _) if *n > 0 => position,
        expr::Expr::Literal(ScalarValue::Int64(Some(0)), _)
        | expr::Expr::Literal(ScalarValue::Int32(Some(0)), _) => lit(1i64),
        _ => when(position.clone().gt(lit(0i64)), position.clone())
            .when(position.clone().eq(lit(0i64)), lit(1i64))
            .otherwise(expr_fn::greatest(vec![
                cast(expr_fn::char_length(string.clone()), DataType::Int64)
                    + position.clone()
                    + lit(1i64),
                lit(1i64),
            ]))?,
    };
    let substr_res = match length_opt {
        Some(length) => expr_fn::substring(string, position, length),
        None => expr_fn::substr(string, position),
    };
    // TODO: Spark client throws "UNEXPECTED EXCEPTION: ArrowInvalid('Unrecognized type: 24')"
    //  when the return type is Utf8View.
    Ok(cast(substr_res, DataType::Utf8))
}

fn left(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, length) = arguments.two()?;
    if matches!(value.get_type(function_context.schema)?, DataType::Binary) {
        return Ok(ScalarUDF::from(SparkBinarySubstring::new()).call(vec![
            value,
            lit(1_i64),
            cast(length, DataType::Int64),
        ]));
    }
    Ok(expr_fn::left(value, length))
}

// TODO: Spark keeps a BINARY `substr`/`substring`/`left`/`overlay` a BINARY cut by bytes
//  (`stringExpressions.scala:1000-1010,2301-2313,2408`). Sail reads the input as a STRING instead
//  because most of its string functions do not take a BINARY yet, and a BINARY result broke every
//  one of them downstream (`trim(substr(b, 2))`, `hex(substr(b, 2))` over Parquet, ...).
fn overlay(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    if arguments.len() == 4
        && matches!(
            arguments[3],
            expr::Expr::Literal(ScalarValue::Int64(Some(-1)), _)
                | expr::Expr::Literal(ScalarValue::Int32(Some(-1)), _)
        )
    {
        arguments.pop();
    }
    if matches!(
        arguments
            .first()
            .map(|arg| arg.get_type(function_context.schema)),
        Some(Ok(DataType::Binary))
    ) {
        let arguments = arguments
            .into_iter()
            .enumerate()
            .map(|(index, arg)| {
                if index >= 2 {
                    cast(arg, DataType::Int64)
                } else {
                    arg
                }
            })
            .collect();
        return Ok(ScalarUDF::from(SparkBinaryOverlay::new()).call(arguments));
    }
    Ok(expr_fn::overlay(arguments))
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

/// Spark measures the character length of string data and the byte length of binary data,
/// so binary must reach the function as-is. Any other type is measured as its string form,
/// via the Spark-compatible cast rather than the Arrow one, which renders a timestamp with
/// a time zone suffix. Collections have no string form in Spark and are rejected.
fn length_argument(input: ScalarFunctionInput, name: &str) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let data_type = arg.get_type(function_context.schema)?;
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => Ok(arg),
        DataType::FixedSizeBinary(_) => Ok(cast(arg, DataType::Binary)),
        DataType::Null => Ok(cast(arg, DataType::Utf8)),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => Err(PlanError::invalid(format!(
            "`{name}` does not support {data_type} input"
        ))),
        _ => Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])),
    }
}

fn length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = length_argument(input, "length")?;
    Ok(ScalarUDF::from(SparkLengthFunc::new()).call(vec![arg]))
}

fn bit_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = length_argument(input, "bit_length")?;
    Ok(ScalarUDF::from(SparkBitLength::new()).call(vec![arg]))
}

fn octet_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = length_argument(input, "octet_length")?;
    Ok(ScalarUDF::from(SparkOctetLength::new()).call(vec![arg]))
}

fn ascii(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::ascii)(input)
}

fn btrim(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arguments = input
        .arguments
        .into_iter()
        .map(|arg| {
            // Spark implicitly casts either numeric argument to string in both ANSI modes.
            if arg.get_type(input.function_context.schema)?.is_numeric() {
                // TODO: Match Spark's scientific notation for floats and non-ANSI decimals
                //  once the shared numeric formatter supports it.
                Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![arg]))
            } else {
                Ok(arg)
            }
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(expr_fn::btrim(arguments))
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
            let timezone = function_context.plan_config.session_timezone.clone();
            Ok(date_format(value, format, timezone.to_string()))
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
        ("btrim", F::custom(btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::custom(length)),
        ("character_length", F::custom(length)),
        ("chr", F::unary(expr_fn::chr)),
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
        ("left", F::custom(left)),
        ("len", F::custom(length)),
        ("length", F::custom(length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(lower)),
        ("lpad", F::var_arg(expr_fn::lpad)),
        ("ltrim", F::var_arg(rev_args(expr_fn::ltrim))),
        ("luhn_check", F::unary(string_fn::luhn_check)),
        ("make_valid_utf8", F::udf(MakeValidUtf8::new())),
        ("mask", F::udf(SparkMask::new())),
        ("octet_length", F::custom(octet_length)),
        ("overlay", F::custom(overlay)),
        ("position", F::custom(position)),
        ("printf", F::udf(FormatStringFunc::new())),
        ("quote", F::udf(SparkQuote::new())),
        ("randstr", F::udf(Randstr::new())),
        ("regexp_count", F::custom(regexp_count)),
        ("regexp_extract", F::udf(SparkRegexpExtract::new())),
        ("regexp_extract_all", F::udf(SparkRegexpExtractAll::new())),
        ("regexp_instr", F::custom(regexp_instr)),
        ("regexp_replace", F::ternary(regexp_replace)),
        ("regexp_substr", F::custom(regexp_substr)),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::var_arg(replace)),
        ("right", F::binary(expr_fn::right)),
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
