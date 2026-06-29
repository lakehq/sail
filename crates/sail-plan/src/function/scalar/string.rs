use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    cast, expr, lit, try_cast, when, BinaryExpr, ExprSchemable, Operator, ScalarUDF,
};
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
use sail_function::scalar::string::spark_binary_string::{PadSide, SparkBinaryPad};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_quote::SparkQuote;
use sail_function::scalar::string::spark_regexp_extract_all::{
    SparkRegexpExtract, SparkRegexpExtractAll,
};
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
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

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn is_binary_expr(arg: &expr::Expr, schema: &DFSchema) -> PlanResult<bool> {
    Ok(is_binary_type(&arg.get_type(schema)?))
}

#[derive(Debug, Clone, Copy)]
enum TrimSide {
    Left,
    Right,
    Both,
}

fn has_binary_expr(args: &[expr::Expr], schema: &DFSchema) -> PlanResult<bool> {
    args.iter()
        .map(|arg| is_binary_expr(arg, schema))
        .try_fold(false, |acc, value| Ok(acc || value?))
}

fn use_binary_pad(args: &[expr::Expr], schema: &DFSchema) -> PlanResult<bool> {
    let Some(value) = args.first() else {
        return Ok(false);
    };
    if !is_binary_expr(value, schema)? {
        return Ok(false);
    }
    match args.len() {
        2 => Ok(true),
        3 => is_binary_expr(&args[2], schema),
        _ => Ok(false),
    }
}

fn lpad(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if use_binary_pad(&arguments, function_context.schema)? {
        return Ok(ScalarUDF::from(SparkBinaryPad::new(PadSide::Left)).call(arguments));
    }
    Ok(expr_fn::lpad(cast_pad_binary_args_to_string(
        arguments,
        function_context.schema,
    )?))
}

fn rpad(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if use_binary_pad(&arguments, function_context.schema)? {
        return Ok(ScalarUDF::from(SparkBinaryPad::new(PadSide::Right)).call(arguments));
    }
    Ok(expr_fn::rpad(cast_pad_binary_args_to_string(
        arguments,
        function_context.schema,
    )?))
}

fn cast_pad_binary_args_to_string(
    args: Vec<expr::Expr>,
    schema: &DFSchema,
) -> PlanResult<Vec<expr::Expr>> {
    args.into_iter()
        .enumerate()
        .map(|(index, arg)| {
            if matches!(index, 0 | 2) && is_binary_expr(&arg, schema)? {
                cast_to_logical_string_or_try(arg, schema, false)
            } else {
                Ok(arg)
            }
        })
        .collect()
}

fn cast_binary_args_to_string(
    args: Vec<expr::Expr>,
    schema: &DFSchema,
) -> PlanResult<Vec<expr::Expr>> {
    if has_binary_expr(&args, schema)? {
        args.into_iter()
            .map(|arg| cast_to_logical_string_or_try(arg, schema, false))
            .collect()
    } else {
        Ok(args)
    }
}

fn btrim(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let args = cast_binary_args_to_string(input.arguments, schema)?;
    Ok(expr_fn::btrim(args))
}

fn trim_with_side(input: ScalarFunctionInput, side: TrimSide) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let args = cast_binary_args_to_string(input.arguments, schema)?
        .into_iter()
        .rev()
        .collect();
    let expr = match side {
        TrimSide::Left => expr_fn::ltrim(args),
        TrimSide::Right => expr_fn::rtrim(args),
        TrimSide::Both => expr_fn::trim(args),
    };
    Ok(expr)
}

fn ltrim(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    trim_with_side(input, TrimSide::Left)
}

fn rtrim(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    trim_with_side(input, TrimSide::Right)
}

fn trim(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    trim_with_side(input, TrimSide::Both)
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

fn decode(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let args = input.arguments;
    if args.len() == 2 {
        return Ok(ScalarUDF::from(SparkDecode::new()).call(args));
    }
    if args.len() < 3 {
        return Err(PlanError::invalid("decode requires at least 2 arguments"));
    }

    let mut args = args.into_iter();
    let expr = args
        .next()
        .ok_or_else(|| PlanError::internal("decode missing expression"))?;
    let mut rest = args.collect::<Vec<_>>();
    let default = if rest.len() % 2 == 1 {
        rest.pop().expect("decode default exists")
    } else {
        lit(ScalarValue::Utf8(None))
    };
    let mut when_then_expr = vec![];
    for pair in rest.chunks_exact(2) {
        let search = pair[0].clone();
        let result = pair[1].clone();
        let condition = expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(expr.clone()),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(search),
        });
        when_then_expr.push((Box::new(condition), Box::new(result)));
    }
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr,
        else_expr: Some(Box::new(default)),
    }))
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
        ("btrim", F::custom(btrim)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(expr_fn::chr)),
        ("collate", F::unknown("collate")),
        ("collation", F::unknown("collation")),
        ("concat_ws", F::udf(SparkConcatWs::new())),
        ("contains", F::custom(contains)),
        ("decode", F::custom(decode)),
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
        ("left", F::binary(expr_fn::left)),
        ("len", F::unary(expr_fn::length)),
        ("length", F::unary(expr_fn::length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(lower)),
        ("lpad", F::custom(lpad)),
        ("ltrim", F::custom(ltrim)),
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
        ("right", F::binary(expr_fn::right)),
        ("rpad", F::custom(rpad)),
        ("rtrim", F::custom(rtrim)),
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
        ("trim", F::custom(trim)),
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
