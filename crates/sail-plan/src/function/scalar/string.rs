use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, ScalarUDFImpl, cast, expr, lit, try_cast, when};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::math::expr_fn as math_fn;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::length::SparkLengthFunc;
use regex_syntax::hir::Look;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_timezone_cast::SparkTimezoneCast;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::string::format_number::FormatNumber;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_length::{SparkBitLength, SparkOctetLength};
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

fn stringify_ltz(
    expression: expr::Expr,
    schema: &DFSchema,
    session_timezone: &Arc<str>,
) -> PlanResult<expr::Expr> {
    if matches!(
        expression.get_type(schema)?,
        DataType::Timestamp(_, Some(_))
    ) {
        Ok(ScalarUDF::from(SparkToUtf8::new(Arc::clone(session_timezone))).call(vec![expression]))
    } else {
        Ok(expression)
    }
}

fn stringify_ltz_indices(
    mut arguments: Vec<expr::Expr>,
    indices: impl IntoIterator<Item = usize>,
    schema: &DFSchema,
    session_timezone: &Arc<str>,
) -> PlanResult<Vec<expr::Expr>> {
    for index in indices {
        if index < arguments.len() {
            arguments[index] = stringify_ltz(arguments[index].clone(), schema, session_timezone)?;
        }
    }
    Ok(arguments)
}

pub(super) fn with_ltz_string_arguments(
    input: ScalarFunctionInput,
    indices: impl IntoIterator<Item = usize>,
    build: impl FnOnce(Vec<expr::Expr>) -> PlanResult<expr::Expr>,
) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = stringify_ltz_indices(
        arguments,
        indices,
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    build(arguments)
}

fn string_arguments(
    indices: &'static [usize],
    build: impl Fn(Vec<expr::Expr>) -> PlanResult<expr::Expr> + Send + Sync + 'static,
) -> ScalarFunction {
    Arc::new(move |input| {
        with_ltz_string_arguments(input, indices.iter().copied(), |arguments| build(arguments))
    })
}

fn all_string_arguments(
    build: impl Fn(Vec<expr::Expr>) -> PlanResult<expr::Expr> + Send + Sync + 'static,
) -> ScalarFunction {
    Arc::new(move |input| {
        let count = input.arguments.len();
        with_ltz_string_arguments(input, 0..count, |arguments| build(arguments))
    })
}

fn string_udf(
    indices: &'static [usize],
    udf: impl ScalarUDFImpl + Send + Sync + 'static,
) -> ScalarFunction {
    let udf = ScalarUDF::from(udf);
    string_arguments(indices, move |arguments| Ok(udf.call(arguments)))
}

fn all_string_udf(udf: impl ScalarUDFImpl + Send + Sync + 'static) -> ScalarFunction {
    let udf = ScalarUDF::from(udf);
    all_string_arguments(move |arguments| Ok(udf.call(arguments)))
}

fn list_with_string_items(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::List(field) => Some(DataType::List(Arc::new(
            field.as_ref().clone().with_data_type(DataType::Utf8),
        ))),
        DataType::LargeList(field) => Some(DataType::LargeList(Arc::new(
            field.as_ref().clone().with_data_type(DataType::Utf8),
        ))),
        _ => None,
    }
}

fn contains_ltz(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, Some(_)) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => contains_ltz(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_ltz(field.data_type())),
        DataType::Map(field, _) => contains_ltz(field.data_type()),
        _ => false,
    }
}

fn concat_ws(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = arguments
        .into_iter()
        .map(|argument| {
            let data_type = argument.get_type(function_context.schema)?;
            if matches!(data_type, DataType::Timestamp(_, Some(_))) {
                stringify_ltz(
                    argument,
                    function_context.schema,
                    &function_context.plan_config.session_timezone,
                )
            } else if contains_ltz(&data_type)
                && let Some(target_type) = list_with_string_items(&data_type)
            {
                Ok(ScalarUDF::from(SparkTimezoneCast::new(
                    target_type,
                    function_context.plan_config.session_timezone.clone(),
                    false,
                ))
                .call(vec![argument]))
            } else {
                Ok(argument)
            }
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(ScalarUDF::from(SparkConcatWs::new()).call(arguments))
}

fn elt(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let indices = 1..arguments.len();
    let arguments = stringify_ltz_indices(
        arguments,
        indices,
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    Ok(ScalarUDF::from(SparkElt::new()).call(arguments))
}

fn format_string(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = stringify_ltz_indices(
        arguments,
        [0],
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    Ok(ScalarUDF::from(FormatStringFunc::new()).call(arguments))
}

fn regexp_replace(string: expr::Expr, pattern: expr::Expr, replacement: expr::Expr) -> expr::Expr {
    if is_single_capture_extract(&pattern, &replacement) {
        regex_fn::regexp_replace(string, pattern, lit("${1}"), None)
    } else {
        regex_fn::regexp_replace(string, pattern, replacement, Some(lit("g")))
    }
}

fn regexp_substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = stringify_ltz_indices(
        arguments,
        [0, 1],
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    let (string, pattern) = arguments
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
    let string = cast_to_logical_string_or_try(
        string,
        function_context.schema,
        false,
        &function_context.plan_config.session_timezone,
    )?;
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
        .map(|expr| {
            cast_to_logical_string_or_try(
                expr,
                function_context.schema,
                false,
                &function_context.plan_config.session_timezone,
            )
        })
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
        _ => Ok(ScalarUDF::from(SparkToUtf8::new(
            function_context.plan_config.session_timezone.clone(),
        ))
        .call(vec![arg])),
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

fn cast_to_logical_string_or_try(
    arg: expr::Expr,
    schema: &DFSchema,
    is_try: bool,
    session_timezone: &Arc<str>,
) -> PlanResult<expr::Expr> {
    if matches!(arg.get_type(schema)?, DataType::Timestamp(_, Some(_))) {
        return Ok(ScalarUDF::from(SparkToUtf8::new(Arc::clone(session_timezone))).call(vec![arg]));
    }
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
        &input.function_context.plan_config.session_timezone,
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
            .map(|expr| {
                cast_to_logical_string_or_try(
                    expr,
                    input.function_context.schema,
                    false,
                    &input.function_context.plan_config.session_timezone,
                )
            })
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
        (
            "btrim",
            all_string_arguments(|args| Ok(expr_fn::btrim(args))),
        ),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::custom(length)),
        ("character_length", F::custom(length)),
        ("chr", F::unary(expr_fn::chr)),
        ("collate", F::unknown("collate")),
        ("collation", F::unknown("collation")),
        ("concat_ws", F::custom(concat_ws)),
        ("contains", F::custom(contains)),
        ("decode", F::udf(SparkDecode::new())),
        ("elt", F::custom(elt)),
        ("encode", string_udf(&[0, 1], SparkEncode::new())),
        ("endswith", F::custom(endswith)),
        (
            "find_in_set",
            string_arguments(&[0, 1], |arguments| {
                let (left, right) = arguments.two()?;
                Ok(expr_fn::find_in_set(left, right))
            }),
        ),
        ("format_number", F::udf(FormatNumber::new())),
        ("format_string", F::custom(format_string)),
        (
            "initcap",
            F::custom(|input| Ok(expr_fn::initcap(validate_utf8(input)?))),
        ),
        (
            "instr",
            string_arguments(&[0, 1], |arguments| {
                let (left, right) = arguments.two()?;
                Ok(expr_fn::instr(left, right))
            }),
        ),
        ("is_valid_utf8", F::custom(is_valid_utf8)),
        ("lcase", F::custom(lower)),
        (
            "left",
            string_arguments(&[0], |arguments| {
                let (string, length) = arguments.two()?;
                Ok(expr_fn::left(string, length))
            }),
        ),
        ("len", F::custom(length)),
        ("length", F::custom(length)),
        ("levenshtein", string_udf(&[0, 1], Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(lower)),
        (
            "lpad",
            string_arguments(&[0, 2], |args| Ok(expr_fn::lpad(args))),
        ),
        (
            "ltrim",
            all_string_arguments(|args| Ok(rev_args(expr_fn::ltrim)(args))),
        ),
        (
            "luhn_check",
            string_arguments(&[0], |arguments| {
                Ok(string_fn::luhn_check(arguments.one()?))
            }),
        ),
        ("make_valid_utf8", string_udf(&[0], MakeValidUtf8::new())),
        ("mask", string_udf(&[0], SparkMask::new())),
        ("octet_length", F::custom(octet_length)),
        ("overlay", string_arguments(&[0, 1], overlay)),
        ("position", F::custom(position)),
        ("printf", F::custom(format_string)),
        ("quote", string_udf(&[0], SparkQuote::new())),
        ("randstr", F::udf(Randstr::new())),
        ("regexp_count", string_udf(&[0, 1], RegexpCountFunc::new())),
        (
            "regexp_extract",
            string_udf(&[0, 1], SparkRegexpExtract::new()),
        ),
        (
            "regexp_extract_all",
            string_udf(&[0, 1], SparkRegexpExtractAll::new()),
        ),
        ("regexp_instr", string_udf(&[0, 1], RegexpInstrFunc::new())),
        (
            "regexp_replace",
            string_arguments(&[0, 1, 2], |arguments| {
                let (string, pattern, replacement) = arguments.three()?;
                Ok(regexp_replace(string, pattern, replacement))
            }),
        ),
        ("regexp_substr", F::custom(regexp_substr)),
        (
            "repeat",
            string_arguments(&[0], |arguments| {
                let (string, count) = arguments.two()?;
                Ok(expr_fn::repeat(string, count))
            }),
        ),
        ("replace", all_string_arguments(replace)),
        (
            "right",
            string_arguments(&[0], |arguments| {
                let (string, length) = arguments.two()?;
                Ok(expr_fn::right(string, length))
            }),
        ),
        (
            "rpad",
            string_arguments(&[0, 2], |args| Ok(expr_fn::rpad(args))),
        ),
        (
            "rtrim",
            all_string_arguments(|args| Ok(rev_args(expr_fn::rtrim)(args))),
        ),
        ("sentences", all_string_udf(SparkSentences::new())),
        ("soundex", string_udf(&[0], Soundex::new())),
        ("space", F::unary(space)),
        ("split", string_udf(&[0, 1], SparkSplit::new())),
        (
            "split_part",
            string_arguments(&[0, 1], |arguments| {
                let (string, delimiter, index) = arguments.three()?;
                Ok(expr_fn::split_part(string, delimiter, index))
            }),
        ),
        ("startswith", F::custom(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        (
            "substring_index",
            string_arguments(&[0, 1], |arguments| {
                let (string, delimiter, count) = arguments.three()?;
                Ok(expr_fn::substr_index(string, delimiter, count))
            }),
        ),
        ("to_binary", all_string_udf(SparkToBinary::new())),
        ("to_char", F::custom(to_char)),
        ("to_number", string_udf(&[0, 1], SparkToNumber::new(false))),
        ("to_varchar", F::custom(to_char)),
        (
            "translate",
            string_arguments(&[0, 1, 2], |arguments| {
                let (string, from, to) = arguments.three()?;
                Ok(expr_fn::translate(string, from, to))
            }),
        ),
        (
            "trim",
            all_string_arguments(|args| Ok(rev_args(expr_fn::trim)(args))),
        ),
        ("try_to_binary", all_string_udf(SparkTryToBinary::new())),
        (
            "try_to_number",
            string_udf(&[0, 1], SparkToNumber::new(true)),
        ),
        ("try_validate_utf8", F::custom(try_validate_utf8)),
        ("ucase", F::custom(upper)),
        ("unbase64", string_udf(&[0], SparkUnbase64::new())),
        ("upper", F::custom(upper)),
        ("validate_utf8", F::custom(validate_utf8)),
        (
            "strpos",
            string_arguments(&[0, 1], |arguments| {
                let (string, substring) = arguments.two()?;
                Ok(expr_fn::strpos(string, substring))
            }),
        ),
    ]
}
