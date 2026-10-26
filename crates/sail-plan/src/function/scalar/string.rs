use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions::{core, regex, string, unicode};
use datafusion::functions_nested;
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF, ValueOrLambda};
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::length::SparkLengthFunc;
use regex_syntax::hir::Look;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
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
use sail_function::scalar::string::spark_regexp_instr::SparkRegexpInstr;
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_char::SparkToChar;
use sail_function::scalar::string::spark_to_number::SparkToNumber;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn regexp_replace(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, pattern, replacement) = values.three()?;
    let single = match (&pattern.kind, &replacement.kind) {
        (
            Deferred::Resolved(ExprKind::Literal(pattern)),
            Deferred::Resolved(ExprKind::Literal(replacement)),
        ) => {
            match (
                pattern.try_as_str().flatten(),
                replacement.try_as_str().flatten(),
            ) {
                (Some(pattern), Some("$1")) if pattern.starts_with('^') => {
                    pattern.strip_suffix(".*$").is_some_and(|p| {
                        regex_syntax::parse(p).is_ok_and(|p| {
                            p.properties().look_set_prefix().contains(Look::Start)
                                && p.properties().explicit_captures_len() == 1
                        })
                    })
                }
                _ => false,
            }
        }
        _ => false,
    };
    let args = if single {
        vec![
            value,
            pattern,
            context.literal(ScalarValue::Utf8(Some("${1}".into()))),
        ]
    } else {
        vec![
            value,
            pattern,
            replacement,
            context.literal(ScalarValue::Utf8(Some("g".into()))),
        ]
    };
    context.scalar_udf(regex::regexp_replace(), args)
}

fn regexp_substr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, pattern) = values.two()?;
    let empty = context.literal(ScalarValue::Utf8(Some("".into())));
    let open = context.literal(ScalarValue::Utf8(Some("(".into())));
    let close = context.literal(ScalarValue::Utf8(Some(")".into())));
    let pattern = context.scalar_udf(string::concat_ws(), vec![empty, open, pattern, close])?;
    let matches = context.scalar_udf(regex::regexp_match(), vec![value, pattern])?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    context.scalar_udf(
        functions_nested::extract::array_element_udf(),
        vec![matches, one],
    )
}

fn regexp_instr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    if values.len() == 2 {
        values.push(context.literal(ScalarValue::Int32(Some(0))));
    }
    let (string, pattern, index) = values.three()?;
    if string
        .result_type
        .resolved("regexp string")?
        .data_type
        .is_nested()
        || pattern
            .result_type
            .resolved("regexp pattern")?
            .data_type
            .is_nested()
    {
        return Err(PlanError::analysis(
            "regexp_instr requires string arguments",
        ));
    }
    let kind = &index.result_type.resolved("regexp index")?.data_type;
    if !(kind.is_numeric() || kind.is_string() || kind.is_null()) {
        return Err(PlanError::analysis(
            "regexp_instr requires an integer index",
        ));
    }
    let index = if kind.is_string() && !context.config.ansi_mode {
        context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkCastStringToInt32::new())),
            vec![index],
        )?
    } else {
        index
    };
    let string = context.cast(string, DataType::Utf8)?;
    let pattern = context.cast(pattern, DataType::Utf8)?;
    let function = Arc::new(HigherOrderUDF::new_from_impl(SparkRegexpInstr::new(
        context.config.ansi_mode,
    )));
    crate::function::scalar::lambda::mixed_lazy_arguments(
        context,
        function,
        vec![
            ValueOrLambda::Value(string),
            ValueOrLambda::Value(pattern),
            ValueOrLambda::Lambda(index),
        ],
    )
}

fn substr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    let length = if values.len() == 3 {
        values.pop()
    } else {
        None
    };
    let (string, position) = values.two()?;
    let string = logical_string(context, string, false)?;
    let position = match position.kind.resolved("substring position")? {
        ExprKind::Literal(ScalarValue::Int32(Some(n))) if *n > 0 => position,
        ExprKind::Literal(ScalarValue::Int64(Some(n))) if *n > 0 => position,
        ExprKind::Literal(ScalarValue::Int32(Some(0)) | ScalarValue::Int64(Some(0))) => {
            context.literal(ScalarValue::Int64(Some(1)))
        }
        _ => {
            let copy = context.copy_expression(&position)?;
            let zero = context.literal(ScalarValue::Int64(Some(0)));
            let positive = context.binary(copy, Operator::Gt, zero)?;
            let copy = context.copy_expression(&position)?;
            let zero = context.literal(ScalarValue::Int64(Some(0)));
            let at_start = context.binary(copy, Operator::Eq, zero)?;
            let copy = context.copy_expression(&string)?;
            let length = context.scalar_udf(unicode::character_length(), vec![copy])?;
            let length = context.cast(length, DataType::Int64)?;
            let copy = context.copy_expression(&position)?;
            let offset = context.binary(length, Operator::Plus, copy)?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            let offset = context.binary(offset, Operator::Plus, one)?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            let offset = context.scalar_udf(core::greatest(), vec![offset, one])?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            context.case(vec![(positive, position), (at_start, one)], Some(offset))?
        }
    };
    let mut arguments = vec![string, position];
    if let Some(length) = length {
        arguments.push(length);
    }
    let result = context.scalar_udf(unicode::substr(), arguments)?;
    context.cast(result, DataType::Utf8)
}

fn overlay(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    if values.len() == 4
        && matches!(
            values[3].kind,
            Deferred::Resolved(ExprKind::Literal(
                ScalarValue::Int32(Some(-1)) | ScalarValue::Int64(Some(-1))
            ))
        )
    {
        values.pop();
    }
    context.scalar_udf(core::overlay(), values)
}

fn position(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    let start = if values.len() == 3 {
        values.pop()
    } else {
        None
    };
    let (needle, string) = values.two()?;
    let needle = logical_string(context, needle, false)?;
    let string = logical_string(context, string, false)?;
    let Some(start) = start else {
        return context.scalar_udf(unicode::strpos(), vec![string, needle]);
    };
    let copy = context.copy_expression(&start)?;
    let string = context.scalar_udf(unicode::substr(), vec![string, copy])?;
    let position = context.scalar_udf(unicode::strpos(), vec![string, needle])?;
    let copy = context.copy_expression(&start)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let invalid = context.binary(copy, Operator::LtEq, zero)?;
    let copy = context.copy_expression(&position)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let missing = context.binary(copy, Operator::Eq, zero)?;
    let copy = context.copy_expression(&position)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let found = context.binary(copy, Operator::Gt, zero)?;
    let position = context.binary(start, Operator::Plus, position)?;
    let one = context.literal(ScalarValue::Int32(Some(1)));
    let position = context.binary(position, Operator::Minus, one)?;
    let zero_a = context.literal(ScalarValue::Int32(Some(0)));
    let zero_b = context.literal(ScalarValue::Int32(Some(0)));
    context.case(
        vec![(invalid, zero_a), (missing, zero_b), (found, position)],
        None,
    )
}

fn space(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let space = context.literal(ScalarValue::Utf8(Some(" ".into())));
    context.scalar_udf(string::repeat(), vec![space, value])
}

fn replace(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    if values.len() == 2 {
        values.push(context.literal(ScalarValue::Utf8(Some("".into()))));
    }
    crate::analyzer::typing::coercion::validate_arity(name, values.len(), 3)?;
    context.scalar_udf(string::replace(), values)
}

fn lower(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = logical_string(context, values.one()?, false)?;
    context.scalar_udf(
        if matches!(name, "lower" | "lcase") {
            string::lower()
        } else {
            string::upper()
        },
        vec![value],
    )
}

fn startswith(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (left, right) = values.two()?;
    let left = logical_string(context, left, false)?;
    let right = logical_string(context, right, false)?;
    context.scalar_udf(
        match name {
            "startswith" => string::starts_with(),
            "endswith" => string::ends_with(),
            _ => string::contains(),
        },
        vec![left, right],
    )
}

fn length(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let data_type = value
        .result_type
        .resolved("length argument")?
        .data_type
        .clone();
    let value = match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => value,
        DataType::FixedSizeBinary(_) => context.cast(value, DataType::Binary)?,
        DataType::Null => context.cast(value, DataType::Utf8)?,
        data_type if data_type.is_nested() => {
            return Err(PlanError::invalid(format!(
                "`{name}` does not support {data_type} input"
            )));
        }
        _ => context.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), vec![value])?,
    };
    let function = match name {
        "bit_length" => ScalarUDF::from(SparkBitLength::new()),
        "octet_length" => ScalarUDF::from(SparkOctetLength::new()),
        _ => ScalarUDF::from(SparkLengthFunc::new()),
    };
    context.scalar_udf(Arc::new(function), vec![value])
}

fn ascii(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = logical_string(context, values.one()?, false)?;
    let value = context.scalar_udf(string::ascii(), vec![value])?;
    context.cast(value, DataType::Int32)
}

fn btrim(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let values = values
        .into_iter()
        .map(|v| {
            if v.result_type
                .resolved("trim argument")?
                .data_type
                .is_numeric()
            {
                context.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), vec![v])
            } else {
                Ok(v)
            }
        })
        .collect::<PlanResult<Vec<_>>>()?;
    context.scalar_udf(string::btrim(), values)
}

fn validate_utf8(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = logical_string(context, values.one()?, name != "validate_utf8")?;
    Ok(if name == "is_valid_utf8" {
        context.is_not_null(value)
    } else {
        value
    })
}

fn to_char(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, format) = values.two()?;
    let kind = &value.result_type.resolved("to_char input")?.data_type;
    Ok(
        if matches!(
            kind,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
        ) {
            context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkDateFormat::new(
                    context.config.session_timezone.clone(),
                ))),
                vec![value, format],
            )?
        } else if kind.is_binary() {
            let ExprKind::Literal(literal) = format.kind.resolved("to_char format")? else {
                return Err(PlanError::invalid(
                    "to_char: the `format` parameter must be a string literal for binary input",
                ));
            };
            let Some(Some(name)) = literal.try_as_str() else {
                return Err(PlanError::invalid(
                    "to_char: the `format` parameter must be a non-NULL string literal for binary input",
                ));
            };
            match name.trim().to_lowercase().as_str() {
                "base64" => context
                    .scalar_udf(Arc::new(ScalarUDF::from(SparkBase64::new())), vec![value])?,
                "hex" => {
                    context.scalar_udf(datafusion_spark::function::math::hex(), vec![value])?
                }
                "utf-8" => context.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkDecode::new())),
                    vec![value, format],
                )?,
                invalid => {
                    return Err(PlanError::invalid(format!(
                        "to_char: the value of the `format` parameter expects one of binary formats 'base64', 'hex', 'utf-8', but got '{invalid}'"
                    )));
                }
            }
        } else {
            context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkToChar::new(context.config.ansi_mode))),
                vec![value, format],
            )?
        },
    )
}

fn resolve_char(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar_udf(string::chr(), vec![values.one()?])
}

fn resolve_trim(mut input: ScalarFunctionInput<'_>, function: Arc<ScalarUDF>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    values.reverse();
    context.scalar_udf(function, values)
}

fn resolve_substring_index(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (a, b, c) = values.three()?;
    context.scalar_udf(unicode::substr_index(), vec![a, b, c])
}

pub(crate) fn logical_string(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    safe: bool,
) -> PlanResult<Expr> {
    let target = match value.result_type.resolved("string input")?.data_type {
        DataType::LargeBinary | DataType::LargeUtf8 => DataType::LargeUtf8,
        DataType::Utf8View => DataType::Utf8View,
        _ => DataType::Utf8,
    };
    if safe {
        context.try_cast(value, target)
    } else {
        context.cast(value, target)
    }
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("ascii", F::custom(ascii)),
        ("base64", F::udf(SparkBase64::new())),
        ("bit_length", F::custom(|input| length(input, "bit_length"))),
        ("btrim", F::custom(btrim)),
        ("char", F::custom(resolve_char)),
        (
            "char_length",
            F::custom(|input| length(input, "char_length")),
        ),
        (
            "character_length",
            F::custom(|input| length(input, "character_length")),
        ),
        ("chr", F::scalar_udf(string::chr)),
        ("collate", F::unknown("collate")),
        ("collation", F::unknown("collation")),
        ("concat_ws", F::udf(SparkConcatWs::new())),
        ("contains", F::custom(|input| startswith(input, "contains"))),
        ("decode", F::udf(SparkDecode::new())),
        ("elt", F::udf(SparkElt::new())),
        ("encode", F::udf(SparkEncode::new())),
        ("endswith", F::custom(|input| startswith(input, "endswith"))),
        ("find_in_set", F::scalar_udf(unicode::find_in_set)),
        ("format_number", F::udf(FormatNumber::new())),
        ("format_string", F::udf(FormatStringFunc::new())),
        ("initcap", F::scalar_udf(unicode::initcap)),
        ("instr", F::scalar_udf(unicode::strpos)),
        (
            "is_valid_utf8",
            F::custom(|input| validate_utf8(input, "is_valid_utf8")),
        ),
        ("lcase", F::custom(|input| lower(input, "lcase"))),
        ("left", F::scalar_udf(unicode::left)),
        ("len", F::custom(|input| length(input, "len"))),
        ("length", F::custom(|input| length(input, "length"))),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(|input| lower(input, "lower"))),
        ("lpad", F::scalar_udf(unicode::lpad)),
        (
            "ltrim",
            F::custom(|input| resolve_trim(input, string::ltrim())),
        ),
        (
            "luhn_check",
            F::scalar_udf(datafusion_spark::function::string::luhn_check),
        ),
        ("make_valid_utf8", F::udf(MakeValidUtf8::new())),
        ("mask", F::udf(SparkMask::new())),
        (
            "octet_length",
            F::custom(|input| length(input, "octet_length")),
        ),
        ("overlay", F::custom(overlay)),
        ("position", F::custom(position)),
        ("printf", F::udf(FormatStringFunc::new())),
        ("quote", F::udf(SparkQuote::new())),
        ("randstr", F::udf(Randstr::new())),
        (
            "regexp_count",
            F::udf(regex::regexpcount::RegexpCountFunc::new()),
        ),
        ("regexp_extract", F::udf(SparkRegexpExtract::new())),
        ("regexp_extract_all", F::udf(SparkRegexpExtractAll::new())),
        ("regexp_instr", F::custom(regexp_instr)),
        ("regexp_replace", F::custom(regexp_replace)),
        ("regexp_substr", F::custom(regexp_substr)),
        ("repeat", F::scalar_udf(string::repeat)),
        ("replace", F::custom(|input| replace(input, "replace"))),
        ("right", F::scalar_udf(unicode::right)),
        ("rpad", F::scalar_udf(unicode::rpad)),
        (
            "rtrim",
            F::custom(|input| resolve_trim(input, string::rtrim())),
        ),
        ("sentences", F::udf(SparkSentences::new())),
        ("soundex", F::udf(Soundex::new())),
        ("space", F::custom(space)),
        ("split", F::udf(SparkSplit::new())),
        ("split_part", F::scalar_udf(string::split_part)),
        (
            "startswith",
            F::custom(|input| startswith(input, "startswith")),
        ),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::custom(resolve_substring_index)),
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::custom(to_char)),
        ("to_number", F::udf(SparkToNumber::new(false))),
        ("to_varchar", F::custom(to_char)),
        ("translate", F::scalar_udf(unicode::translate)),
        (
            "trim",
            F::custom(|input| resolve_trim(input, string::btrim())),
        ),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::udf(SparkToNumber::new(true))),
        (
            "try_validate_utf8",
            F::custom(|input| validate_utf8(input, "try_validate_utf8")),
        ),
        ("ucase", F::custom(|input| lower(input, "ucase"))),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::custom(|input| lower(input, "upper"))),
        (
            "validate_utf8",
            F::custom(|input| validate_utf8(input, "validate_utf8")),
        ),
        ("strpos", F::scalar_udf(unicode::strpos)),
    ]
}
