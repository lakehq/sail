use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF, ValueOrLambda};
use datafusion_spark::function::string::length::SparkLengthFunc;
use regex_syntax::hir::Look;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::string::spark_base64::SparkBase64;
use sail_function::scalar::string::spark_encode_decode::SparkDecode;
use sail_function::scalar::string::spark_length::{SparkBitLength, SparkOctetLength};
use sail_function::scalar::string::spark_regexp_instr::SparkRegexpInstr;
use sail_function::scalar::string::spark_to_char::SparkToChar;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_ascii(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = logical_string(context, values.one()?, false)?;
        let value = context.scalar("ascii", vec![value])?;
        context.cast(value, DataType::Int32)?
    })
}

fn resolve_char(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar("chr", vec![values.one()?])
}

fn resolve_lower(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = logical_string(context, values.one()?, false)?;
        context.scalar(
            if matches!(name, "lower" | "lcase") {
                "lower"
            } else {
                "upper"
            },
            vec![value],
        )?
    })
}

fn resolve_validate_utf8(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = logical_string(context, values.one()?, name != "validate_utf8")?;
        if name == "is_valid_utf8" {
            context.is_not_null(value)
        } else {
            value
        }
    })
}

fn resolve_startswith(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (left, right) = values.two()?;
        let left = logical_string(context, left, false)?;
        let right = logical_string(context, right, false)?;
        context.scalar(
            match name {
                "startswith" => "starts_with",
                "endswith" => "ends_with",
                _ => "contains",
            },
            vec![left, right],
        )?
    })
}

fn resolve_length(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
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
        context.scalar_udf(Arc::new(function), vec![value])?
    })
}

fn resolve_btrim(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
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
        context.scalar("btrim", values)?
    })
}

fn resolve_trim(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    Ok({
        values.reverse();
        context.scalar(name, values)?
    })
}

fn resolve_space(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = values.one()?;
        let space = context.literal(ScalarValue::Utf8(Some(" ".into())));
        context.scalar("repeat", vec![space, value])?
    })
}

fn resolve_replace(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    Ok({
        if values.len() == 2 {
            values.push(context.literal(ScalarValue::Utf8(Some("".into()))));
        }
        crate::analyzer::typing::coercion::validate_arity(name, values.len(), 3)?;
        context.scalar("replace", values)?
    })
}

fn resolve_overlay(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    Ok({
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
        context.scalar("overlay", values)?
    })
}

fn resolve_substring_index(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (a, b, c) = values.three()?;
        context.scalar("substr_index", vec![a, b, c])?
    })
}

fn resolve_substr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    substring(context, values)
}

fn resolve_position(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    position(context, values)
}

fn resolve_regexp_replace(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
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
        context.scalar("regexp_replace", args)?
    })
}

fn resolve_regexp_substr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (value, pattern) = values.two()?;
        let empty = context.literal(ScalarValue::Utf8(Some("".into())));
        let open = context.literal(ScalarValue::Utf8(Some("(".into())));
        let close = context.literal(ScalarValue::Utf8(Some(")".into())));
        let pattern = context.scalar("concat_ws", vec![empty, open, pattern, close])?;
        let matches = context.scalar("regexp_match", vec![value, pattern])?;
        let one = context.literal(ScalarValue::Int64(Some(1)));
        context.scalar("array_element", vec![matches, one])?
    })
}

fn resolve_regexp_instr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;

    Ok({
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
        crate::function::scalar::sequence::mixed_lazy_arguments(
            context,
            function,
            vec![
                ValueOrLambda::Value(string),
                ValueOrLambda::Value(pattern),
                ValueOrLambda::Lambda(index),
            ],
        )?
    })
}

fn resolve_to_char(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (value, format) = values.two()?;
        let kind = &value.result_type.resolved("to_char input")?.data_type;
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
        }
    })
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

pub(crate) fn substring(
    context: &mut FunctionContextInput<'_>,
    mut values: Vec<Expr>,
) -> PlanResult<Expr> {
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
            let length = context.scalar("char_length", vec![copy])?;
            let length = context.cast(length, DataType::Int64)?;
            let copy = context.copy_expression(&position)?;
            let offset = context.binary(length, Operator::Plus, copy)?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            let offset = context.binary(offset, Operator::Plus, one)?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            let offset = context.scalar("greatest", vec![offset, one])?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            context.case(vec![(positive, position), (at_start, one)], Some(offset))?
        }
    };
    let mut arguments = vec![string, position];
    if let Some(length) = length {
        arguments.push(length);
    }
    let result = context.scalar("substr", arguments)?;
    context.cast(result, DataType::Utf8)
}

pub(crate) fn position(
    context: &mut FunctionContextInput<'_>,
    mut values: Vec<Expr>,
) -> PlanResult<Expr> {
    let start = if values.len() == 3 {
        values.pop()
    } else {
        None
    };
    let (needle, string) = values.two()?;
    let needle = logical_string(context, needle, false)?;
    let string = logical_string(context, string, false)?;
    let Some(start) = start else {
        return context.scalar("strpos", vec![string, needle]);
    };
    let copy = context.copy_expression(&start)?;
    let string = context.scalar("substr", vec![string, copy])?;
    let position = context.scalar("strpos", vec![string, needle])?;
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

pub(super) fn list_built_in_string_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "base64",
            F::udf(sail_function::scalar::string::spark_base64::SparkBase64::new()),
        ),
        ("chr", F::kernel("chr")),
        ("collate", F::kernel("collate")),
        ("collation", F::kernel("collation")),
        (
            "concat_ws",
            F::udf(sail_function::scalar::string::spark_concat_ws::SparkConcatWs::new()),
        ),
        (
            "decode",
            F::udf(sail_function::scalar::string::spark_encode_decode::SparkDecode::new()),
        ),
        (
            "elt",
            F::udf(datafusion_spark::function::string::elt::SparkElt::new()),
        ),
        (
            "encode",
            F::udf(sail_function::scalar::string::spark_encode_decode::SparkEncode::new()),
        ),
        ("find_in_set", F::kernel("find_in_set")),
        (
            "format_number",
            F::udf(sail_function::scalar::string::format_number::FormatNumber::new()),
        ),
        (
            "format_string",
            F::udf(datafusion_spark::function::string::format_string::FormatStringFunc::new()),
        ),
        ("initcap", F::kernel("initcap")),
        ("instr", F::kernel("instr")),
        ("left", F::kernel("left")),
        (
            "levenshtein",
            F::udf(sail_function::scalar::string::levenshtein::Levenshtein::new()),
        ),
        ("lpad", F::kernel("lpad")),
        (
            "luhn_check",
            F::scalar_udf(datafusion_spark::function::string::luhn_check),
        ),
        (
            "make_valid_utf8",
            F::udf(sail_function::scalar::string::make_valid_utf8::MakeValidUtf8::new()),
        ),
        (
            "mask",
            F::udf(sail_function::scalar::string::spark_mask::SparkMask::new()),
        ),
        (
            "printf",
            F::udf(datafusion_spark::function::string::format_string::FormatStringFunc::new()),
        ),
        (
            "quote",
            F::udf(sail_function::scalar::string::spark_quote::SparkQuote::new()),
        ),
        (
            "randstr",
            F::udf(sail_function::scalar::string::randstr::Randstr::new()),
        ),
        (
            "regexp_count",
            F::udf(datafusion::functions::regex::regexpcount::RegexpCountFunc::new()),
        ),
        (
            "regexp_extract",
            F::udf(
                sail_function::scalar::string::spark_regexp_extract_all::SparkRegexpExtract::new(),
            ),
        ),
        (
            "regexp_extract_all",
            F::udf(
                sail_function::scalar::string::spark_regexp_extract_all::SparkRegexpExtractAll::new(
                ),
            ),
        ),
        ("repeat", F::kernel("repeat")),
        ("right", F::kernel("right")),
        ("rpad", F::kernel("rpad")),
        (
            "sentences",
            F::udf(sail_function::scalar::string::spark_sentences::SparkSentences::new()),
        ),
        (
            "soundex",
            F::udf(sail_function::scalar::string::soundex::Soundex::new()),
        ),
        (
            "split",
            F::udf(sail_function::scalar::string::spark_split::SparkSplit::new()),
        ),
        ("split_part", F::kernel("split_part")),
        ("strpos", F::kernel("strpos")),
        (
            "to_binary",
            F::udf(sail_function::scalar::string::spark_to_binary::SparkToBinary::new()),
        ),
        (
            "to_number",
            F::udf(sail_function::scalar::string::spark_to_number::SparkToNumber::new(false)),
        ),
        ("translate", F::kernel("translate")),
        (
            "try_to_binary",
            F::udf(sail_function::scalar::string::spark_to_binary::SparkTryToBinary::new()),
        ),
        (
            "try_to_number",
            F::udf(sail_function::scalar::string::spark_to_number::SparkToNumber::new(true)),
        ),
        (
            "unbase64",
            F::udf(sail_function::scalar::string::spark_base64::SparkUnbase64::new()),
        ),
        ("ascii", F::custom(resolve_ascii)),
        ("char", F::custom(resolve_char)),
        ("lower", F::custom(|input| resolve_lower(input, "lower"))),
        ("lcase", F::custom(|input| resolve_lower(input, "lcase"))),
        ("upper", F::custom(|input| resolve_lower(input, "upper"))),
        ("ucase", F::custom(|input| resolve_lower(input, "ucase"))),
        (
            "validate_utf8",
            F::custom(|input| resolve_validate_utf8(input, "validate_utf8")),
        ),
        (
            "try_validate_utf8",
            F::custom(|input| resolve_validate_utf8(input, "try_validate_utf8")),
        ),
        (
            "is_valid_utf8",
            F::custom(|input| resolve_validate_utf8(input, "is_valid_utf8")),
        ),
        (
            "startswith",
            F::custom(|input| resolve_startswith(input, "startswith")),
        ),
        (
            "endswith",
            F::custom(|input| resolve_startswith(input, "endswith")),
        ),
        (
            "contains",
            F::custom(|input| resolve_startswith(input, "contains")),
        ),
        ("length", F::custom(|input| resolve_length(input, "length"))),
        ("len", F::custom(|input| resolve_length(input, "len"))),
        (
            "char_length",
            F::custom(|input| resolve_length(input, "char_length")),
        ),
        (
            "character_length",
            F::custom(|input| resolve_length(input, "character_length")),
        ),
        (
            "bit_length",
            F::custom(|input| resolve_length(input, "bit_length")),
        ),
        (
            "octet_length",
            F::custom(|input| resolve_length(input, "octet_length")),
        ),
        ("btrim", F::custom(resolve_btrim)),
        ("trim", F::custom(|input| resolve_trim(input, "trim"))),
        ("ltrim", F::custom(|input| resolve_trim(input, "ltrim"))),
        ("rtrim", F::custom(|input| resolve_trim(input, "rtrim"))),
        ("space", F::custom(resolve_space)),
        (
            "replace",
            F::custom(|input| resolve_replace(input, "replace")),
        ),
        ("overlay", F::custom(resolve_overlay)),
        ("substring_index", F::custom(resolve_substring_index)),
        ("substr", F::custom(resolve_substr)),
        ("substring", F::custom(resolve_substr)),
        ("position", F::custom(resolve_position)),
        ("locate", F::custom(resolve_position)),
        ("regexp_replace", F::custom(resolve_regexp_replace)),
        ("regexp_substr", F::custom(resolve_regexp_substr)),
        ("regexp_instr", F::custom(resolve_regexp_instr)),
        ("to_char", F::custom(resolve_to_char)),
        ("to_varchar", F::custom(resolve_to_char)),
    ]
}
