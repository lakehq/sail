use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::string::contains::ContainsFunc;
use datafusion::functions_array::string::string_to_array;
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, expr, lit, try_cast, ExprSchemable, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::string::levenshtein::Levenshtein;
use crate::extension::function::string::make_valid_utf8::MakeValidUtf8;
use crate::extension::function::string::spark_base64::{SparkBase64, SparkUnbase64};
use crate::extension::function::string::spark_encode_decode::{SparkDecode, SparkEncode};
use crate::extension::function::string::spark_mask::SparkMask;
use crate::extension::function::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use crate::extension::function::string::spark_to_number::SparkToNumber;
use crate::extension::function::string::spark_try_to_number::SparkTryToNumber;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::utils::ItemTaker;

fn regexp_replace(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { mut arguments, .. } = input;
    if arguments.len() != 3 {
        return Err(PlanError::invalid("regexp_replace requires 3 arguments"));
    }
    // Spark replaces all occurrences of the pattern.
    arguments.push(expr::Expr::Literal(
        ScalarValue::Utf8(Some("g".to_string())),
        None,
    ));
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(
            functions::regex::regexpreplace::RegexpReplaceFunc::new(),
        )),
        args: arguments,
    }))
}

fn substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() == 2 {
        let (first, second) = arguments.two()?;
        let first = match &first {
            expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
            | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
            | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => first,
            _ => {
                let first_data_type = first.get_type(function_context.schema)?;
                if matches!(
                    first_data_type,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) {
                    first
                } else {
                    expr::Expr::Cast(expr::Cast {
                        expr: Box::new(first),
                        data_type: DataType::Utf8,
                    })
                }
            }
        };
        // TODO: Spark client throws "UNEXPECTED EXCEPTION: ArrowInvalid('Unrecognized type: 24')"
        //  when the return type is Utf8View.
        return Ok(expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr_fn::substr(first, second)),
            data_type: DataType::Utf8,
        }));
    }
    if arguments.len() == 3 {
        let (first, second, third) = arguments.three()?;
        let first = match &first {
            expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
            | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
            | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => first,
            _ => {
                let first_data_type = first.get_type(function_context.schema)?;
                if matches!(
                    first_data_type,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) {
                    first
                } else {
                    expr::Expr::Cast(expr::Cast {
                        expr: Box::new(first),
                        data_type: DataType::Utf8,
                    })
                }
            }
        };
        // TODO: Spark client throws "UNEXPECTED EXCEPTION: ArrowInvalid('Unrecognized type: 24')"
        //  when the return type is Utf8View.
        return Ok(expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr_fn::substring(first, second, third)),
            data_type: DataType::Utf8,
        }));
    }
    Err(PlanError::invalid("substr requires 2 or 3 arguments"))
}

fn concat_ws(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (delimiter, args) = arguments.at_least_one()?;
    if args.is_empty() {
        return Ok(expr::Expr::Literal(
            ScalarValue::Utf8(Some("".to_string())),
            None,
        ));
    }
    Ok(expr_fn::concat_ws(delimiter, args))
}

fn overlay(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    if arguments.len() == 3 {
        return Ok(expr_fn::overlay(arguments));
    }
    if arguments.len() == 4 {
        let (str, substr, pos, count) = arguments.four()?;
        return match count {
            expr::Expr::Literal(ScalarValue::Int64(Some(-1)), _metadata)
            | expr::Expr::Literal(ScalarValue::Int32(Some(-1)), _metadata) => {
                Ok(expr_fn::overlay(vec![str, substr, pos]))
            }
            _ => Ok(expr_fn::overlay(vec![str, substr, pos, count])),
        };
    }
    Err(PlanError::invalid("overlay requires 3 or 4 arguments"))
}

fn position(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
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

fn replace(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
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
    let expr = match &expr {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => expr,
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
    let str = match &str {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(str),
            data_type: DataType::Utf8,
        }),
    };
    let substr = match &substr {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => substr,
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
    let str = match &str {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(str),
            data_type: DataType::Utf8,
        }),
    };
    let substr = match &substr {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => substr,
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
    let expr = match &expr {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => expr,
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
    let expr = match &expr {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => expr,
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
    let expr = match &expr {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => expr,
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
    let str = match &str {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => str,
        _ => expr::Expr::Cast(expr::Cast {
            expr: Box::new(str),
            data_type: DataType::Utf8,
        }),
    };
    let search_str = match &search_str {
        expr::Expr::Literal(ScalarValue::Utf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_), _metadata)
        | expr::Expr::Literal(ScalarValue::Utf8View(_), _metadata) => search_str,
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

fn validate_utf8_or_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    let data_type = match arg.get_type(input.function_context.schema)? {
        DataType::LargeBinary | DataType::LargeUtf8 => DataType::LargeUtf8,
        _ => DataType::Utf8,
    };
    Ok(if is_try {
        try_cast(arg, data_type)
    } else {
        cast(arg, data_type)
    })
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

fn make_valid_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(MakeValidUtf8::new())),
        args: input.arguments.clone(),
    }))
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

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
        ("is_valid_utf8", F::custom(is_valid_utf8)),
        ("lcase", F::unary(expr_fn::lower)),
        ("left", F::binary(expr_fn::left)),
        ("len", F::unary(expr_fn::length)),
        ("length", F::unary(expr_fn::length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::unary(expr_fn::lower)),
        ("lpad", F::var_arg(expr_fn::lpad)),
        (
            "ltrim",
            F::var_arg(|args| expr_fn::ltrim(args.into_iter().rev().collect())),
        ),
        ("luhn_check", F::unknown("luhn_check")),
        ("make_valid_utf8", F::custom(make_valid_utf8)),
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
        (
            "rtrim",
            F::var_arg(|args| expr_fn::rtrim(args.into_iter().rev().collect())),
        ),
        ("sentences", F::unknown("sentences")),
        ("soundex", F::unknown("soundex")),
        ("space", F::unary(space)),
        (
            "split",
            F::binary(|arg1, arg2| string_to_array(arg1, arg2, lit(ScalarValue::Utf8(None)))),
        ),
        ("split_part", F::ternary(expr_fn::split_part)),
        ("startswith", F::binary(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::unknown("to_char")),
        ("to_number", F::udf(SparkToNumber::new())),
        ("to_varchar", F::unknown("to_varchar")),
        ("translate", F::ternary(expr_fn::translate)),
        (
            "trim",
            F::var_arg(|args| expr_fn::trim(args.into_iter().rev().collect())),
        ),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::udf(SparkTryToNumber::new())),
        ("try_validate_utf8", F::custom(try_validate_utf8)),
        ("ucase", F::unary(upper)),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::unary(upper)),
        ("validate_utf8", F::custom(validate_utf8)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}
