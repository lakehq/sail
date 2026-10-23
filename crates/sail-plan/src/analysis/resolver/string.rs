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

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn string_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "ascii"
                | "char"
                | "lower"
                | "lcase"
                | "upper"
                | "ucase"
                | "validate_utf8"
                | "try_validate_utf8"
                | "is_valid_utf8"
                | "startswith"
                | "endswith"
                | "contains"
                | "length"
                | "len"
                | "char_length"
                | "character_length"
                | "bit_length"
                | "octet_length"
                | "btrim"
                | "trim"
                | "ltrim"
                | "rtrim"
                | "space"
                | "replace"
                | "overlay"
                | "substring_index"
                | "substr"
                | "substring"
                | "position"
                | "locate"
                | "regexp_replace"
                | "regexp_substr"
                | "regexp_instr"
                | "to_char"
                | "to_varchar"
        ) {
            return Ok(None);
        }
        let mut values = std::mem::take(values);
        let result = match name {
            "char" => self.scalar("chr", vec![values.one()?])?,
            "ascii" => {
                let value = self.logical_string(values.one()?, false)?;
                let value = self.scalar("ascii", vec![value])?;
                self.cast(value, DataType::Int32)?
            }
            "lower" | "lcase" | "upper" | "ucase" => {
                let value = self.logical_string(values.one()?, false)?;
                self.scalar(
                    if matches!(name, "lower" | "lcase") {
                        "lower"
                    } else {
                        "upper"
                    },
                    vec![value],
                )?
            }
            "validate_utf8" | "try_validate_utf8" | "is_valid_utf8" => {
                let value = self.logical_string(values.one()?, name != "validate_utf8")?;
                if name == "is_valid_utf8" {
                    self.is_not_null(value)
                } else {
                    value
                }
            }
            "startswith" | "endswith" | "contains" => {
                let (left, right) = values.two()?;
                let left = self.logical_string(left, false)?;
                let right = self.logical_string(right, false)?;
                self.scalar(
                    match name {
                        "startswith" => "starts_with",
                        "endswith" => "ends_with",
                        _ => "contains",
                    },
                    vec![left, right],
                )?
            }
            "length" | "len" | "char_length" | "character_length" | "bit_length"
            | "octet_length" => {
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
                    DataType::FixedSizeBinary(_) => self.cast(value, DataType::Binary)?,
                    DataType::Null => self.cast(value, DataType::Utf8)?,
                    data_type if data_type.is_nested() => {
                        return Err(PlanError::invalid(format!(
                            "`{name}` does not support {data_type} input"
                        )));
                    }
                    _ => {
                        self.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), vec![value])?
                    }
                };
                let function = match name {
                    "bit_length" => ScalarUDF::from(SparkBitLength::new()),
                    "octet_length" => ScalarUDF::from(SparkOctetLength::new()),
                    _ => ScalarUDF::from(SparkLengthFunc::new()),
                };
                self.scalar_udf(Arc::new(function), vec![value])?
            }
            "btrim" => {
                let values = values
                    .into_iter()
                    .map(|v| {
                        if v.result_type
                            .resolved("trim argument")?
                            .data_type
                            .is_numeric()
                        {
                            self.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), vec![v])
                        } else {
                            Ok(v)
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                self.scalar("btrim", values)?
            }
            "trim" | "ltrim" | "rtrim" => {
                values.reverse();
                self.scalar(name, values)?
            }
            "space" => {
                let value = values.one()?;
                let space = self.literal(ScalarValue::Utf8(Some(" ".into())));
                self.scalar("repeat", vec![space, value])?
            }
            "replace" => {
                if values.len() == 2 {
                    values.push(self.literal(ScalarValue::Utf8(Some("".into()))));
                }
                sail_type_system::coercion::validate_arity(name, values.len(), 3)?;
                self.scalar("replace", values)?
            }
            "overlay" => {
                if values.len() == 4
                    && matches!(
                        values[3].node,
                        Deferred::Resolved(ExpressionNode::Literal(
                            ScalarValue::Int32(Some(-1)) | ScalarValue::Int64(Some(-1))
                        ))
                    )
                {
                    values.pop();
                }
                self.scalar("overlay", values)?
            }
            "substring_index" => {
                let (a, b, c) = values.three()?;
                self.scalar("substr_index", vec![a, b, c])?
            }
            "substr" | "substring" => self.substring(values)?,
            "position" | "locate" => self.position(values)?,
            "regexp_replace" => {
                let (value, pattern, replacement) = values.three()?;
                let single = match (&pattern.node, &replacement.node) {
                    (
                        Deferred::Resolved(ExpressionNode::Literal(pattern)),
                        Deferred::Resolved(ExpressionNode::Literal(replacement)),
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
                        self.literal(ScalarValue::Utf8(Some("${1}".into()))),
                    ]
                } else {
                    vec![
                        value,
                        pattern,
                        replacement,
                        self.literal(ScalarValue::Utf8(Some("g".into()))),
                    ]
                };
                self.scalar("regexp_replace", args)?
            }
            "regexp_substr" => {
                let (value, pattern) = values.two()?;
                let empty = self.literal(ScalarValue::Utf8(Some("".into())));
                let open = self.literal(ScalarValue::Utf8(Some("(".into())));
                let close = self.literal(ScalarValue::Utf8(Some(")".into())));
                let pattern = self.scalar("concat_ws", vec![empty, open, pattern, close])?;
                let matches = self.scalar("regexp_match", vec![value, pattern])?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                self.scalar("array_element", vec![matches, one])?
            }
            "regexp_instr" => {
                if values.len() == 2 {
                    values.push(self.literal(ScalarValue::Int32(Some(0))));
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
                let index = if kind.is_string() && !self.context.config.ansi_mode {
                    self.scalar_udf(
                        Arc::new(ScalarUDF::from(SparkCastStringToInt32::new())),
                        vec![index],
                    )?
                } else {
                    index
                };
                let string = self.cast(string, DataType::Utf8)?;
                let pattern = self.cast(pattern, DataType::Utf8)?;
                let function = Arc::new(HigherOrderUDF::new_from_impl(SparkRegexpInstr::new(
                    self.context.config.ansi_mode,
                )));
                self.mixed_lazy_arguments(
                    function,
                    vec![
                        ValueOrLambda::Value(string),
                        ValueOrLambda::Value(pattern),
                        ValueOrLambda::Lambda(index),
                    ],
                )?
            }
            "to_char" | "to_varchar" => {
                let (value, format) = values.two()?;
                let kind = &value.result_type.resolved("to_char input")?.data_type;
                if matches!(
                    kind,
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
                ) {
                    self.scalar_udf(
                        Arc::new(ScalarUDF::from(SparkDateFormat::new(
                            self.context.config.session_timezone.clone(),
                        ))),
                        vec![value, format],
                    )?
                } else if kind.is_binary() {
                    let ExpressionNode::Literal(literal) =
                        format.node.resolved("to_char format")?
                    else {
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
                        "base64" => self.scalar_udf(
                            Arc::new(ScalarUDF::from(SparkBase64::new())),
                            vec![value],
                        )?,
                        "hex" => {
                            self.scalar_udf(datafusion_spark::function::math::hex(), vec![value])?
                        }
                        "utf-8" => self.scalar_udf(
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
                    self.scalar_udf(
                        Arc::new(ScalarUDF::from(SparkToChar::new(
                            self.context.config.ansi_mode,
                        ))),
                        vec![value, format],
                    )?
                }
            }
            _ => return Err(PlanError::internal("unknown string function")),
        };
        Ok(Some(result))
    }

    fn logical_string(
        &mut self,
        value: LogicalExpression,
        safe: bool,
    ) -> PlanResult<LogicalExpression> {
        let target = match value.result_type.resolved("string input")?.data_type {
            DataType::LargeBinary | DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8View => DataType::Utf8View,
            _ => DataType::Utf8,
        };
        if safe {
            self.try_cast(value, target)
        } else {
            self.cast(value, target)
        }
    }

    fn substring(&mut self, mut values: Vec<LogicalExpression>) -> PlanResult<LogicalExpression> {
        let length = if values.len() == 3 {
            values.pop()
        } else {
            None
        };
        let (string, position) = values.two()?;
        let string = self.logical_string(string, false)?;
        let position = match position.node.resolved("substring position")? {
            ExpressionNode::Literal(ScalarValue::Int32(Some(n))) if *n > 0 => position,
            ExpressionNode::Literal(ScalarValue::Int64(Some(n))) if *n > 0 => position,
            ExpressionNode::Literal(ScalarValue::Int32(Some(0)) | ScalarValue::Int64(Some(0))) => {
                self.literal(ScalarValue::Int64(Some(1)))
            }
            _ => {
                let copy = self.copy_expression(&position)?;
                let zero = self.literal(ScalarValue::Int64(Some(0)));
                let positive = self.binary(copy, Operator::Gt, zero)?;
                let copy = self.copy_expression(&position)?;
                let zero = self.literal(ScalarValue::Int64(Some(0)));
                let at_start = self.binary(copy, Operator::Eq, zero)?;
                let copy = self.copy_expression(&string)?;
                let length = self.scalar("char_length", vec![copy])?;
                let length = self.cast(length, DataType::Int64)?;
                let copy = self.copy_expression(&position)?;
                let offset = self.binary(length, Operator::Plus, copy)?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                let offset = self.binary(offset, Operator::Plus, one)?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                let offset = self.scalar("greatest", vec![offset, one])?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                self.case(vec![(positive, position), (at_start, one)], Some(offset))?
            }
        };
        let mut arguments = vec![string, position];
        if let Some(length) = length {
            arguments.push(length);
        }
        let result = self.scalar("substr", arguments)?;
        self.cast(result, DataType::Utf8)
    }

    fn position(&mut self, mut values: Vec<LogicalExpression>) -> PlanResult<LogicalExpression> {
        let start = if values.len() == 3 {
            values.pop()
        } else {
            None
        };
        let (needle, string) = values.two()?;
        let needle = self.logical_string(needle, false)?;
        let string = self.logical_string(string, false)?;
        let Some(start) = start else {
            return self.scalar("strpos", vec![string, needle]);
        };
        let copy = self.copy_expression(&start)?;
        let string = self.scalar("substr", vec![string, copy])?;
        let position = self.scalar("strpos", vec![string, needle])?;
        let copy = self.copy_expression(&start)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let invalid = self.binary(copy, Operator::LtEq, zero)?;
        let copy = self.copy_expression(&position)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let missing = self.binary(copy, Operator::Eq, zero)?;
        let copy = self.copy_expression(&position)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let found = self.binary(copy, Operator::Gt, zero)?;
        let position = self.binary(start, Operator::Plus, position)?;
        let one = self.literal(ScalarValue::Int32(Some(1)));
        let position = self.binary(position, Operator::Minus, one)?;
        let zero_a = self.literal(ScalarValue::Int32(Some(0)));
        let zero_b = self.literal(ScalarValue::Int32(Some(0)));
        self.case(
            vec![(invalid, zero_a), (missing, zero_b), (found, position)],
            None,
        )
    }
}
