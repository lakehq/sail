use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, internal_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, expr, lit,
};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use sail_common_datafusion::display::{
    ArrayFormatter, FormatOptions, spark_f32_to_string, spark_f64_to_string,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::scalar::math::spark_hex::{printed_text, prints_as_text};
use crate::scalar::math::spark_unhex::{
    SparkUnHex, coerce_input, conversion_invalid_input_err, sql_string_value,
};
use crate::scalar::string::spark_base64::{SparkUnbase64, is_valid_base64};

/// What `ToBinary.fmt` resolves to (`stringExpressions.scala`): the format is a foldable STRING,
/// compared lower-cased and with NO trimming; a NULL format makes the whole result NULL.
enum Format {
    Hex,
    Utf8,
    Base64,
    Null,
}

/// `Err` carries the lower-cased text of a value that is not a format name, as Spark prints it.
fn parse_format(text: Option<&str>) -> std::result::Result<Format, String> {
    let Some(text) = text else {
        return Ok(Format::Null);
    };
    let lowered = text.to_lowercase();
    match lowered.as_str() {
        "hex" => Ok(Format::Hex),
        "utf-8" | "utf8" => Ok(Format::Utf8),
        "base64" => Ok(Format::Base64),
        _ => Err(lowered),
    }
}

/// The text Spark's implicit cast to STRING gives a format value (a number or a boolean is not an
/// error here: its text is simply not a format name).
fn scalar_text(value: &ScalarValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    Some(match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.clone(),
        ScalarValue::Float64(Some(value)) => spark_f64_to_string(*value),
        ScalarValue::Float32(Some(value)) => spark_f32_to_string(*value),
        // A BINARY is cast to the STRING of its bytes.
        ScalarValue::Binary(Some(bytes))
        | ScalarValue::LargeBinary(Some(bytes))
        | ScalarValue::BinaryView(Some(bytes))
        | ScalarValue::FixedSizeBinary(_, Some(bytes)) => {
            String::from_utf8_lossy(bytes).into_owned()
        }
        // Any other value prints as the cast to STRING does (a timestamp, a calendar interval...).
        other => other
            .to_array()
            .ok()
            .and_then(|array| {
                let options = FormatOptions::default();
                ArrayFormatter::try_new(&array, &options)
                    .ok()
                    .map(|formatter| formatter.value(0).to_string())
            })
            .unwrap_or_else(|| other.to_string()),
    })
}

/// Spark checks the format argument when it analyzes the call, even in a branch that never runs
/// and in a plan that is never executed: the planner calls this for the second argument. A literal
/// must name a format (`to_binary`) and anything that is not foldable is rejected by both.
pub fn validate_format(format: &Expr, is_try: bool) -> Result<()> {
    match format {
        // An ARRAY, MAP or STRUCT is a type error that `nullOnInvalidFormat` does not cover.
        Expr::Literal(value, _) if !matches!(value.data_type(), DataType::Null) => {
            match value.data_type() {
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_) => Err(invalid_format_value_err("ARRAY")),
                DataType::Map(_, _) => Err(invalid_format_value_err("MAP")),
                DataType::Struct(_) | DataType::Union(_, _) => {
                    Err(invalid_format_value_err("NAMED_STRUCT"))
                }
                _ => match parse_format(scalar_text(value).as_deref()) {
                    Err(shown) if !is_try => Err(invalid_format_err(&shown)),
                    _ => Ok(()),
                },
            }
        }
        Expr::Literal(..) => Ok(()),
        other if !is_foldable(other) => Err(non_foldable_err(other)),
        _ => Ok(()),
    }
}

fn invalid_format_err(shown: &str) -> datafusion_common::DataFusionError {
    invalid_format_value_err(&sql_string_value(shown))
}

fn invalid_format_value_err(shown: &str) -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.INVALID_ARG_VALUE] Cannot resolve \"to_binary\" due to data type mismatch: The fmt value must to be a case-insensitive \"STRING\" literal of 'hex', 'utf-8', 'utf8', or 'base64', but got {shown}."
    ))
}

fn non_foldable_err(format: &Expr) -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"to_binary\" due to data type mismatch: the input `fmt` should be a foldable \"STRING\" expression; however, got \"{format}\"."
    ))
}

fn wrong_num_args<T>(name: &str, actual: usize) -> Result<T> {
    plan_err!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{name}` requires [1, 2] parameters but the actual number is {actual}. Please, refer to 'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix."
    )
}

/// Spark's `foldable`: deterministic and free of column references. A format that is foldable but
/// not yet a literal is left for the simplifier to fold; one that is not foldable is an error.
pub fn is_foldable(expression: &Expr) -> bool {
    if expression.is_volatile() {
        return false;
    }
    let mut foldable = true;
    let _ = expression.apply(|node| {
        if matches!(
            node,
            Expr::Column(_)
                | Expr::OuterReferenceColumn(_, _)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::Placeholder(_)
                | Expr::Lambda(_)
                | Expr::LambdaVariable(_)
                | Expr::ScalarVariable(..)
                | Expr::Unnest(_)
        ) {
            foldable = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    foldable
}

fn unhex_expression(value: Expr, is_try: bool, ansi_mode: bool) -> Expr {
    Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkUnHex::with_options(
            !is_try, ansi_mode,
        ))),
        args: vec![value],
    })
}

fn null_binary() -> Expr {
    lit(ScalarValue::Binary(None))
}

/// Plans `to_binary` / `try_to_binary` once the format is known. `to_binary(x[, 'hex'])` is
/// `Unhex(x, failOnError = true)`, so a malformed value raises; `try_to_binary` is the same under
/// `TryEval` and turns a malformed value and an unknown format into NULL.
fn simplify_to_binary(
    args: Vec<Expr>,
    name: &str,
    is_try: bool,
    ansi_mode: bool,
) -> Result<ExprSimplifyResult> {
    match args.len() {
        1 => Ok(ExprSimplifyResult::Simplified(unhex_expression(
            args.one()?,
            is_try,
            ansi_mode,
        ))),
        2 => {
            let (value, format) = args.two()?;
            let Expr::Literal(literal, _) = &format else {
                return if is_foldable(&format) {
                    Ok(ExprSimplifyResult::Original(vec![value, format]))
                } else {
                    Err(non_foldable_err(&format))
                };
            };
            match parse_format(scalar_text(literal).as_deref()) {
                Ok(Format::Hex) => Ok(ExprSimplifyResult::Simplified(unhex_expression(
                    value, is_try, ansi_mode,
                ))),
                // The value is cast to STRING first, which depends on its type: left to run time.
                Ok(Format::Utf8 | Format::Base64) => {
                    Ok(ExprSimplifyResult::Original(vec![value, format]))
                }
                Ok(Format::Null) => Ok(ExprSimplifyResult::Simplified(null_binary())),
                Err(_) if is_try => Ok(ExprSimplifyResult::Simplified(null_binary())),
                Err(shown) => Err(invalid_format_err(&shown)),
            }
        }
        n => wrong_num_args(name, n),
    }
}

fn coerce_to_binary_types(
    name: &str,
    arg_types: &[DataType],
    ansi_mode: bool,
) -> Result<Vec<DataType>> {
    match arg_types {
        [value] => Ok(vec![coerce_input(value, "to_binary", ansi_mode)?]),
        [value, format] => Ok(vec![
            coerce_input(value, "to_binary", ansi_mode)?,
            coerce_format(format)?,
        ]),
        other => wrong_num_args(name, other.len()),
    }
}

/// `ToBinary.fmt` is `ImplicitCastInputTypes` over STRING, so a format of any atomic type (a
/// BINARY included) is cast to its text. Only ARRAY, MAP and STRUCT are rejected, and they are
/// rejected by `try_to_binary` too: `nullOnInvalidFormat` only covers an unknown NAME.
fn coerce_format(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(data_type.clone()),
        DataType::Dictionary(_, value_type) => coerce_format(value_type),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => Err(invalid_format_value_err("ARRAY")),
        DataType::Map(_, _) => Err(invalid_format_value_err("MAP")),
        DataType::Struct(_) | DataType::Union(_, _) => {
            Err(invalid_format_value_err("NAMED_STRUCT"))
        }
        _ => Ok(DataType::Utf8),
    }
}

fn first_argument(args: ScalarFunctionArgs) -> ScalarFunctionArgs {
    let ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    } = args;
    ScalarFunctionArgs {
        args: args[0..1].to_vec(),
        arg_fields: arg_fields[0..1].to_vec(),
        number_rows,
        return_field,
        config_options,
    }
}

/// The first argument alone, cast to STRING when it is not already text (`ImplicitCastInputTypes`):
/// the `utf-8` and `base64` formats work on what the value prints as.
fn printed_first_argument(args: ScalarFunctionArgs) -> Result<ScalarFunctionArgs> {
    let mut args = first_argument(args);
    if !prints_as_text(&args.args[0].data_type()) {
        return Ok(args);
    }
    let (array, is_scalar) = match &args.args[0] {
        ColumnarValue::Array(array) => (Arc::clone(array), false),
        ColumnarValue::Scalar(value) => (value.to_array()?, true),
    };
    let text = printed_text(&args, array)?;
    args.arg_fields = vec![Arc::new(Field::new(
        args.arg_fields[0].name(),
        text.data_type().clone(),
        args.arg_fields[0].is_nullable(),
    ))];
    args.args = vec![if is_scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&text, 0)?)
    } else {
        ColumnarValue::Array(text)
    }];
    Ok(args)
}

/// The rows of a STRING or BINARY array as text: a BINARY is cast to STRING as Spark does, so a byte
/// that is not UTF-8 becomes U+FFFD.
fn text_rows(array: &ArrayRef) -> Result<StringArray> {
    Ok(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().clone(),
        DataType::LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .map(|v| v.map(str::to_owned))
            .collect(),
        DataType::Utf8View => array
            .as_string_view()
            .iter()
            .map(|v| v.map(str::to_owned))
            .collect(),
        DataType::Binary => array
            .as_binary::<i32>()
            .iter()
            .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
            .collect(),
        DataType::LargeBinary => array
            .as_binary::<i64>()
            .iter()
            .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
            .collect(),
        DataType::FixedSizeBinary(_) => array
            .as_fixed_size_binary()
            .iter()
            .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
            .collect(),
        other => return internal_err!("`to_binary` cannot read {other} as text"),
    })
}

fn into_array(value: &ColumnarValue) -> Result<(ArrayRef, bool)> {
    match value {
        ColumnarValue::Array(array) => Ok((Arc::clone(array), false)),
        ColumnarValue::Scalar(value) => Ok((value.to_array()?, true)),
    }
}

fn from_array(array: ArrayRef, is_scalar: bool) -> Result<ColumnarValue> {
    if is_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &array, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}

/// The `utf-8` format: `Encode` of the value's text. A BINARY goes through its text, so a byte
/// that is not UTF-8 becomes U+FFFD.
fn encode_utf8(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let value = &args.args[0];
    if !matches!(
        value.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
    ) {
        return value.cast_to(&DataType::Binary, None);
    }
    let (array, is_scalar) = into_array(value)?;
    let text: ArrayRef = Arc::new(text_rows(&array)?);
    from_array(cast(&text, &DataType::Binary)?, is_scalar)
}

/// The `base64` format: `UnBase64(failOnError = true)`. Each row is checked with
/// `UnBase64.isValidBase64`: a malformed one raises `CONVERSION_INVALID_INPUT`, or is NULL for
/// `try_to_binary`, which nulls that row alone.
fn decode_base64(mut args: ScalarFunctionArgs, is_try: bool) -> Result<ColumnarValue> {
    let (array, is_scalar) = into_array(&args.args[0])?;
    let mut checked = Vec::with_capacity(array.len());
    for value in text_rows(&array)?.iter() {
        match value {
            Some(text) if !is_valid_base64(text) => {
                if !is_try {
                    return Err(conversion_invalid_input_err(text, "BASE64"));
                }
                checked.push(None);
            }
            other => checked.push(other.map(str::to_owned)),
        }
    }
    args.arg_fields = vec![Arc::new(Field::new(
        args.arg_fields[0].name(),
        DataType::Utf8,
        true,
    ))];
    args.args = vec![ColumnarValue::Array(Arc::new(StringArray::from(checked)))];
    let decoded = SparkUnbase64::new().invoke_with_args(args)?;
    let (decoded, _) = into_array(&decoded)?;
    from_array(decoded, is_scalar)
}

/// The runtime path, for a format the simplifier could not fold into a literal. The format is
/// foldable (a column format is refused when planning), so every row holds the same value.
fn invoke_to_binary(
    args: ScalarFunctionArgs,
    is_try: bool,
    ansi_mode: bool,
) -> Result<ColumnarValue> {
    let format = match args.args.as_slice() {
        [_] => Ok(Format::Hex),
        [_, ColumnarValue::Scalar(value)] => parse_format(scalar_text(value).as_deref()),
        [_, ColumnarValue::Array(array)] if !array.is_empty() => {
            // A foldable format holds one value (the planner folds it to a literal, so this arm is a
            // guard): a format that differs by row must not silently use the first row's.
            let first = ScalarValue::try_from_array(array, 0)?;
            for row in 1..array.len() {
                if ScalarValue::try_from_array(array, row)? != first {
                    return internal_err!(
                        "`to_binary` expects a format that is the same on every row"
                    );
                }
            }
            parse_format(scalar_text(&first).as_deref())
        }
        [_, ColumnarValue::Array(_)] => Ok(Format::Null),
        other => return internal_err!("`to_binary` expects 1 or 2 arguments, got {}", other.len()),
    };
    match format {
        Ok(Format::Hex) => {
            SparkUnHex::with_options(!is_try, ansi_mode).invoke_with_args(first_argument(args))
        }
        Ok(Format::Utf8) => encode_utf8(printed_first_argument(args)?),
        Ok(Format::Base64) => decode_base64(printed_first_argument(args)?, is_try),
        Ok(Format::Null) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        Err(_) if is_try => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        Err(shown) => Err(invalid_format_err(&shown)),
    }
}

/// Spark's `to_binary` and `try_to_binary`: one expression (`ToBinary`), whose `try_` form is the
/// same plan under `TryEval` with `nullOnInvalidFormat = true`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToBinary {
    signature: Signature,
    is_try: bool,
    ansi_mode: bool,
}

impl Default for SparkToBinary {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkToBinary {
    pub fn new(is_try: bool, ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            is_try,
            ansi_mode,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    fn function_name(&self) -> &'static str {
        if self.is_try {
            "try_to_binary"
        } else {
            "to_binary"
        }
    }
}

impl ScalarUDFImpl for SparkToBinary {
    fn name(&self) -> &str {
        if self.is_try {
            "spark_try_to_binary"
        } else {
            "spark_to_binary"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// With the `hex` format `ToBinary` is `Unhex`, whose `nullable` is `true`, and `TryEval`'s
    /// `nullable` is `true` unconditionally.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // `UnBase64` is a `UnaryExpression`, so with the `base64` format the result is nullable only
        // when its (cast) input is; every other form is always nullable.
        let follows_input = !self.is_try
            && matches!(
                args.scalar_arguments.get(1),
                Some(Some(format))
                    if matches!(parse_format(scalar_text(format).as_deref()), Ok(Format::Base64))
            );
        let nullable = !follows_input
            || args.arg_fields.first().is_some_and(|field| {
                field.is_nullable() || is_marked_variant_storage_type(field.data_type())
            });
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_to_binary_types(self.function_name(), arg_types, self.ansi_mode)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_to_binary(args, self.is_try, self.ansi_mode)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        simplify_to_binary(args, self.function_name(), self.is_try, self.ansi_mode)
    }
}
