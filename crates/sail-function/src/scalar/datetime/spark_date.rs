use std::sync::Arc;

use datafusion::arrow::array::{Array, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion::functions::datetime::to_date::ToDateFunc;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignature, TypeSignatureClass};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_date;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDate {
    signature: Signature,
    is_try: bool,
}

impl SparkDate {
    /// Creates a SparkDate.
    ///
    /// When `is_try` is true, returns NULL on invalid input (for try_cast).
    /// When `is_try` is false, throws an error on invalid input (for cast).
    pub fn new(is_try: bool) -> Self {
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));
        // Only try mode accepts the 2-argument formatted form; the non-try form is a
        // synonym for `cast(expr AS DATE)` and accepts a single argument.
        let signature = if is_try {
            Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![string.clone()]),
                    TypeSignature::Coercible(vec![string.clone(), string]),
                ],
                Volatility::Immutable,
            )
        } else {
            Signature::coercible(vec![string], Volatility::Immutable)
        };
        Self { signature, is_try }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn string_to_date32(value: &str, is_try: bool) -> Result<Option<i32>> {
        match parse_date(value).and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?))) {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(e) => Err(exec_datafusion_err!("{e}")),
        }
    }

    /// Parses the value column using the format argument, returning NULL per row on failure.
    fn date_formatted(
        &self,
        args: &[ColumnarValue],
        invoke_args: &ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let format_arg = &args[1];
        match &args[0] {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
                Some(value) => {
                    let date =
                        value.and_then(|s| date_formatted_row(s, 0, format_arg, invoke_args));
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(date)))
                }
                _ => exec_err!("Unsupported data type {scalar:?} for function spark_date"),
            },
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut builder = Date32Array::builder(len);
                for row in 0..len {
                    let value = string_value_at(&args[0], row)?;
                    let date =
                        value.and_then(|s| date_formatted_row(s, row, format_arg, invoke_args));
                    match date {
                        Some(days) => builder.append_value(days),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

impl ScalarUDFImpl for SparkDate {
    fn name(&self) -> &str {
        "spark_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.len() {
            1 => {
                let ScalarFunctionArgs { args, .. } = args;
                let arg = args.one()?;
                let is_try = self.is_try;
                match arg {
                    ColumnarValue::Array(array) => {
                        let array = match array.data_type() {
                            DataType::Utf8 => as_string_array(&array)?
                                .iter()
                                .map(|x| {
                                    x.map(|v| Self::string_to_date32(v, is_try))
                                        .transpose()
                                        .map(|opt| opt.flatten())
                                })
                                .collect::<Result<Date32Array>>()?,
                            DataType::LargeUtf8 => as_large_string_array(&array)?
                                .iter()
                                .map(|x| {
                                    x.map(|v| Self::string_to_date32(v, is_try))
                                        .transpose()
                                        .map(|opt| opt.flatten())
                                })
                                .collect::<Result<Date32Array>>()?,
                            DataType::Utf8View => as_string_view_array(&array)?
                                .iter()
                                .map(|x| {
                                    x.map(|v| Self::string_to_date32(v, is_try))
                                        .transpose()
                                        .map(|opt| opt.flatten())
                                })
                                .collect::<Result<Date32Array>>()?,
                            _ => return exec_err!("expected string array for `date`"),
                        };
                        Ok(ColumnarValue::Array(Arc::new(array)))
                    }
                    ColumnarValue::Scalar(scalar) => {
                        let value = match scalar.try_as_str() {
                            Some(x) => x
                                .map(|v| Self::string_to_date32(v, is_try))
                                .transpose()?
                                .flatten(),
                            _ => {
                                return exec_err!("expected string scalar for `date`");
                            }
                        };
                        Ok(ColumnarValue::Scalar(ScalarValue::Date32(value)))
                    }
                }
            }
            2 => {
                // The planner only routes the 2-argument form here in try mode; the non-try
                // 2-argument form goes through DataFusion's `to_date`. This guard should
                // therefore be unreachable and exists only as a safeguard.
                if !self.is_try {
                    return exec_err!("`spark_date` with a format argument requires try mode");
                }
                self.date_formatted(&args.args, &args)
            }
            n => exec_err!("`spark_date` expects 1 or 2 arguments, got {n}"),
        }
    }
}

/// Returns the string value at `row`, or `None` when the entry is NULL.
fn string_value_at(arg: &ColumnarValue, row: usize) -> Result<Option<&str>> {
    match arg {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let a = as_string_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            DataType::LargeUtf8 => {
                let a = as_large_string_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            DataType::Utf8View => {
                let a = as_string_view_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            other => exec_err!("Unexpected type encountered '{other}'"),
        },
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(value) => Ok(value),
            None => exec_err!("Unexpected scalar type encountered '{scalar}'"),
        },
    }
}

/// Parses a single value against the format argument at `row`, returning the Date32 days.
fn date_formatted_row(
    value: &str,
    row: usize,
    format_arg: &ColumnarValue,
    invoke_args: &ScalarFunctionArgs,
) -> Option<i32> {
    let format = string_value_at(format_arg, row).ok().flatten()?;
    date_with_to_date_func(value, format, invoke_args)
}

/// Parses one value/format pair via DataFusion's `to_date`, returning `None` on failure.
fn date_with_to_date_func(
    value: &str,
    format: &str,
    invoke_args: &ScalarFunctionArgs,
) -> Option<i32> {
    // Invoke DataFusion per row so one parse failure becomes one NULL, not a batch error.
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string()))),
        ],
        arg_fields: invoke_args.arg_fields.clone(),
        number_rows: 1,
        return_field: invoke_args.return_field.clone(),
        config_options: Arc::clone(&invoke_args.config_options),
    };
    match ToDateFunc::new().invoke_with_args(args) {
        Ok(ColumnarValue::Scalar(ScalarValue::Date32(days))) => days,
        _ => None,
    }
}
