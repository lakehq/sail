use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::boolean::{is_null, or};
use datafusion::arrow::compute::kernels::nullif::nullif;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions::string::concat::ConcatFunc;
use datafusion_common::utils::list_ndims;
use datafusion_common::{plan_err, ExprSchema, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions_nested::concat::ArrayConcat;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcat {
    signature: Signature,
}

impl Default for SparkConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![]), TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// [Credit]: <https://github.com/apache/datafusion/blob/7ccc6d7c55ae9dbcb7dee031f394bf11a03000ba/datafusion/functions-nested/src/concat.rs#L276-L310>
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Ok(DataType::Utf8);
        }
        if arg_types
            .iter()
            .any(|arg_type| matches!(arg_type, DataType::List(_)))
        {
            let mut expr_type: Option<DataType> = None;
            let mut max_dims = 0;
            for arg_type in arg_types {
                match arg_type {
                    DataType::List(field) => {
                        if !field.data_type().equals_datatype(&DataType::Null) {
                            let dims = list_ndims(arg_type);
                            expr_type = Some(match max_dims.cmp(&dims) {
                                Ordering::Greater => expr_type.unwrap_or_else(|| arg_type.clone()),
                                Ordering::Equal => {
                                    if let Some(expr_type) = expr_type {
                                        merge_list_types(&expr_type, arg_type).ok_or_else(|| {
                                            datafusion_common::plan_datafusion_err!(
                                                "It is not possible to concatenate arrays of different types. Expected: {expr_type}, got: {arg_type}"
                                            )
                                        })?
                                    } else {
                                        arg_type.clone()
                                    }
                                }
                                Ordering::Less => {
                                    max_dims = dims;
                                    arg_type.clone()
                                }
                            });
                        }
                    }
                    _ => {
                        return plan_err!(
                            "The array_concat function can only accept list as the args."
                        )
                    }
                }
            }
            // All arrays had Null element type (e.g. array() + array()) — keep as List(Null)
            Ok(expr_type.unwrap_or_else(|| arg_types[0].clone()))
        } else if arg_types
            .iter()
            .all(|arg_type| matches!(arg_type, DataType::Binary))
        {
            Ok(DataType::Binary)
        } else if arg_types
            .iter()
            .all(|arg_type| matches!(arg_type, DataType::Binary | DataType::LargeBinary))
        {
            Ok(DataType::LargeBinary)
        } else {
            Ok(arg_types
                .iter()
                .find(|&arg_type| matches!(arg_type, &DataType::Utf8View))
                .or_else(|| {
                    arg_types
                        .iter()
                        .find(|&arg_type| matches!(arg_type, &DataType::LargeUtf8))
                })
                .unwrap_or(&DataType::Utf8)
                .clone())
        }
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        if args.is_empty() {
            true
        } else {
            args.iter().any(|arg| arg.nullable(schema).unwrap_or(true))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                Some(String::new()),
            )));
        }
        let return_type = args.return_field.data_type().clone();
        let mut null_mask = None;
        for arg in args.args.clone() {
            match arg {
                ColumnarValue::Scalar(s) if s.is_null() => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from(&return_type)?));
                }
                ColumnarValue::Array(a) => {
                    let mask = is_null(&a)?;
                    null_mask = match null_mask {
                        Some(existing) => Some(or(&existing, &mask)?),
                        None => Some(mask),
                    };
                }
                _ => (),
            }
        }
        let null_mask = null_mask.unwrap_or_else(|| BooleanArray::from(vec![false; 1]));

        let return_field = args.return_field.clone();
        let return_type = return_field.data_type();

        let concatenated = if args
            .args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            // Cast arrays with Null element type to the return type for proper concatenation
            // This handles cases like concat(array(), array(1, 2, 3)) where the first array
            // has type List(Null) and needs to be cast to List(Int32)
            let casted_args = cast_list_columnar_values(args.args, return_type)?;
            let casted_scalar_args = ScalarFunctionArgs {
                args: casted_args,
                arg_fields: args.arg_fields,
                number_rows: args.number_rows,
                return_field: args.return_field,
                config_options: args.config_options,
            };
            ArrayConcat::new().invoke_with_args(casted_scalar_args)
        } else {
            let casted_columns =
                if args.args.iter().any(|arg| {
                    matches!(arg.data_type(), DataType::LargeUtf8 | DataType::LargeBinary)
                }) {
                    cast_columnar_values(args.args, &DataType::LargeUtf8)?
                } else {
                    cast_columnar_values(args.args, &DataType::Utf8)?
                };

            let casted_args = ScalarFunctionArgs {
                args: casted_columns,
                arg_fields: args.arg_fields,
                number_rows: args.number_rows,
                return_field: args.return_field,
                config_options: args.config_options,
            };

            ConcatFunc::new().invoke_with_args(casted_args)
        }?;

        let concatenated_array = match concatenated {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        Ok(ColumnarValue::Array(nullif(
            cast(&concatenated_array, return_type)?.as_ref(),
            &null_mask,
        )?))
    }
}

fn cast_columnar_values(
    values: Vec<ColumnarValue>,
    target_type: &DataType,
) -> Result<Vec<ColumnarValue>> {
    values
        .into_iter()
        .map(|value| {
            let is_timestamp = matches!(value.data_type(), DataType::Timestamp(_, _));
            match value {
                ColumnarValue::Scalar(scalar) => {
                    let casted = scalar.cast_to(target_type)?;
                    if is_timestamp {
                        Ok(ColumnarValue::Scalar(spark_format_timestamp_scalar(
                            casted,
                        )?))
                    } else {
                        Ok(ColumnarValue::Scalar(casted))
                    }
                }
                ColumnarValue::Array(array) => {
                    let cast_array = cast(&array, target_type)?;
                    if is_timestamp {
                        Ok(ColumnarValue::Array(spark_format_timestamp_array(
                            cast_array,
                        )?))
                    } else {
                        Ok(ColumnarValue::Array(cast_array))
                    }
                }
            }
        })
        .collect()
}

/// Arrow formats timestamps as ISO 8601 (e.g. "2024-01-15T12:00:00Z" or
/// "2024-01-15 12:00:00+08:00"). Spark uses a space separator and no timezone
/// suffix (e.g. "2024-01-15 12:00:00").
fn spark_format_timestamp_str(s: &str) -> String {
    // Replace ISO 8601 'T' separator with a space
    let s = s.replace('T', " ");
    // Strip timezone suffix (Z, +HH:MM, -HH:MM) that starts after the seconds
    // part. "YYYY-MM-DD HH:MM:SS" is 19 chars, so search from position 19 to
    // avoid matching the '-' in the date portion.
    let cutoff = s
        .char_indices()
        .skip_while(|(i, _)| *i < 19)
        .find(|(_, c)| matches!(c, 'Z' | '+' | '-'))
        .map(|(i, _)| i);
    if let Some(idx) = cutoff {
        s[..idx].to_string()
    } else {
        s
    }
}

fn spark_format_timestamp_scalar(scalar: ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Ok(ScalarValue::Utf8(Some(spark_format_timestamp_str(&s)))),
        ScalarValue::LargeUtf8(Some(s)) => {
            Ok(ScalarValue::LargeUtf8(Some(spark_format_timestamp_str(&s))))
        }
        other => Ok(other),
    }
}

fn spark_format_timestamp_array(
    array: Arc<dyn datafusion::arrow::array::Array>,
) -> Result<Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::Array;
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "expected StringArray after timestamp cast".to_string(),
            )
        })?;
    let result: StringArray = string_array
        .iter()
        .map(|opt| opt.map(spark_format_timestamp_str))
        .collect();
    Ok(Arc::new(result))
}

/// Cast list arrays to the target list type.
/// This is needed for concatenating empty arrays and arrays that only differ by
/// element nullability.
fn cast_list_columnar_values(
    values: Vec<ColumnarValue>,
    target_type: &DataType,
) -> Result<Vec<ColumnarValue>> {
    values
        .into_iter()
        .map(|value| {
            let value_type = value.data_type();
            let needs_cast = matches!(value_type, DataType::List(_)) && value_type != *target_type;
            if needs_cast {
                match value {
                    ColumnarValue::Scalar(scalar) => {
                        Ok(ColumnarValue::Scalar(scalar.cast_to(target_type)?))
                    }
                    ColumnarValue::Array(array) => {
                        let cast_array = cast(&array, target_type)?;
                        Ok(ColumnarValue::Array(cast_array))
                    }
                }
            } else {
                Ok(value)
            }
        })
        .collect()
}

fn merge_list_types(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::List(left), DataType::List(right)) => {
            let data_type =
                merge_list_types(left.data_type(), right.data_type()).or_else(|| {
                    left.data_type()
                        .equals_datatype(right.data_type())
                        .then(|| left.data_type().clone())
                })?;
            Some(DataType::List(Arc::new(
                left.as_ref()
                    .clone()
                    .with_data_type(data_type)
                    .with_nullable(left.is_nullable() || right.is_nullable()),
            )))
        }
        _ if left.equals_datatype(right) => Some(left.clone()),
        _ => None,
    }
}
