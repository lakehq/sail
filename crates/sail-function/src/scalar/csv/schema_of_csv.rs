use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{Array, ArrayRef, MapArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfCsv {
    signature: Signature,
}

impl Default for SparkSchemaOfCsv {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaOfCsv {
    pub const SCHEMA_OF_CSV_NAME: &'static str = "schema_of_csv";

    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn validate_args_len<T>(args: &[T]) -> Result<()> {
        if args.is_empty() || args.len() > 2 {
            return plan_err!(
                "function `{}` expected 1 to 2 args but got {}",
                Self::SCHEMA_OF_CSV_NAME,
                args.len()
            );
        }
        Ok(())
    }

    fn validate_args_are_literal(cols: &[ColumnarValue]) -> Result<()> {
        if let Some(ColumnarValue::Array(_)) = cols.first() {
            return Err(DataFusionError::Execution(format!(
                "Expected a literal value for the first arg of `{}`, instead got a column",
                Self::SCHEMA_OF_CSV_NAME,
            )));
        }
        if let Some(ColumnarValue::Array(_)) = cols.get(1) {
            return Err(DataFusionError::Execution(format!(
                "Expected a literal value for the second arg of `{}`, instead got a column",
                Self::SCHEMA_OF_CSV_NAME,
            )));
        }
        Ok(())
    }

    fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
        fn is_valid_csv_arg(arg_type: &DataType) -> bool {
            matches!(
                arg_type,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null
            )
        }

        fn validate_options_arg(arg_type: &DataType) -> Result<()> {
            match arg_type {
                DataType::Null => Ok(()),
                DataType::Map(map_field, _) => match map_field.data_type() {
                    DataType::Struct(fields) => {
                        let key = fields[0].clone();
                        let value = fields[1].clone();
                        if !key.data_type().is_string() || !value.data_type().is_string() {
                            return Err(DataFusionError::Plan(format!(
                                "For function `{}`, the options map keys/values should both be type string. Instead got key: {}, value: {}",
                                SparkSchemaOfCsv::SCHEMA_OF_CSV_NAME,
                                key.data_type(),
                                value.data_type(),
                            )));
                        }
                        Ok(())
                    }
                    _ => unreachable!(),
                },
                _ => plan_err!(
                    "For function `{}` found invalid options arg type: {:?}",
                    SparkSchemaOfCsv::SCHEMA_OF_CSV_NAME,
                    arg_type
                ),
            }
        }

        match arg_types {
            [arg_type] if is_valid_csv_arg(arg_type) => Ok(()),
            [csv_arg_type, options_arg_type] if is_valid_csv_arg(csv_arg_type) => {
                validate_options_arg(options_arg_type)
            }
            _ => plan_err!(
                "For function `{}` found invalid arg types: {:?}",
                Self::SCHEMA_OF_CSV_NAME,
                arg_types
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSchemaOfCsv {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::SCHEMA_OF_CSV_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Self::validate_args_len(arg_types)?;
        Self::validate_arg_types(arg_types)?;
        let mut coerce_to = vec![DataType::Utf8];
        if arg_types.len() > 1 {
            coerce_to.push(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                        Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ));
        }
        Ok(coerce_to)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Self::validate_args_len(&args.args)?;
        Self::validate_args_are_literal(&args.args)?;
        let hints = vec![Hint::AcceptsSingular, Hint::AcceptsSingular];
        make_scalar_function(schema_of_csv_inner, hints)(&args.args)
    }
}

#[derive(Debug)]
struct SparkSchemaOfCsvOptions {
    sep: String,
    timestamp_format: String,
}

impl Default for SparkSchemaOfCsvOptions {
    fn default() -> Self {
        Self {
            sep: ",".to_string(),
            timestamp_format: "%Y-%m-%d %H:%M:%S".to_string(),
        }
    }
}

impl SparkSchemaOfCsvOptions {
    fn from_map(map_array: &MapArray) -> Result<Self> {
        let mut options = Self::default();
        if map_array.is_empty() || map_array.is_null(0) {
            return Ok(options);
        }
        let entries = map_array.value(0);
        let entries = entries
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("expected map entries to be a struct array".to_string())
            })?;
        let keys = entries
            .column_by_name(SAIL_MAP_KEY_FIELD_NAME)
            .and_then(|x| x.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Execution("expected map keys to be a string array".to_string())
            })?;
        let values = entries
            .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)
            .and_then(|x| x.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Execution("expected map values to be a string array".to_string())
            })?;
        for (key, value) in keys.iter().zip(values.iter()) {
            let (key, value) = match (key, value) {
                (Some(key), Some(value)) => (key, value),
                _ => {
                    return plan_err!(
                        "function `{}` does not support null option keys or values",
                        SparkSchemaOfCsv::SCHEMA_OF_CSV_NAME
                    );
                }
            };
            match key {
                "sep" | "delimiter" => options.sep = value.to_string(),
                "timestampFormat" => {
                    options.timestamp_format = super::convert_java_timestamp_format(value);
                }
                // Silently ignore unrecognised options, matching Spark's behaviour.
                _ => {}
            }
        }
        Ok(options)
    }
}

fn schema_of_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    SparkSchemaOfCsv::validate_args_len(args)?;
    let rows = downcast_arg!(&args[0], StringArray);
    let options = if let Some(arg) = args.get(1) {
        SparkSchemaOfCsvOptions::from_map(downcast_arg!(arg, MapArray))?
    } else {
        SparkSchemaOfCsvOptions::default()
    };
    if rows.is_empty() {
        return Err(DataFusionError::Execution(
            "No value passed into input".to_string(),
        ));
    }
    if rows.is_null(0) {
        return Ok(Arc::new(StringArray::from(vec![Option::<String>::None])));
    }
    let ddl = infer_csv_schema(rows.value(0), &options);
    Ok(Arc::new(StringArray::from(vec![ddl])))
}

fn infer_csv_schema(csv_row: &str, options: &SparkSchemaOfCsvOptions) -> String {
    let fields = csv_row
        .split(&options.sep)
        .map(str::trim)
        .enumerate()
        .map(|(index, value)| format!("_c{index}: {}", infer_csv_field_type(value, options)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("STRUCT<{fields}>")
}

fn infer_csv_field_type(value: &str, options: &SparkSchemaOfCsvOptions) -> &'static str {
    if value.is_empty() {
        return "STRING";
    }
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return "BOOLEAN";
    }
    if value.parse::<i32>().is_ok() {
        return "INT";
    }
    if value.parse::<i64>().is_ok() {
        return "BIGINT";
    }
    if value.contains(['.', 'e', 'E']) && value.parse::<f64>().is_ok() {
        return "DOUBLE";
    }
    if NaiveDateTime::parse_from_str(value, &options.timestamp_format).is_ok() {
        return "TIMESTAMP";
    }
    if NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() {
        return "DATE";
    }
    "STRING"
}
