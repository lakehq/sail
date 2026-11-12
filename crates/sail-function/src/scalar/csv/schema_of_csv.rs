use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{Array, ArrayRef, MapArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;

use crate::scalar::csv::options::{
    CsvFunction, find_option, find_option_with_alias, reject_null_entries, validate_options,
};
use crate::scalar::datetime::format::DateTimeFormat;
use crate::utils::schema_inference::{type_coercion, validation};

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
}

impl ScalarUDFImpl for SparkSchemaOfCsv {
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
        type_coercion::coerce_types(self.name(), arg_types, true)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validation::validate_literal_args(self.name(), &args.args)?;
        let hints = vec![Hint::AcceptsSingular, Hint::AcceptsSingular];
        make_scalar_function(schema_of_csv_inner, hints)(&args.args)
    }
}

#[derive(Debug)]
struct SparkSchemaOfCsvOptions {
    sep: String,
    timestamp_format: DateTimeFormat,
    date_format: DateTimeFormat,
    timezone: Option<String>,
}

impl Default for SparkSchemaOfCsvOptions {
    #[expect(clippy::expect_used)]
    fn default() -> Self {
        Self {
            sep: ",".to_string(),
            timestamp_format: DateTimeFormat::for_parsing("yyyy-MM-dd HH:mm:ss")
                .expect("default timestamp format should be valid"),
            date_format: DateTimeFormat::for_parsing("yyyy-MM-dd")
                .expect("default date format should be valid"),
            timezone: None,
        }
    }
}

impl SparkSchemaOfCsvOptions {
    fn from_map(map_array: &MapArray) -> Result<Self> {
        let mut options = Self::default();
        // `schema_of_csv` takes only literal arguments, so it is always evaluated eagerly: both the
        // structural NULL-entry check and the value validation run unconditionally here.
        reject_null_entries(map_array, CsvFunction::SchemaOf)?;
        validate_options(map_array, CsvFunction::SchemaOf)?;

        // Read through the same case-insensitive, sep-before-delimiter path as `from_csv`/`to_csv`,
        // so a duplicated key resolves to the last variant and `sep` wins over `delimiter`.
        if let Some(sep) = find_option_with_alias(map_array, "sep", "delimiter") {
            options.sep = sep.to_string();
        }
        if let Some(format) = find_option(map_array, "timestampFormat") {
            options.timestamp_format = DateTimeFormat::for_parsing(format)?;
        }
        if let Some(format) = find_option(map_array, "dateFormat") {
            options.date_format = DateTimeFormat::for_parsing(format)?;
        }
        if let Some(timezone) = find_option(map_array, "timeZone") {
            options.timezone = Some(timezone.to_string());
        }
        Ok(options)
    }
}

fn schema_of_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    validation::validate_args_len(SparkSchemaOfCsv::SCHEMA_OF_CSV_NAME, args)?;
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
    if options.timestamp_format.parse_datetime_value(value).is_ok() {
        return "TIMESTAMP";
    }
    if options.date_format.parse_datetime_value(value).is_ok() {
        return "DATE";
    }
    // Fallback to ISO date format if dateFormat doesn't match
    if NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() {
        return "DATE";
    }
    "STRING"
}
