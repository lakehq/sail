use core::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::prelude::*;
use chrono::ParseError;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{exec_err, internal_err, plan_err, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use sail_common::spec::{
    self, SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME,
    SAIL_MAP_VALUE_FIELD_NAME,
};
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser as sail_parser;

use crate::functions_nested_utils::*;
use crate::functions_utils::make_scalar_function;

/// UDF implementation of `from_csv`, similar to Spark's `from_csv`.
/// This function parses a column of CSV entries using a specified schema string
/// and returns a `StructArray` with the parsed fields.
///
/// Parameters include:
/// - The first input is a `StringArray` containing the CSV-formatted values.
/// - The second input is a `StringArray` specifying the schema for parsing, using SQL-like types
///   (e.g., "name STRING, age INT") for the CSV data.
/// - Optionally, a third input can be provided as a `MapArray` containing options related to the parsing.
///   This may include a "sep" field to specify a custom separator, with the default being a comma (",").
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromCSV {
    signature: Signature,
}

/// Configuration options for the `from_csv` function.
/// These include the CSV field separator and timestamp format string.
#[derive(Debug)]
struct SparkFromCSVOptions {
    sep: String,
    timestamp_format: String,
}

impl SparkFromCSVOptions {
    pub const SEP_OPTION: &'static str = "sep";
    pub const DELIMITER_OPTION: &'static str = "delimiter";
    pub const SEP_DEFAULT: &'static str = ",";
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";

    // ISO 8601. // This format is the Rust chrono crate format equivalent of the Scala/Java Spark format
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%d %H:%M:%S";

    /// Build `SparkFromCSVOptions` from a DataFusion `MapArray` of key-value pairs.
    fn from_map(map: &MapArray) -> Self {
        let sep = find_key_value(map, Self::SEP_OPTION)
            .or(find_key_value(map, Self::DELIMITER_OPTION))
            .unwrap_or(Self::SEP_DEFAULT.to_string());

        let timestamp_format = find_key_value(map, Self::TIMESTAMP_FORMAT_OPTION)
            .as_deref()
            .map(Self::convert_format)
            .unwrap_or(Self::TIMESTAMP_FORMAT_DEFAULT.to_string());

        Self {
            sep,
            timestamp_format,
        }
    }

    /// Converts a Spark/Java-style timestamp format string (e.g., "yyyy-MM-dd")
    /// into a format compatible with the `chrono` crate (e.g., "%Y-%m-%d").
    fn convert_format(fmt: &str) -> String {
        fmt.replace("yyyy", "%Y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("HH", "%H")
            .replace("mm", "%M")
            .replace("ss", "%S")
    }
}

impl Default for SparkFromCSVOptions {
    /// Returns the default parsing options (comma separator, ISO timestamp format).
    fn default() -> Self {
        Self {
            sep: Self::SEP_DEFAULT.to_string(),
            timestamp_format: Self::TIMESTAMP_FORMAT_DEFAULT.to_string(),
        }
    }
}

impl Default for SparkFromCSV {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFromCSV {
    pub const FROM_CSV_NAME: &'static str = "from_csv";

    /// Constructor for the UDF
    pub fn new() -> Self {
        Self {
            // - The first element is a `StringArray` containing CSV-formatted values.
            // - The second element is a `StringArray` representing the schema associated with the CSV data.
            // - Optionally, the third element is a `MapArray` containing options related to CSV parsing.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkFromCSV {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::FROM_CSV_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The base return type is unknown until arguments are provided
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // We cannot know the final DataType result without knowing the schema input args
        Ok(DataType::Struct(Fields::empty()))
    }

    /// Determines the return type of the function based on the schema string and separator
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let ReturnFieldArgs {
            scalar_arguments, ..
        } = args;
        let schema: &String = if let Some(schema) = scalar_arguments.get(1) {
            match schema {
                Some(ScalarValue::Utf8(Some(schema)))
                | Some(ScalarValue::LargeUtf8(Some(schema)))
                | Some(ScalarValue::Utf8View(Some(schema))) => Ok(schema),
                _ => internal_err!("Expected UTF-8 schema string"),
            }?
        } else {
            return plan_err!(
                "`{}` function requires 2 or 3 arguments, got {}",
                Self::FROM_CSV_NAME,
                scalar_arguments.len()
            );
        };

        let dt: DataType = DataType::Struct(parse_fields(schema)?);
        Ok(Arc::new(Field::new(self.name(), dt, true)))
    }

    /// Executes the function with given arguments and produces the resulting array
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_from_csv_inner, vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8, DataType::Utf8] | [DataType::LargeUtf8, DataType::Utf8] => {
                Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
            }
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Map(_, _)] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    arg_types[2].clone(),
                ])
            }
            _ => plan_err!(
                "`{}` function requires 2 or 3 arguments, got {}",
                Self::FROM_CSV_NAME,
                arg_types.len()
            ),
        }
    }
}

/// Core implementation of the `from_csv` function logic.
///
/// This function processes an array of CSV lines, using a specified schema string,
/// and converts them into a `StructArray` of scalar values. It supports custom parsing options
/// including custom field delimiters and timestamp formats. It expects at least two arguments:
/// an array of CSV strings and a schema string. Optionally, a map of parsing options can be provided.
///
/// # Parameters
/// - `args`: An array of input arrays, where:
///   - `args[0]` is a `StringArray` containing the CSV data lines.
///   - `args[1]` is a `StringArray` containing the schema definition.
///   - `args[2]` (optional) is a `MapArray` of options like delimiters, timestamp formats, etc.
///
/// # Returns
/// A `Result` containing a `StructArray` which stores parsed CSV values per specified schema.
///
/// # Errors
/// This function returns an error if:
/// - The number of arguments is incorrect (not 2 or 3).
/// - Schema parsing fails due to errors in the schema string.
/// - CSV line parsing fails, such as when field count mismatches or data type conversion errors occur.
fn spark_from_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`{}` function requires 2 or 3 arguments, got {}",
            SparkFromCSV::FROM_CSV_NAME,
            args.len()
        );
    };

    let array: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema_str: &StringArray = downcast_arg!(&args[1], StringArray);
    let schema_str: &str = if schema_str.is_empty() {
        return exec_err!(
            "`{}` function requires a schema string, got an empty string",
            SparkFromCSV::FROM_CSV_NAME
        );
    } else {
        schema_str.value(0)
    };

    let options: SparkFromCSVOptions = if let Some(options) = args.get(2) {
        SparkFromCSVOptions::from_map(downcast_arg!(options, MapArray))
    } else {
        SparkFromCSVOptions::default()
    };

    let fields: Fields = parse_fields(schema_str)?;

    let mut children_scalars: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(array.len()); fields.len()];
    let mut validity: Vec<bool> = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            for j in 0..children_scalars.len() {
                children_scalars[j].push(ScalarValue::try_new_null(fields[j].data_type())?);
            }
            validity.push(false);
        } else {
            let line: &str = array.value(i);
            let values: Vec<ScalarValue> =
                parse_csv_line_to_scalar_values(line, &options, &fields)?;
            for (j, value) in values.into_iter().enumerate() {
                children_scalars[j].push(value);
            }
            validity.push(true);
        }
    }

    let children_arrays: Vec<ArrayRef> = children_scalars
        .into_iter()
        .map(|arr| ScalarValue::iter_to_array(arr))
        .collect::<Result<_>>()?;

    Ok(Arc::new(StructArray::new(
        fields,
        children_arrays,
        Some(validity.into()),
    )))
}

/// Parses a single CSV line into a collection of `ScalarValue`s based on field types.
///
/// This function takes a CSV line and splits it according to the specified separator,
/// then converts each value into a `ScalarValue` guided by the provided field data types.
/// It can handle different types of CSV fields, including timestamps with specific formatting.
///
/// # Parameters
/// - `line`: The string representation of a single CSV line to be parsed.
/// - `options`: `SparkFromCSVOptions` detailing parsing settings such as separator and timestamp format.
/// - `fields`: The `Fields` that define the structural and type constraints for each parsed column.
///
/// # Returns
/// A `Result` containing a `Vec<ScalarValue>` after successful parsing of the CSV line.
///
/// # Errors
/// Returns an error if:
/// - The number of values in the line does not match the number of fields indicated by the schema.
/// - The string values cannot be converted into the types specified by the schema due to format issues.
fn parse_csv_line_to_scalar_values(
    line: &str,
    options: &SparkFromCSVOptions,
    fields: &Fields,
) -> Result<Vec<ScalarValue>> {
    let values: Vec<&str> = line.split(&options.sep).map(|s| s.trim()).collect();

    if values.len() != fields.len() {
        return exec_err!(
            "CSV line has {} values but schema expects {} fields: '{}'",
            values.len(),
            fields.len(),
            line
        );
    }

    values
        .iter()
        .zip(fields.iter())
        .map(|(value, field)| {
            if value.is_empty() {
                ScalarValue::try_new_null(field.data_type())
            } else {
                match field.data_type() {
                    DataType::Timestamp(_, _) => parse_timestamp(field.data_type(), value, options),
                    _ => ScalarValue::try_from_string(value.to_string(), field.data_type()),
                }
            }
        })
        .collect()
}

/// Parses a timestamp from a string value using a specified format.
///
/// This function attempts to parse the input string `value` into a timestamp,
/// using the format specified in `options`. If the input contains both date
/// and time, `NaiveDateTime::parse_from_str` is used. If the input contains
/// only a date, `NaiveDate::parse_from_str` is used instead.
///
/// # Parameters
/// - `data_type`: The data type of the timestamp, used to ensure proper conversion.
///
/// - `value`: The string representation of the date or datetime to be parsed.
///
/// - `options`: An instance of `SparkFromCSVOptions` that contains the format
///   to use when parsing the timestamp.
///
/// # Returns
/// A `ScalarValue` representing the parsed timestamp, or an error message if
/// parsing fails due to incompatible formats or input.
///
/// # Errors
/// Returns an error if the input `value` doesn't match the expected `format`,
/// or if parsing the date/time fails.
fn parse_timestamp(
    data_type: &DataType,
    value: &str,
    options: &SparkFromCSVOptions,
) -> Result<ScalarValue> {
    let format: &String = &options.timestamp_format;
    let datetime: std::result::Result<String, ParseError> = if let Ok(datetime) =
        NaiveDateTime::parse_from_str(value, format).map(|datetime| format!("{datetime}"))
    {
        Ok(datetime)
    } else {
        NaiveDate::parse_from_str(value, format).map(|date| format!("{date}"))
    };
    match datetime {
        Ok(datetime) => ScalarValue::try_from_string(datetime, data_type),
        Err(e) => exec_err!("Failed to parse timestamp: {}", e),
    }
}

/// Parses a schema string into Arrow `Fields`.
///
/// This function takes a schema definition as a string and converts it into a
/// `Fields` structure, which represents a collection of `Field` items. Each `Field`
/// has a name and a `DataType`, providing a detailed schema that can be used in datasets.
///
/// # Parameters
/// - `schema`: A string representing the schema, which uses a format like
///   "name STRING, age INT". This string defines the names and types of fields
///   in a database or data structure.
///
/// # Returns
/// A `Result` containing `Fields`, which are essentially a list of `Field`
/// items. Each `Field` describes a single attribute, including its name
/// and data type.
///
/// # Errors
/// Returns an error if the schema string is invalid, such as if it contains
/// duplicate field names or uses an unsupported field type syntax.
fn parse_fields(schema: &str) -> Result<Fields> {
    let schema = schema.trim();
    let type_str = if schema
        .get(..6)
        .is_some_and(|p| p.eq_ignore_ascii_case("struct"))
        && schema.get(6..).is_some_and(|p| p.starts_with('<'))
        && schema.ends_with('>')
    {
        schema.to_string()
    } else {
        // Schema string is a list of fields. Wrap it into `STRUCT<...>`.
        format!("STRUCT<{schema}>")
    };

    let ast = sail_parser::parse_data_type(&type_str)
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    let spec::DataType::Struct { fields } = spec_dt else {
        return Err(DataFusionError::Plan(format!(
            "Expected STRUCT schema, got: {spec_dt:?}"
        )));
    };

    let mut out: Vec<Arc<Field>> = Vec::with_capacity(fields.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(fields.len());
    for f in fields.iter() {
        let name = f.name.clone();
        if !seen.insert(name.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate field name '{name}'"
            )));
        }
        let dt = spec_to_arrow_data_type(&f.data_type)?;
        out.push(Arc::new(Field::new(name, dt, f.nullable)));
    }
    Ok(Fields::from(out))
}

/// Parses a raw SQL type string into an Arrow `DataType`.
///
/// This function utilizes the DataFusion SQL parser to interpret a string
/// representing a SQL data type. It translates the input into an Arrow `DataType`
/// which can be used to define the schema of data within Arrow-based processing.
///
/// # Parameters
/// - `raw`: A string representing the SQL type, such as "INT", "VARCHAR(255)",
///   or "STRUCT<field: INT, other_field: STRING>".
///
/// # Returns
/// A `Result` containing the Arrow `DataType` that corresponds to the SQL type
/// specified in the input string.
///
/// # Errors
/// This function returns an error if:
/// - Tokenizing the string fails, indicating a syntax issue in the provided SQL.
/// - Parsing fails, suggesting that the SQL type is not recognized or improperly formatted.
/// - Conversion to an Arrow `DataType` fails because the SQL type is unsupported.
pub fn parse_data_type(raw: &str) -> Result<DataType> {
    if raw.trim().eq_ignore_ascii_case("timestamp") {
        return Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some(Arc::from("UTC")),
        ));
    }

    let ast = sail_parser::parse_data_type(raw)
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse SQL type '{raw}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze SQL type '{raw}': {e}")))?;
    spec_to_arrow_data_type(&spec_dt)
}

fn spec_to_arrow_data_type(dt: &spec::DataType) -> Result<DataType> {
    use spec::DataType as SDT;

    fn to_time_unit(unit: &spec::TimeUnit) -> TimeUnit {
        match unit {
            spec::TimeUnit::Second => TimeUnit::Second,
            spec::TimeUnit::Millisecond => TimeUnit::Millisecond,
            spec::TimeUnit::Microsecond => TimeUnit::Microsecond,
            spec::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    match dt {
        SDT::Null => Ok(DataType::Null),
        SDT::Boolean => Ok(DataType::Boolean),
        SDT::Int8 => Ok(DataType::Int8),
        SDT::Int16 => Ok(DataType::Int16),
        SDT::Int32 => Ok(DataType::Int32),
        SDT::Int64 => Ok(DataType::Int64),
        SDT::UInt8 => Ok(DataType::UInt8),
        SDT::UInt16 => Ok(DataType::UInt16),
        SDT::UInt32 => Ok(DataType::UInt32),
        SDT::UInt64 => Ok(DataType::UInt64),
        SDT::Float16 => Ok(DataType::Float16),
        SDT::Float32 => Ok(DataType::Float32),
        SDT::Float64 => Ok(DataType::Float64),
        SDT::Binary | SDT::ConfiguredBinary => Ok(DataType::Binary),
        SDT::FixedSizeBinary { size } => Ok(DataType::FixedSizeBinary(*size)),
        SDT::LargeBinary => Ok(DataType::LargeBinary),
        SDT::BinaryView => Ok(DataType::BinaryView),
        SDT::Utf8 | SDT::ConfiguredUtf8 { .. } => Ok(DataType::Utf8),
        SDT::LargeUtf8 => Ok(DataType::LargeUtf8),
        SDT::Utf8View => Ok(DataType::Utf8View),
        SDT::Date32 => Ok(DataType::Date32),
        SDT::Date64 => Ok(DataType::Date64),
        SDT::Timestamp {
            time_unit,
            timestamp_type,
        } => {
            let _ = (time_unit, timestamp_type);
            Ok(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from("UTC")),
            ))
        }
        SDT::Time32 { time_unit: u } => Ok(DataType::Time32(to_time_unit(u))),
        SDT::Time64 { time_unit: u } => Ok(DataType::Time64(to_time_unit(u))),
        SDT::Duration { time_unit: u } => Ok(DataType::Duration(to_time_unit(u))),
        SDT::Interval { interval_unit, .. } => Ok(DataType::Interval(match interval_unit {
            spec::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            spec::IntervalUnit::DayTime => IntervalUnit::DayTime,
            spec::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        })),
        SDT::Decimal128 { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        SDT::Decimal256 { precision, scale } => Ok(DataType::Decimal256(*precision, *scale)),
        SDT::List {
            data_type,
            nullable,
        } => Ok(DataType::List(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            spec_to_arrow_data_type(data_type.as_ref())?,
            *nullable,
        )))),
        SDT::FixedSizeList {
            data_type,
            nullable,
            length,
        } => Ok(DataType::FixedSizeList(
            Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                spec_to_arrow_data_type(data_type.as_ref())?,
                *nullable,
            )),
            *length,
        )),
        SDT::LargeList {
            data_type,
            nullable,
        } => Ok(DataType::LargeList(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            spec_to_arrow_data_type(data_type.as_ref())?,
            *nullable,
        )))),
        SDT::Struct { fields } => {
            let mut out: Vec<Arc<Field>> = Vec::with_capacity(fields.len());
            for f in fields.iter() {
                out.push(Arc::new(Field::new(
                    f.name.clone(),
                    spec_to_arrow_data_type(&f.data_type)?,
                    f.nullable,
                )));
            }
            Ok(DataType::Struct(Fields::from(out)))
        }
        SDT::Map {
            key_type,
            value_type,
            value_type_nullable,
            keys_sorted,
        } => {
            let fields = Fields::from(vec![
                Arc::new(Field::new(
                    SAIL_MAP_KEY_FIELD_NAME,
                    spec_to_arrow_data_type(key_type.as_ref())?,
                    false,
                )),
                Arc::new(Field::new(
                    SAIL_MAP_VALUE_FIELD_NAME,
                    spec_to_arrow_data_type(value_type.as_ref())?,
                    *value_type_nullable,
                )),
            ]);
            Ok(DataType::Map(
                Arc::new(Field::new(
                    SAIL_MAP_FIELD_NAME,
                    DataType::Struct(fields),
                    false,
                )),
                *keys_sorted,
            ))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported data type in from_csv schema: {other:?}"
        ))),
    }
}

/// Finds the index of a specified key in a `MapArray`.
///
/// This helper function locates the index of a given key within a `MapArray`,
/// where the keys are stored in a "key" column. It is useful for quickly identifying
/// the position of an option or setting within structured options data.
fn find_key_index(options: &MapArray, search_key: &str) -> Option<usize> {
    options
        .entries()
        .column_by_name(SAIL_MAP_KEY_FIELD_NAME)
        .and_then(|x| x.as_any().downcast_ref::<StringArray>())
        .and_then(|x| {
            x.iter()
                .enumerate()
                .find(|(_, x)| x.as_ref().is_some_and(|x| *x == search_key))
        })
        .map(|(i, _)| i)
}

/// Retrieves the value associated with a specified key from a `MapArray`.
///
/// This function extracts the string value assigned to a given key within a `MapArray`,
/// leveraging the index found by `find_key_index`. It searches for the key in the "key"
/// column and returns the corresponding value from the "value" column if found.
fn find_key_value(options: &MapArray, search_key: &str) -> Option<String> {
    if let Some(index) = find_key_index(options, search_key) {
        options
            .entries()
            .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)
            .and_then(|x| x.as_any().downcast_ref::<StringArray>())
            .map(|values| values.value(index).to_string())
    } else {
        None
    }
}

/// Unit test for `spark_from_csv_inner` that verifies CSV parsing into a `StructArray`.
/// This test simulates a column of CSV lines and checks:
/// - correct parsing of valid rows
/// - handling of null rows
/// - correct nullability for missing fields
#[test]
fn test_from_csv_simple_struct() -> Result<()> {
    // Input CSV lines for the column ("name,age"), including a null and an empty field
    let csv_data = vec![Some("alice,30"), Some("bob,25"), None, Some("charlie,")];

    // Wrap input as Arrow StringArray
    let input_array = Arc::new(StringArray::from(csv_data)) as ArrayRef;

    // Define the schema: name is a string, age is an int
    let schema_str = Arc::new(StringArray::from(vec!["name STRING, age INT"])) as ArrayRef;

    // Execute the function with CSV column and schema
    let result = spark_from_csv_inner(&[input_array, schema_str])?;

    // Downcast the result to a StructArray
    let struct_array = result.as_any().downcast_ref::<StructArray>();

    let Some(struct_array) = struct_array else {
        return internal_err!(
            "[test][{}] Expected StructArray",
            SparkFromCSV::FROM_CSV_NAME
        );
    };

    // There should be 4 entries total, and 1 null struct (the third)
    assert_eq!(struct_array.len(), 4);
    assert_eq!(struct_array.null_count(), 1);

    // Check the `name` field (Utf8)
    let name_array = struct_array.column_by_name("name");

    let Some(name_array) = name_array else {
        return internal_err!(
            "[test][{}] Expected `name` field not found",
            SparkFromCSV::FROM_CSV_NAME
        );
    };

    let name_array = name_array.as_any().downcast_ref::<StringArray>();

    let Some(name_array) = name_array else {
        return internal_err!(
            "[test][{}] Expected StringArray",
            SparkFromCSV::FROM_CSV_NAME
        );
    };

    assert_eq!(name_array.value(0), "alice");
    assert_eq!(name_array.value(1), "bob");
    assert!(name_array.is_null(2)); // Entire struct was null
    assert_eq!(name_array.value(3), "charlie");

    // Check the `age` field (Int32)
    let age_array = struct_array.column_by_name("age");

    let Some(age_array) = age_array else {
        return internal_err!(
            "[test][{}] Expected `age` field not found",
            SparkFromCSV::FROM_CSV_NAME
        );
    };

    let age_array = age_array.as_any().downcast_ref::<Int32Array>();

    let Some(age_array) = age_array else {
        return internal_err!(
            "[test][{}] Expected Int32Array",
            SparkFromCSV::FROM_CSV_NAME
        );
    };

    assert_eq!(age_array.value(0), 30);
    assert_eq!(age_array.value(1), 25);
    assert!(age_array.is_null(2)); // Struct was null
    assert!(age_array.is_null(3)); // Empty value parsed as null

    Ok(())
}

#[cfg(test)]
macro_rules! downcast_option {
    ($opt:expr, $typ:ty, $err_msg:expr) => {{
        let some_value = $opt;
        let some_value = match some_value {
            Some(value) => value,
            None => return internal_err!(concat!("[test][{}] ", $err_msg), SparkFromCSV::FROM_CSV_NAME),
        };
        let downcasted_value = some_value.as_any().downcast_ref::<$typ>();
        match downcasted_value {
            Some(downcasted_value) => downcasted_value,
            None => return internal_err!(concat!("[test][{}] ", stringify!(Expected $typ)), SparkFromCSV::FROM_CSV_NAME),
        }
    }};
}

#[test]
fn test_from_csv_decimal_and_timestamp() -> Result<()> {
    let csv_data = vec![
        Some("9.99,2023-01-01 00:00:00"),
        Some("12.34,2024-05-06 15:45:00"),
        None,
        Some(",2025-01-01 12:00:00"),
        Some("7.77,"),
    ];
    let input_array = Arc::new(StringArray::from(csv_data)) as ArrayRef;
    let schema_str = Arc::new(StringArray::from(vec![
        "price DECIMAL(5,2), created TIMESTAMP",
    ])) as ArrayRef;
    let result = spark_from_csv_inner(&[input_array, schema_str])?;

    let struct_array: &StructArray = downcast_option!(
        result.as_any().downcast_ref::<StructArray>(),
        StructArray,
        "Expected StructArray"
    );

    assert_eq!(struct_array.len(), 5);
    assert_eq!(struct_array.null_count(), 1);

    let price_array: &Decimal128Array = downcast_option!(
        struct_array.column_by_name("price"),
        Decimal128Array,
        "Expected `price` field not found"
    );
    assert_eq!(price_array.value(0), 999);
    assert_eq!(price_array.value(1), 1234);
    assert!(price_array.is_null(2));
    assert!(price_array.is_null(3));
    assert_eq!(price_array.value(4), 777);

    let ts_array: &TimestampNanosecondArray = downcast_option!(
        struct_array.column_by_name("created"),
        TimestampNanosecondArray,
        "Expected `created` field not found"
    );
    assert_eq!(ts_array.value(0), 1672531200000000000);
    assert_eq!(ts_array.value(1), 1715010300000000000);
    assert!(ts_array.is_null(2));
    assert_eq!(ts_array.value(3), 1735732800000000000);
    assert!(ts_array.is_null(4));

    Ok(())
}
