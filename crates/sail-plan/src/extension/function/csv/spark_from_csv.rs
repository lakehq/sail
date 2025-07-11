use core::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{exec_err, internal_err, plan_datafusion_err, plan_err, ScalarValue};
use datafusion_expr::sqlparser::ast::{ArrayElemTypeDef, DataType as SQLType};
use datafusion_expr::sqlparser::dialect::GenericDialect;
use datafusion_expr::sqlparser::parser::{Parser, ParserOptions};
use datafusion_expr::sqlparser::tokenizer::Tokenizer;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;

use crate::extension::function::functions_nested_utils::*;
use crate::extension::function::functions_utils::make_scalar_function;

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
#[derive(Debug)]
pub struct SparkFromCSV {
    signature: Signature,
}

impl Default for SparkFromCSV {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFromCSV {
    pub const FROM_CSV_NAME: &'static str = "from_csv";
    pub const SEP_OPTION: &'static str = "sep";
    pub const SEP_DEFAULT: &'static str = ",";

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
        let options = scalar_arguments.get(2).and_then(|opt| match opt {
            Some(ScalarValue::Map(map_array)) => Some(map_array.entries()),
            _ => None,
        });

        let Some(options) = options else {
            return plan_err!(
                "`{}` function requires a third argument of type MapArray",
                SparkFromCSV::FROM_CSV_NAME
            );
        };

        let schema: &String = match scalar_arguments[1] {
            Some(ScalarValue::Utf8(Some(schema)))
            | Some(ScalarValue::LargeUtf8(Some(schema)))
            | Some(ScalarValue::Utf8View(Some(schema))) => Ok(schema),
            _ => internal_err!("Expected UTF-8 schema string"),
        }?;

        let sep: String =
            get_sep_from_options(options).unwrap_or(SparkFromCSV::SEP_DEFAULT.to_string());
        let schema: Result<DataType> = parse_fields(schema, &sep).map(DataType::Struct);
        schema.map(|dt| Arc::new(Field::new(self.name(), dt, true)))
    }

    /// Executes the function with given arguments and produces the resulting array
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_from_csv_inner, vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    DataType::Map(
                        Arc::new(Field::new(Self::SEP_OPTION, DataType::Utf8, true)),
                        false,
                    ),
                ])
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

/// Core implementation of `from_csv` function logic
fn spark_from_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`{}` function requires 2 or 3 arguments, got {}",
            SparkFromCSV::FROM_CSV_NAME,
            args.len()
        );
    };

    let array: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema_str: &str = downcast_arg!(&args[1], StringArray).value(0);

    let sep = if args.len() == 3 {
        let options: &MapArray = downcast_arg!(&args[2], MapArray);
        let options: &StructArray = options.entries();
        get_sep_from_options(options).unwrap_or(SparkFromCSV::SEP_DEFAULT.to_string())
    } else {
        SparkFromCSV::SEP_DEFAULT.to_string()
    };

    let fields: Fields = parse_fields(schema_str, &sep)?;

    let mut children_scalars: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(array.len()); fields.len()];
    let mut validity: Vec<bool> = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            for col in &mut children_scalars {
                col.push(ScalarValue::Null);
            }
            validity.push(false);
        } else {
            let line = array.value(i);
            let values = parse_csv_line_to_scalar_values(line, &sep, &fields)?;
            for (j, value) in values.into_iter().enumerate() {
                children_scalars[j].push(value);
            }
            validity.push(true);
        }
    }

    let children_arrays: Vec<ArrayRef> = children_scalars
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect::<Result<_>>()?;

    Ok(Arc::new(StructArray::new(
        fields,
        children_arrays,
        Some(validity.into()),
    )))
}

/// Parses a CSV line into a vector of `ScalarValue`s, according to the given field types
fn parse_csv_line_to_scalar_values(
    line: &str,
    sep: &str,
    fields: &Fields,
) -> Result<Vec<ScalarValue>> {
    let values: Vec<&str> = line.split(sep).map(|s| s.trim()).collect();

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
        .map(|(value, field)| ScalarValue::try_from_string(value.to_string(), field.data_type()))
        .collect()
}

/// Extracts the separator string ("sep") from a struct options array
fn get_sep_from_options(options: &StructArray) -> Result<String> {
    let sep_column = options.column_by_name("sep").ok_or_else(|| {
        plan_datafusion_err!(
            "Missing '{}' option in `{}` options",
            SparkFromCSV::SEP_OPTION,
            SparkFromCSV::FROM_CSV_NAME
        )
    })?;

    let default_sep = Arc::new(StringArray::from(vec![
        SparkFromCSV::SEP_DEFAULT.to_string()
    ]));

    let sep_array = match sep_column.as_any().downcast_ref::<StringArray>() {
        Some(sep_array) => sep_array,
        None => default_sep.as_ref(),
    };

    if sep_array.is_empty() || sep_array.is_null(0) {
        return plan_err!("'{}' option cannot be null", SparkFromCSV::SEP_OPTION);
    }

    let sep = sep_array.value(0).to_string(); // Convert to owned String
    Ok(sep)
}

/// Parses a schema string like "name STRING, age INT" into Arrow `Fields`
fn parse_fields(schema: &str, sep: &str) -> Result<Fields> {
    let schema: Result<Fields> = parse_schema_string(schema, sep);
    schema.map(|fields| {
        let vec_fields: Vec<Arc<Field>> = fields.iter().cloned().collect();
        Fields::from(vec_fields)
    })
}

/// Parses a schema definition string into a `Fields` list with duplicate check
fn parse_schema_string(schema_str: &str, sep: &str) -> Result<Fields> {
    schema_str
        .split(sep)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|spec| {
            let parts: Vec<_> = spec.split_whitespace().collect();
            if parts.len() != 2 {
                return exec_err!("Invalid field spec: '{}'", spec);
            }

            let name = parts[0];
            let type_str = parts[1];
            let data_type = parse_data_type(type_str)?;
            Ok((name.to_string(), Field::new(name, data_type, true)))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .try_fold(
            (HashSet::new(), Vec::new()),
            |(seen, mut acc), (name, field)| {
                if seen.contains(&name) {
                    Err(DataFusionError::Plan(format!(
                        "Duplicate field name '{name}'"
                    )))
                } else {
                    let mut seen = seen;
                    seen.insert(name);
                    acc.push(field);
                    Ok((seen, acc))
                }
            },
        )
        .map(|(_, fields)| Fields::from(fields))
}

/// Parses a single type string (e.g. "INT", "STRUCT<id INT>") into an Arrow DataType using `sqlparser`
pub fn parse_data_type(raw: &str) -> Result<DataType> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, raw);
    let tokens = tokenizer
        .tokenize()
        .map_err(|e| DataFusionError::Plan(format!("Tokenization error: {e}")))?;

    let mut parser = Parser::new(&dialect)
        .with_options(ParserOptions::default())
        .with_tokens(tokens);

    let sql_type = parser
        .parse_data_type()
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse SQL type '{raw}': {e}")))?;

    convert_sql_type(&sql_type)
}

/// Converts a parsed `sqlparser::ast::DataType` into an Arrow `DataType`
fn convert_sql_type(sql_type: &SQLType) -> Result<DataType> {
    match sql_type {
        SQLType::Int(_) | SQLType::Integer(_) | SQLType::Int4(_) => Ok(DataType::Int32),
        SQLType::BigInt(_) | SQLType::Int8(_) | SQLType::Int64 => Ok(DataType::Int64),
        SQLType::SmallInt(_) | SQLType::Int2(_) | SQLType::Int16 => Ok(DataType::Int16),
        SQLType::TinyInt(_) => Ok(DataType::Int8),

        SQLType::UInt8 => Ok(DataType::UInt8),
        SQLType::UInt16 => Ok(DataType::UInt16),
        SQLType::UInt32 | SQLType::UnsignedInteger => Ok(DataType::UInt32),
        SQLType::UInt64 | SQLType::BigIntUnsigned(_) => Ok(DataType::UInt64),

        SQLType::Float(_)
        | SQLType::Float64
        | SQLType::Double(_)
        | SQLType::DoublePrecision
        | SQLType::Float8 => Ok(DataType::Float64),
        SQLType::Float32 | SQLType::Real | SQLType::Float4 => Ok(DataType::Float32),

        SQLType::Decimal(info) | SQLType::Numeric(info) => {
            let precision_scale = match info {
                datafusion_expr::sqlparser::ast::ExactNumberInfo::Precision(p) => Ok((*p, 10)), // default scale
                datafusion_expr::sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                    Ok((*p, *s))
                }
                datafusion_expr::sqlparser::ast::ExactNumberInfo::None => Err(
                    DataFusionError::Plan("Decimal type missing precision and scale".to_string()),
                ),
            };

            precision_scale
                .map(|(precision, scale)| DataType::Decimal128(precision as u8, scale as i8))
        }

        SQLType::Char(_)
        | SQLType::Character(_)
        | SQLType::Varchar(_)
        | SQLType::CharacterVarying(_)
        | SQLType::CharVarying(_)
        | SQLType::Text
        | SQLType::String(_)
        | SQLType::Nvarchar(_) => Ok(DataType::Utf8),

        SQLType::Binary(_) | SQLType::Varbinary(_) => Ok(DataType::Binary),

        SQLType::Boolean | SQLType::Bool => Ok(DataType::Boolean),

        SQLType::Date | SQLType::Date32 => Ok(DataType::Date32),
        SQLType::Timestamp(_, _) | SQLType::Datetime(_) | SQLType::Datetime64(_, _) => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }

        SQLType::Array(inner) => {
            let inner_type = match inner {
                ArrayElemTypeDef::AngleBracket(t)
                | ArrayElemTypeDef::SquareBracket(t, _)
                | ArrayElemTypeDef::Parenthesis(t) => convert_sql_type(t)?,
                ArrayElemTypeDef::None => {
                    return Err(DataFusionError::Plan(
                        "ARRAY type missing inner element type".to_string(),
                    ));
                }
            };
            Ok(DataType::List(Arc::new(Field::new(
                "element", inner_type, true,
            ))))
        }

        SQLType::Struct(fields, _) => {
            let parsed_fields: Result<Vec<Field>> = fields
                .iter()
                .map(|f| {
                    let dt = convert_sql_type(&f.field_type)?;
                    let name = f
                        .field_name
                        .as_ref()
                        .map(|id| id.value.clone())
                        .ok_or_else(|| {
                            DataFusionError::Plan("Missing field name in STRUCT".to_string())
                        })?;
                    Ok(Field::new(&name, dt, true))
                })
                .collect();
            Ok(DataType::Struct(Fields::from(parsed_fields?)))
        }

        _ => Err(DataFusionError::Plan(format!(
            "Unsupported SQL type: {sql_type:?}"
        ))),
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
fn test_from_csv_with_array_and_bool() -> Result<()> {
    let csv_data = vec![
        Some("true;[1,2,3]"),
        Some("false;[4,5]"),
        Some("true;[]"),
        None,
        Some(";[7,null,9]"),
        Some("false;"),
    ];
    let input_array: ArrayRef = Arc::new(StringArray::from(csv_data)) as ArrayRef;
    let schema_str: ArrayRef =
        Arc::new(StringArray::from(vec!["flag BOOLEAN, values ARRAY<INT>"])) as ArrayRef;

    let key_builder = StringBuilder::new();
    let values_builder = StringBuilder::new();
    let mut options = MapBuilder::new(None, key_builder, values_builder);
    options.keys().append_value(SparkFromCSV::SEP_OPTION);
    options.values().append_value(";");
    let _ = options.append(true);
    let options: ArrayRef = Arc::new(options.finish()) as ArrayRef;

    let result = spark_from_csv_inner(&[input_array, schema_str, options])?;

    let struct_array: &StructArray = downcast_option!(
        result.as_any().downcast_ref::<StructArray>(),
        StructArray,
        "Expected StructArray"
    );

    assert_eq!(struct_array.len(), 6);
    assert_eq!(struct_array.null_count(), 1);

    let flag_array: &BooleanArray = downcast_option!(
        struct_array.column_by_name("flag"),
        BooleanArray,
        "Expected `flag` field not found"
    );
    assert!(flag_array.value(0));
    assert!(!flag_array.value(1));
    assert!(flag_array.value(2));
    assert!(flag_array.is_null(3));
    assert!(flag_array.is_null(4));
    assert!(!flag_array.value(5));

    let values_array: &ListArray = downcast_option!(
        struct_array.column_by_name("values"),
        ListArray,
        "Expected `values` field not found"
    );
    assert_eq!(values_array.len(), 6);
    assert_eq!(values_array.null_count(), 1);

    Ok(())
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
