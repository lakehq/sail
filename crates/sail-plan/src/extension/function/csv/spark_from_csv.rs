use std::sync::Arc;

use crate::extension::function::functions_nested_utils::*;
use crate::extension::function::functions_utils::make_scalar_function;
use core::any::type_name;
use datafusion::arrow::{array::*, datatypes::*};
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::sqlparser::ast::{ArrayElemTypeDef, DataType as SQLType};
use datafusion_expr::sqlparser::dialect::GenericDialect;
use datafusion_expr::sqlparser::parser::{Parser, ParserOptions};
use datafusion_expr::sqlparser::tokenizer::Tokenizer;
use datafusion_expr::{
    ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use std::collections::HashSet;

/// UDF implementation of `from_csv`, similar to Spark's `from_csv`.
/// This parses a column of CSV lines using a provided schema string
/// and returns a `StructArray` with the parsed fields.
///
/// The schema is specified using SQL-like types (e.g., "name STRING, age INT").
/// A third argument can be provided as a Map with a "sep" field to override the separator (default is ",").
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
    /// Constructor for the UDF
    pub fn new() -> Self {
        Self {
            // Because you could use it with either:
            // - One column expression + the literal string representing the schema + options
            // - One column expression + the literal string column with a string value representing the schema + options
            signature: Signature::variadic(
                vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8],
                datafusion_expr::Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkFromCSV {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "from_csv"
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
    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        // We need to implement the return type related to the args
        let options_array: Option<&StructArray> =
            args.scalar_arguments.get(2).and_then(|opt| match opt {
                Some(ScalarValue::Map(map_array)) => Some(map_array.entries()),
                _ => None,
            });

        let schema: &String = match args.scalar_arguments[1] {
            Some(ScalarValue::Utf8(Some(schema)))
            | Some(ScalarValue::LargeUtf8(Some(schema)))
            | Some(ScalarValue::Utf8View(Some(schema))) => Ok(schema),
            _ => Err(DataFusionError::Internal(
                "Expected UTF-8 schema string".to_string(),
            )),
        }?;

        let sep: &str = get_sep_from_options(options_array.unwrap()).unwrap_or(",");
        let schema: Result<DataType> =
            parse_fields(schema, sep).map(DataType::Struct);

        schema.map(ReturnInfo::new_nullable)
    }

    /// Executes the function with given arguments and produces the resulting array
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_from_csv_inner, vec![])(&args)
    }
}

/// Core implementation of `from_csv` function logic
fn spark_from_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`from_csv` function requires 2 or 3 arguments, got {}",
            args.len()
        );
    };

    let array: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema_str: &str = downcast_arg!(&args[1], StringArray).value(0);

    let sep: &str = if args.len() == 3 {
        let options: &MapArray = downcast_arg!(&args[2], MapArray);
        let options: &StructArray = options.entries();
        get_sep_from_options(options).unwrap_or(",")
    } else {
        ","
    };

    let fields: Fields = parse_fields(schema_str, sep)?;

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
            let values = parse_csv_line_to_scalar_values(line, sep, &fields)?;
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
fn get_sep_from_options(options: &StructArray) -> Result<&str> {
    let sep_column = options.column_by_name("sep").ok_or_else(|| {
        DataFusionError::Plan("Missing 'sep' option in from_csv options".to_string())
    })?;

    let sep_array = sep_column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan("'sep' option must be a StringArray".to_string()))?;

    if sep_array.is_empty() || sep_array.is_null(0) {
        return Err(DataFusionError::Plan(
            "'sep' option cannot be null".to_string(),
        ));
    }

    Ok(sep_array.value(0))
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
                        "Duplicate field name '{}'",
                        name
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
    let schema_str = Arc::new(StringArray::from(vec!["name string, age int"])) as ArrayRef;

    // Execute the function with CSV column and schema
    let result = spark_from_csv_inner(&[input_array, schema_str])?;

    // Downcast the result to a StructArray
    let struct_array = result
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Expected StructArray");

    // There should be 4 entries total, and 1 null struct (the third)
    assert_eq!(struct_array.len(), 4);
    assert_eq!(struct_array.null_count(), 1);

    // Check the `name` field (Utf8)
    let name_array = struct_array
        .column_by_name("name")
        .expect("name field not found")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(name_array.value(0), "alice");
    assert_eq!(name_array.value(1), "bob");
    assert!(name_array.is_null(2)); // Entire struct was null
    assert_eq!(name_array.value(3), "charlie");

    // Check the `age` field (Int32)
    let age_array = struct_array
        .column_by_name("age")
        .expect("age field not found")
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(age_array.value(0), 30);
    assert_eq!(age_array.value(1), 25);
    assert!(age_array.is_null(2)); // Struct was null
    assert!(age_array.is_null(3)); // Empty value parsed as null

    Ok(())
}
