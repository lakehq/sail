use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use core::any::type_name;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use regex::Regex;

use datafusion::arrow::{array::*, datatypes::*};
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{
    exec_err, DataFusionError as DFCommonError, Result as DFCommonResult, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use sail_common::spec::i256;

use crate::extension::function::functions_nested_utils::*;
use crate::extension::function::functions_utils::make_scalar_function;

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // We cannot know the final DataType result without knowing the schema input args
        Ok(DataType::Struct(Fields::empty()))
    }

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
            parse_fields(schema, sep).map(|fields| DataType::Struct(fields));

        schema.map(|dt| ReturnInfo::new_nullable(dt))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_from_csv_inner, vec![])(&args)
    }
}

fn spark_from_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`from_csv` function requires 2 or 3 arguments, got {}",
            args.len()
        );
    };

    let array: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema_str: &str = downcast_arg!(&args[1], StringArray).value(0);
    let options: &MapArray = downcast_arg!(&args[2], MapArray);
    let options: &StructArray = options.entries();
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
            // Fila completa null
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

    let struct_array = Arc::new(StructArray::new(
        fields.clone(),
        children_arrays,
        Some(validity.into()),
    ));

    Ok(struct_array)
}

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

fn parse_fields(schema: &str, sep: &str) -> Result<Fields> {
    let schema: Result<Fields> = parse_schema_string(schema, sep);
    schema.map(|fields| {
        let vec_fields: Vec<Arc<Field>> = fields.iter().cloned().collect();
        Fields::from(vec_fields)
    })
}

// TODO: Double-check this implementation
fn parse_schema_string(schema_str: &str, sep: &str) -> Result<Fields> {
    let fields: Result<Vec<Field>> = schema_str
        .split(sep)
        .map(|c| {
            let parts: Vec<_> = c.trim().split_whitespace().collect();
            if parts.len() != 2 {
                return exec_err!("Invalid field spec: '{}'", c);
            };
            let name: String = parts[0].to_string();
            let data_type: Result<DataType> = parse_data_type(&parts[1].to_uppercase().as_str());
            data_type.map(|dt| Field::new(&name, dt, true))
        })
        .collect();

    fields.map(|f| Fields::from_iter(f))
}

// TODO: Is there any SQL type parser?
// TODO: Double-check this implementation
pub fn parse_data_type(raw: &str) -> Result<DataType> {
    let raw = raw.trim().to_lowercase();

    if let Some(dt) = parse_decimal_type(&raw)? {
        return Ok(dt);
    }

    if let Some(dt) = parse_array_type(&raw)? {
        return Ok(dt);
    }

    if let Some(dt) = parse_struct_type(&raw)? {
        return Ok(dt);
    }

    parse_simple_type(&raw)
}

fn parse_decimal_type(raw: &str) -> Result<Option<DataType>> {
    let decimal_re = Regex::new(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$").unwrap();
    if let Some(caps) = decimal_re.captures(raw) {
        let precision: u8 = caps[1].parse().unwrap_or(38);
        let scale: u8 = caps[2].parse().unwrap_or(10);
        Ok(Some(DataType::Decimal128(
            precision,
            scale.try_into().unwrap(),
        )))
    } else {
        Ok(None)
    }
}

fn parse_array_type(raw: &str) -> Result<Option<DataType>> {
    let array_re = Regex::new(r"^array\s*<\s*(.+)\s*>$").unwrap();
    if let Some(caps) = array_re.captures(raw) {
        let inner_type_str = &caps[1];
        let inner_type = parse_data_type(inner_type_str)?;
        Ok(Some(DataType::List(Arc::new(Field::new(
            "element", inner_type, true,
        )))))
    } else {
        Ok(None)
    }
}

fn parse_struct_type(raw: &str) -> Result<Option<DataType>> {
    let struct_re = Regex::new(r"^struct\s*<(.+)>$").unwrap();
    if let Some(caps) = struct_re.captures(raw) {
        let fields_str = &caps[1];
        let fields: Result<Vec<Field>> = fields_str
            .split(',')
            .map(|part| {
                let mut parts = part.splitn(2, ':');
                let name = parts.next().ok_or_else(|| {
                    DataFusionError::Plan("Missing field name in struct".to_string())
                })?;
                let dtype_str = parts.next().ok_or_else(|| {
                    DataFusionError::Plan(format!("Missing type for field '{}'", name))
                })?;
                let dtype = parse_data_type(dtype_str)?;
                Ok(Field::new(name.trim(), dtype, true))
            })
            .collect();

        Ok(Some(DataType::Struct(Fields::from(fields?))))
    } else {
        Ok(None)
    }
}

fn parse_data_type(raw: &str) -> Result<DataType> {
    let dialect = GenericDialect {}; // puedes cambiar a otro dialecto si quieres
    let sql_type = Parser::parse_sql_type(&dialect, raw)
        .map_err(|e| DataFusionError::Plan(format!("SQL type parse error: {e}")))?;

    convert_sql_type(&sql_type)
}

fn convert_sql_type(sql_type: &SQLType) -> Result<DataType> {
    match sql_type {
        SQLType::Int(_) => Ok(DataType::Int32),
        SQLType::BigInt(_) => Ok(DataType::Int64),
        SQLType::Varchar(_) | SQLType::Char(_) | SQLType::Text => Ok(DataType::Utf8),
        SQLType::Float(_) => Ok(DataType::Float64),
        SQLType::Decimal(precision, scale) => Ok(DataType::Decimal128(
            precision.unwrap_or(38) as u8,
            scale.unwrap_or(10) as i8,
        )),
        SQLType::Timestamp { .. } => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        // Agrega aquí más conversiones según lo necesites
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported SQL type: {:?}",
            sql_type
        ))),
    }
}

fn parse_simple_type(raw: &str) -> Result<DataType> {
    match raw {
        "boolean" | "bool" => Ok(DataType::Boolean),
        "tinyint" | "int1" => Ok(DataType::Int8),
        "smallint" | "int2" => Ok(DataType::Int16),
        "int" | "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),

        "unsigned tinyint" | "uint1" => Ok(DataType::UInt8),
        "unsigned smallint" | "uint2" => Ok(DataType::UInt16),
        "unsigned int" | "unsigned integer" | "uint4" => Ok(DataType::UInt32),
        "unsigned bigint" | "uint8" => Ok(DataType::UInt64),

        "float" | "float4" => Ok(DataType::Float32),
        "double" | "float8" | "real" => Ok(DataType::Float64),

        "char" | "character" | "varchar" | "text" | "string" => Ok(DataType::Utf8),
        "binary" | "blob" => Ok(DataType::Binary),

        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)), // TODO: Fix it
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported SQL type: '{}'",
            raw
        ))),
    }
}

#[test]
fn test_from_csv_simple_struct() -> Result<()> {
    // 1. Input CSV lines (name, age)
    let csv_data = vec![Some("alice,30"), Some("bob,25"), None, Some("charlie,")];

    let input_array = Arc::new(StringArray::from(csv_data)) as ArrayRef;

    // 2. Define schema string and separator
    let schema_str = Arc::new(StringArray::from(vec!["name string, age int"])) as ArrayRef;

    // 3. Call the function
    let result = spark_from_csv_inner(&[input_array, schema_str])?;

    // 4. Check the output type and content
    let struct_array = result
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Expected StructArray");

    assert_eq!(struct_array.len(), 4);
    assert_eq!(struct_array.null_count(), 1); // third row is null

    // Field 0: name (Utf8)
    let name_array = struct_array
        .column_by_name("name")
        .expect("name field not found")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(name_array.value(0), "alice");
    assert_eq!(name_array.value(1), "bob");
    assert!(name_array.is_null(2)); // Struct was null
    assert_eq!(name_array.value(3), "charlie");

    // Field 1: age (Int32)
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
