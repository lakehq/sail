use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use core::any::type_name;
use regex::Regex;

use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Fields},
};
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{
    exec_err, DataFusionError as DFCommonError, Result as DFCommonResult, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

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
        let key_value_array: Option<&StructArray> =
            args.scalar_arguments.get(2).and_then(|opt| match opt {
                Some(ScalarValue::Map(map_array)) => Some(map_array.entries()),
                _ => None,
            });

        // TODO: Control de errores
        let sep_array: &StringArray = downcast_arg!(
            key_value_array
                .unwrap()
                .column_by_name("sep")
                .expect("'sep' option is not available"),
            StringArray
        );

        let sep: &str = sep_array.value(0);

        let schema: Result<Fields> = match &args.scalar_arguments[1] {
            Some(ScalarValue::Utf8(Some(schema)))
            | Some(ScalarValue::LargeUtf8(Some(schema)))
            | Some(ScalarValue::Utf8View(Some(schema))) => parse_schema_string(schema, sep),

            _ => {
                // Manejo para el caso no esperado
                Err(DataFusionError::Internal(format!("Unsupported type: TODO")))
            }
        };
        schema.map(|fields| {
            let vec_fields: Vec<Arc<Field>> = fields.iter().cloned().collect();
            let dt = DataType::Struct(Fields::from(vec_fields));
            ReturnInfo::new_nullable(dt)
        })
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

    let column: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema: &StringArray = downcast_arg!(&args[1], StringArray);
    todo!()
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
        // "timestamp" => Ok(DataType::Timestamp(None, None)), // TODO: Fix it
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported SQL type: '{}'",
            raw
        ))),
    }
}
