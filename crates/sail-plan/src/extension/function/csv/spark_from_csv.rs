use core::any::type_name;
use datafusion::arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Fields},
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};

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
            // - Two column expressions plus options
            // - One column expression plus the struct DataType representing the schema plus options
            // - One column expression plus the string representing the schema plus options
            signature: Signature::variadic(
                vec![DataType::Utf8],
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::empty()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_from_csv_inner, vec![])(&args)
    }
}

pub fn spark_from_csv_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "`from_csv` function requires 3 arguments, got {}",
            args.len()
        );
    };

    let column: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema: &StringArray = downcast_arg!(&args[1], StringArray);
    todo!()
}

fn parse_schema_string(schema_str: &str, sep: &str) -> Result<Fields> {
    let fields: Result<Vec<Field>> = schema_str
        .split(sep)
        .map(|c| {
            let parts: Vec<_> = c.trim().split_whitespace().collect();
            if parts.len() != 2 {
                return exec_err!("Invalid field spec: '{}'", c);
            };
            let name: String = parts[0].to_string();
            let dt: DataType = match parts[1].to_uppercase().as_str() {
                "INT" | "INT32" => DataType::Int32,
                "STRING" => DataType::Utf8,
                "FLOAT" | "FLOAT64" => DataType::Float64,
                // TODO. complete with all types. Is there any already built-in parser?
                _ => return exec_err!("Unsupported type: {}", parts[1]),
            };
            Ok(Field::new(&name, dt, true))
        })
        .collect();

    fields.map(|f| Fields::from_iter(f))
}
