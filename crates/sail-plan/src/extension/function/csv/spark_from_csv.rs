use core::any::type_name;
use datafusion::arrow::{
    array::{ArrayRef, StringArray},
    datatypes::*,
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // We cannot know the final DataType result without knowing the schema input args
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        // We need to implement the return type related to the args
        todo!()
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
fn parse_data_type(raw_data_type: &str) -> Result<DataType> {
    let dt: Result<DataType> = DataType::try_from(raw_data_type)
        .map_err(|_e| DataFusionError::Internal(format!("Unsupported type: {}", raw_data_type)));
    dt
}
