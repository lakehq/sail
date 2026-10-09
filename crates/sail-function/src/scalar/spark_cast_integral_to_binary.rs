use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, BinaryArray};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

/// Spark `CAST(integral AS BINARY)` writes the fixed-width two's-complement representation in
/// big-endian order (`NumberConverter.toBinary`). Arrow's generic cast uses native order instead.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastIntegralToBinary {
    signature: Signature,
}

impl Default for SparkCastIntegralToBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCastIntegralToBinary {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkCastIntegralToBinary {
    fn name(&self) -> &str {
        "spark_cast_integral_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [argument] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                "spark_cast_integral_to_binary",
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            argument.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(cast_integral_array_to_binary, vec![])(&args.args)
    }
}

fn cast_integral_array_to_binary(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_cast_integral_to_binary",
            (1, 1),
            args.len(),
        ));
    };
    macro_rules! to_binary {
        ($type:ty) => {
            array
                .as_primitive::<$type>()
                .iter()
                .map(|value| value.map(|value| value.to_be_bytes().to_vec()))
                .collect::<BinaryArray>()
        };
    }
    let result = match array.data_type() {
        DataType::Int8 => to_binary!(Int8Type),
        DataType::Int16 => to_binary!(Int16Type),
        DataType::Int32 => to_binary!(Int32Type),
        DataType::Int64 => to_binary!(Int64Type),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_cast_integral_to_binary",
                "TINYINT, SMALLINT, INT, or BIGINT",
                other,
            ));
        }
    };
    Ok(Arc::new(result))
}
