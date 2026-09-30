use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_common::cast::{as_int8_array, as_int16_array, as_int32_array, as_int64_array};
use datafusion_common::internal_err;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

/// `CAST(integral AS BINARY)` under Spark's legacy (non-ANSI) semantics: big-endian,
/// fixed-width bytes (`NumberConverter.toBinary`), unlike Arrow's own numeric-to-binary
/// cast kernel, which uses native (little-endian on common platforms) byte order.
/// Reference: `sql/catalyst/.../NumberConverter.scala:195-225`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntegralToBinary {
    signature: Signature,
}

impl Default for SparkIntegralToBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIntegralToBinary {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkIntegralToBinary {
    fn name(&self) -> &str {
        "spark_integral_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "return_type should not be called; return_field_from_args is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(integral_to_binary, vec![])(&args.args)
    }
}

fn integral_to_binary(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_integral_to_binary",
            (1, 1),
            args.len(),
        ));
    };
    let result: BinaryArray = match array.data_type() {
        DataType::Int8 => as_int8_array(array)?
            .iter()
            .map(|v| v.map(|v| v.to_be_bytes().to_vec()))
            .collect(),
        DataType::Int16 => as_int16_array(array)?
            .iter()
            .map(|v| v.map(|v| v.to_be_bytes().to_vec()))
            .collect(),
        DataType::Int32 => as_int32_array(array)?
            .iter()
            .map(|v| v.map(|v| v.to_be_bytes().to_vec()))
            .collect(),
        DataType::Int64 => as_int64_array(array)?
            .iter()
            .map(|v| v.map(|v| v.to_be_bytes().to_vec()))
            .collect(),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_integral_to_binary",
                "TINYINT, SMALLINT, INT, or BIGINT",
                other,
            ));
        }
    };
    Ok(Arc::new(result))
}
