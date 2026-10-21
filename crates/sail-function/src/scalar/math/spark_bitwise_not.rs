// [CREDIT]: https://github.com/apache/datafusion/blob/16c0755b51d040ebd1ca87acb7f1908d63936b74/datafusion/spark/src/function/bitwise/bitwise_not.rs

use std::sync::Arc;

use arrow::array::*;
use arrow::compute::kernels::bitwise;
use arrow::datatypes::{
    DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility, lit,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBitwiseNot {
    signature: Signature,
}

impl Default for SparkBitwiseNot {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitwiseNot {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // Untyped NULL must resolve to Spark's default integral type, INT.
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt8]),
                    TypeSignature::Exact(vec![DataType::UInt16]),
                    TypeSignature::Exact(vec![DataType::UInt32]),
                    TypeSignature::Exact(vec![DataType::UInt64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBitwiseNot {
    fn name(&self) -> &str {
        "bitwise_not"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "SparkBitwiseNot: return_type() is not used; return_field_from_args() is implemented"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[0].data_type().clone(),
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [arg] = args.as_slice() else {
            return plan_err!("bitwise_not expects exactly 1 argument");
        };
        // Select the mask after CASE/projection coercion, when the operand's width is final.
        let mask = match info.get_data_type(arg)? {
            DataType::Int8 => lit(-1_i8),
            DataType::Int16 => lit(-1_i16),
            DataType::Int32 => lit(-1_i32),
            DataType::Int64 => lit(-1_i64),
            DataType::UInt8 => lit(u8::MAX),
            DataType::UInt16 => lit(u16::MAX),
            DataType::UInt32 => lit(u32::MAX),
            DataType::UInt64 => lit(u64::MAX),
            _ => return Ok(ExprSimplifyResult::Original(args)),
        };
        // XOR evaluates the operand once and follows repaired scalar-subquery nullability.
        Ok(ExprSimplifyResult::Simplified(arg.clone() ^ mask))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("bitwise_not expects exactly 1 argument");
        }
        make_scalar_function(spark_bitwise_not, vec![])(&args.args)
    }
}

pub fn spark_bitwise_not(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = args[0].as_ref();
    match array.data_type() {
        DataType::Int8 => {
            let result: Int8Array = bitwise::bitwise_not(array.as_primitive::<Int8Type>())?;
            Ok(Arc::new(result))
        }
        DataType::Int16 => {
            let result: Int16Array = bitwise::bitwise_not(array.as_primitive::<Int16Type>())?;
            Ok(Arc::new(result))
        }
        DataType::Int32 => {
            let result: Int32Array = bitwise::bitwise_not(array.as_primitive::<Int32Type>())?;
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let result: Int64Array = bitwise::bitwise_not(array.as_primitive::<Int64Type>())?;
            Ok(Arc::new(result))
        }
        DataType::UInt8 => {
            let result: UInt8Array = bitwise::bitwise_not(array.as_primitive::<UInt8Type>())?;
            Ok(Arc::new(result))
        }
        DataType::UInt16 => {
            let result: UInt16Array = bitwise::bitwise_not(array.as_primitive::<UInt16Type>())?;
            Ok(Arc::new(result))
        }
        DataType::UInt32 => {
            let result: UInt32Array = bitwise::bitwise_not(array.as_primitive::<UInt32Type>())?;
            Ok(Arc::new(result))
        }
        DataType::UInt64 => {
            let result: UInt64Array = bitwise::bitwise_not(array.as_primitive::<UInt64Type>())?;
            Ok(Arc::new(result))
        }
        _ => {
            plan_err!(
                "bitwise_not function does not support data type: {}",
                array.data_type()
            )
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_bitwise_not_nullability() {
        let bitwise_not = SparkBitwiseNot::new();

        // --- non-nullable Int32 input ---
        let non_nullable_i32 = Arc::new(Field::new("c", DataType::Int32, false));
        let out_non_null = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_i32)],
                // single-argument function -> one scalar_argument slot (None)
                scalar_arguments: &[None],
            })
            .unwrap();

        // result should be non-nullable and the same DataType as input
        assert!(!out_non_null.is_nullable());
        assert_eq!(out_non_null.data_type(), &DataType::Int32);

        // --- nullable Int32 input ---
        let nullable_i32 = Arc::new(Field::new("c", DataType::Int32, true));
        let out_nullable = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_i32)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // result should be nullable and the same DataType as input
        assert!(out_nullable.is_nullable());
        assert_eq!(out_nullable.data_type(), &DataType::Int32);

        // --- also test another integer type (Int64) for completeness ---
        let non_nullable_i64 = Arc::new(Field::new("c", DataType::Int64, false));
        let out_i64 = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_i64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(!out_i64.is_nullable());
        assert_eq!(out_i64.data_type(), &DataType::Int64);

        let nullable_i64 = Arc::new(Field::new("c", DataType::Int64, true));
        let out_i64_null = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_i64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(out_i64_null.is_nullable());
        assert_eq!(out_i64_null.data_type(), &DataType::Int64);
    }
}
