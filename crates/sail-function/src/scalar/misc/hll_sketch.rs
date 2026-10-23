use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Int64Builder, new_null_array,
};
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

use crate::functions_utils::make_scalar_function;
use crate::hll_sketch::{estimate_hll_sketch, union_hll_sketch_bytes};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllSketchEstimateFunction {
    signature: Signature,
}

impl Default for HllSketchEstimateFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketchEstimateFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Null]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for HllSketchEstimateFunction {
    fn name(&self) -> &str {
        "hll_sketch_estimate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_misc_hll_sketch_typing::hll_sketch_estimate_function_return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!(
                "hll_sketch_estimate requires 1 argument, got {}",
                args.len()
            );
        }
        make_scalar_function(hll_sketch_estimate_inner, vec![])(&args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllUnionFunction {
    signature: Signature,
}

impl Default for HllUnionFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HllUnionFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for HllUnionFunction {
    fn name(&self) -> &str {
        "hll_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_misc_hll_sketch_typing::hll_union_function_return_type(
            arg_types,
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::Boolean(Some(false)),
            ));
        }
        if args.len() != 3 {
            return exec_err!("hll_union requires 2 or 3 arguments, got {}", args.len());
        }
        make_scalar_function(hll_union_inner, vec![])(&args)
    }
}

fn hll_sketch_estimate_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if matches!(args[0].data_type(), DataType::Null) {
        return Ok(new_null_array(&DataType::Int64, args[0].len()));
    }
    let sketches = as_binary_array(&args[0], "hll_sketch_estimate")?;
    let mut builder = Int64Builder::with_capacity(sketches.len());
    for row in 0..sketches.len() {
        if sketches.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(estimate_hll_sketch(
                sketches.value(row),
                "hll_sketch_estimate",
            )?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn hll_union_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args
        .iter()
        .any(|arg| matches!(arg.data_type(), DataType::Null))
    {
        return Ok(new_null_array(&DataType::Binary, args[0].len()));
    }
    let left = as_binary_array(&args[0], "hll_union")?;
    let right = as_binary_array(&args[1], "hll_union")?;
    let allow_different_lg_config_k = as_bool_array(&args[2], "hll_union")?;
    let mut builder = BinaryBuilder::new();
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) || allow_different_lg_config_k.is_null(row) {
            builder.append_null();
        } else {
            let value = union_hll_sketch_bytes(
                left.value(row),
                right.value(row),
                allow_different_lg_config_k.value(row),
                "hll_union",
            )?;
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn as_binary_array<'a>(array: &'a ArrayRef, function_name: &str) -> Result<&'a BinaryArray> {
    array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        datafusion_common::DataFusionError::Internal(format!(
            "{function_name} expected BinaryArray, got {}",
            array.data_type()
        ))
    })
}

fn as_bool_array<'a>(array: &'a ArrayRef, function_name: &str) -> Result<&'a BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{function_name} expected BooleanArray, got {}",
                array.data_type()
            ))
        })
}
