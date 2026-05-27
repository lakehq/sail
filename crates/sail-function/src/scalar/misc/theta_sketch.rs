use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Array, Int64Builder};
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

use crate::functions_utils::make_scalar_function;
use crate::theta_sketch::{
    difference_sketch_bytes, estimate_sketch, intersect_sketch_bytes, union_sketch_bytes,
    validate_lg_nom_entries, DEFAULT_LG_NOM_ENTRIES,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaSketchEstimateFunction {
    signature: Signature,
}

impl Default for ThetaSketchEstimateFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaSketchEstimateFunction {
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

impl ScalarUDFImpl for ThetaSketchEstimateFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_sketch_estimate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!(
                "theta_sketch_estimate requires 1 argument, got {}",
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Binary | DataType::Null => Ok(DataType::Int64),
            ref data_type => {
                exec_err!("theta_sketch_estimate requires a binary argument, got {data_type}")
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!(
                "theta_sketch_estimate requires 1 argument, got {}",
                args.len()
            );
        }
        make_scalar_function(theta_sketch_estimate_inner, vec![])(&args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaUnionFunction {
    signature: Signature,
}

impl Default for ThetaUnionFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaUnionFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ThetaUnionFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_binary_set_op_types("theta_union", arg_types, true)?;
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::Int32(Some(DEFAULT_LG_NOM_ENTRIES)),
            ));
        }
        if args.len() != 3 {
            return exec_err!("theta_union requires 2 or 3 arguments, got {}", args.len());
        }
        make_scalar_function(theta_union_inner, vec![])(&args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaIntersectionFunction {
    signature: Signature,
}

impl Default for ThetaIntersectionFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaIntersectionFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Null, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Null]),
                    TypeSignature::Exact(vec![DataType::Null, DataType::Null]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ThetaIntersectionFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_intersection"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_binary_set_op_types("theta_intersection", arg_types, false)?;
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return exec_err!(
                "theta_intersection requires 2 arguments, got {}",
                args.len()
            );
        }
        make_scalar_function(theta_intersection_inner, vec![])(&args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaDifferenceFunction {
    signature: Signature,
}

impl Default for ThetaDifferenceFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaDifferenceFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Null, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Null]),
                    TypeSignature::Exact(vec![DataType::Null, DataType::Null]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ThetaDifferenceFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_difference"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_binary_set_op_types("theta_difference", arg_types, false)?;
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return exec_err!("theta_difference requires 2 arguments, got {}", args.len());
        }
        make_scalar_function(theta_difference_inner, vec![])(&args)
    }
}

fn theta_sketch_estimate_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let sketches = as_binary_array(&args[0], "theta_sketch_estimate")?;
    let mut builder = Int64Builder::with_capacity(sketches.len());
    for row in 0..sketches.len() {
        if sketches.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(estimate_sketch(
                sketches.value(row),
                "theta_sketch_estimate",
            )?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn theta_union_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_binary_array(&args[0], "theta_union")?;
    let right = as_binary_array(&args[1], "theta_union")?;
    let lg_nom_entries = as_int32_array(&args[2], "theta_union")?;
    let mut builder = BinaryBuilder::new();
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) || lg_nom_entries.is_null(row) {
            builder.append_null();
        } else {
            let lg_nom_entries = validate_lg_nom_entries(lg_nom_entries.value(row), "theta_union")?;
            let value = union_sketch_bytes(
                left.value(row),
                right.value(row),
                lg_nom_entries,
                "theta_union",
            )?;
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn theta_intersection_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_binary_array(&args[0], "theta_intersection")?;
    let right = as_binary_array(&args[1], "theta_intersection")?;
    let mut builder = BinaryBuilder::new();
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) {
            builder.append_null();
        } else {
            let value =
                intersect_sketch_bytes(left.value(row), right.value(row), "theta_intersection")?;
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn theta_difference_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_binary_array(&args[0], "theta_difference")?;
    let right = as_binary_array(&args[1], "theta_difference")?;
    let mut builder = BinaryBuilder::new();
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) {
            builder.append_null();
        } else {
            let value =
                difference_sketch_bytes(left.value(row), right.value(row), "theta_difference")?;
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn validate_binary_set_op_types(
    function_name: &str,
    arg_types: &[DataType],
    has_lg_nom_entries: bool,
) -> Result<()> {
    let expected = if has_lg_nom_entries { 2..=3 } else { 2..=2 };
    if !expected.contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} received invalid argument count: {}",
            arg_types.len()
        );
    }
    for data_type in &arg_types[0..2] {
        if !matches!(data_type, DataType::Binary | DataType::Null) {
            return exec_err!("{function_name} requires binary sketch arguments, got {data_type}");
        }
    }
    if arg_types.len() == 3 && !matches!(arg_types[2], DataType::Int32 | DataType::Null) {
        return exec_err!(
            "{function_name} requires an integer lgNomEntries argument, got {}",
            arg_types[2]
        );
    }
    Ok(())
}

fn as_binary_array<'a>(array: &'a ArrayRef, function_name: &str) -> Result<&'a BinaryArray> {
    array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        datafusion_common::DataFusionError::Internal(format!(
            "{function_name} expected BinaryArray, got {}",
            array.data_type()
        ))
    })
}

fn as_int32_array<'a>(array: &'a ArrayRef, function_name: &str) -> Result<&'a Int32Array> {
    array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        datafusion_common::DataFusionError::Internal(format!(
            "{function_name} expected Int32Array, got {}",
            array.data_type()
        ))
    })
}
