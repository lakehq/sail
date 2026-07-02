use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, FixedSizeBinaryArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PadSide {
    Left,
    Right,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBinaryPad {
    side: PadSide,
    signature: Signature,
}

impl SparkBinaryPad {
    pub fn new(side: PadSide) -> Self {
        Self {
            side,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBinaryPad {
    fn name(&self) -> &str {
        match self.side {
            PadSide::Left => "spark_binary_lpad",
            PadSide::Right => "spark_binary_rpad",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!(
                "Spark binary pad requires 2 or 3 arguments, got {}",
                args.args.len()
            );
        }
        let arrays = columnar_values_to_arrays(&args.args, args.number_rows)?;
        let input = &arrays[0];
        let len = &arrays[1];
        let pad = arrays.get(2);
        let default_pad = [0_u8];
        let mut builder = BinaryBuilder::with_capacity(input.len(), input.len());
        for row in 0..input.len() {
            let Some(value) = binary_value(input, row)? else {
                builder.append_null();
                continue;
            };
            let Some(target_len) = int64_value(len, row)? else {
                builder.append_null();
                continue;
            };
            let pad = if let Some(pad) = pad {
                let Some(pad) = binary_value(pad, row)? else {
                    builder.append_null();
                    continue;
                };
                pad
            } else {
                &default_pad
            };
            builder.append_value(pad_bytes(value, target_len, pad, self.side));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&arg_types.len()) {
            return exec_err!(
                "Spark binary pad requires 2 or 3 arguments, got {}",
                arg_types.len()
            );
        }
        let mut out = vec![coerce_binary(&arg_types[0])?, DataType::Int64];
        if let Some(pad) = arg_types.get(2) {
            out.push(coerce_binary(pad)?);
        }
        Ok(out)
    }
}

fn coerce_binary(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Null
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok(DataType::Binary),
        other => exec_err!("Spark binary string function expects BINARY, got {other:?}"),
    }
}

fn columnar_values_to_arrays(args: &[ColumnarValue], number_rows: usize) -> Result<Vec<ArrayRef>> {
    if args
        .iter()
        .any(|arg| matches!(arg, ColumnarValue::Array(_)))
    {
        ColumnarValue::values_to_arrays(args)
    } else {
        args.iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => value.to_array_of_size(number_rows),
                ColumnarValue::Array(_) => unreachable!(),
            })
            .collect()
    }
}

fn binary_value(array: &ArrayRef, row: usize) -> Result<Option<&[u8]>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Binary => Ok(Some(array.as_binary::<i32>().value(row))),
        DataType::LargeBinary => Ok(Some(array.as_binary::<i64>().value(row))),
        DataType::BinaryView => Ok(Some(array.as_binary_view().value(row))),
        DataType::FixedSizeBinary(_) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "expected FixedSizeBinaryArray, got {}",
                        array.data_type()
                    ))
                })?;
            Ok(Some(array.value(row)))
        }
        DataType::Null => Ok(None),
        other => exec_err!("Spark binary string function expects BINARY, got {other:?}"),
    }
}

fn int64_value(array: &ArrayRef, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int64 => Ok(Some(
            array
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(row),
        )),
        other => exec_err!("Spark binary pad length expects BIGINT, got {other:?}"),
    }
}

fn pad_bytes(value: &[u8], target_len: i64, pad: &[u8], side: PadSide) -> Vec<u8> {
    if target_len <= 0 {
        return vec![];
    }
    let target_len = target_len as usize;
    if value.len() >= target_len {
        return value[..target_len].to_vec();
    }
    if pad.is_empty() {
        return value.to_vec();
    }
    let fill_len = target_len - value.len();
    let mut out = Vec::with_capacity(target_len);
    if matches!(side, PadSide::Left) {
        append_repeated_pad(&mut out, pad, fill_len);
        out.extend_from_slice(value);
    } else {
        out.extend_from_slice(value);
        append_repeated_pad(&mut out, pad, fill_len);
    }
    out
}

fn append_repeated_pad(out: &mut Vec<u8>, pad: &[u8], len: usize) {
    for i in 0..len {
        out.push(pad[i % pad.len()]);
    }
}
