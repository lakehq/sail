use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_utils::make_scalar_function;
use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBinarySubstring {
    signature: Signature,
    force_nullable: bool,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBinaryOverlay {
    signature: Signature,
}

impl SparkBinarySubstring {
    pub fn new(force_nullable: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            force_nullable,
        }
    }
}
impl Default for SparkBinarySubstring {
    fn default() -> Self {
        Self::new(false)
    }
}
impl SparkBinaryOverlay {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}
impl Default for SparkBinaryOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkBinarySubstring {
    fn name(&self) -> &str {
        // The flag has to survive the physical codec, which reconstructs a UDF from its name,
        // so it is part of the name -- the same encoding `SparkOverlay` uses for its own.
        if self.force_nullable {
            "spark_binary_substring_nullable"
        } else {
            "spark_binary_substring"
        }
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
        if !(2..=3).contains(&args.arg_fields.len()) {
            return Err(invalid_arg_count_exec_err(
                "spark_binary_substring",
                (2, 3),
                args.arg_fields.len(),
            ));
        }
        if args.arg_fields[0].data_type() != &DataType::Binary {
            return Err(unsupported_data_types_exec_err(
                "spark_binary_substring",
                "BINARY, integral position [, integral length]",
                &args.arg_fields.iter().map(|field| field.data_type().clone()).collect::<Vec<_>>(),
            ));
        }
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            self.force_nullable || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(binary_substring, vec![])(&args.args)
    }
}

fn slice(bytes: &[u8], pos: i64, length: i64) -> Vec<u8> {
    if pos > bytes.len() as i64 {
        return vec![];
    }
    let start = if pos > 0 {
        pos - 1
    } else if pos < 0 {
        bytes.len() as i64 + pos
    } else {
        0
    };
    let end = if bytes.len() as i64 - start < length {
        bytes.len() as i64
    } else {
        start.saturating_add(length)
    };
    let start = start.max(0);
    if start >= end {
        vec![]
    } else {
        bytes[start as usize..end.min(bytes.len() as i64) as usize].to_vec()
    }
}

fn binary_substring(args: &[ArrayRef]) -> Result<ArrayRef> {
    let ([input, position] | [input, position, _]) = args else {
        return Err(invalid_arg_count_exec_err("spark_binary_substring", (2, 3), args.len()));
    };
    let input = input.as_binary::<i32>();
    let position = position.as_primitive::<Int64Type>();
    let length = args.get(2).map(|arg| arg.as_primitive::<Int64Type>());
    let result = (0..input.len())
        .map(|index| {
            match (
                input.is_null(index),
                position.is_null(index),
                length.is_some_and(|arg| arg.is_null(index)),
            ) {
                (true, _, _) | (_, true, _) | (_, _, true) => None,
                _ => Some(slice(
                    input.value(index),
                    position.value(index),
                    length.map_or(i64::MAX, |arg| arg.value(index)),
                )),
            }
        })
        .collect::<BinaryArray>();
    Ok(Arc::new(result))
}

impl ScalarUDFImpl for SparkBinaryOverlay {
    fn name(&self) -> &str {
        "spark_binary_overlay"
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
        if !(3..=4).contains(&args.arg_fields.len()) {
            return Err(invalid_arg_count_exec_err(
                "spark_binary_overlay",
                (3, 4),
                args.arg_fields.len(),
            ));
        }
        if args.arg_fields[0].data_type() != &DataType::Binary
            || args.arg_fields[1].data_type() != &DataType::Binary
        {
            return Err(unsupported_data_types_exec_err(
                "spark_binary_overlay",
                "BINARY, BINARY, integral position [, integral length]",
                &args.arg_fields.iter().map(|field| field.data_type().clone()).collect::<Vec<_>>(),
            ));
        }
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(binary_overlay, vec![])(&args.args)
    }
}

fn binary_overlay(args: &[ArrayRef]) -> Result<ArrayRef> {
    let ([input, replacement, position] | [input, replacement, position, _]) = args else {
        return Err(invalid_arg_count_exec_err("spark_binary_overlay", (3, 4), args.len()));
    };
    let input = input.as_binary::<i32>();
    let replacement = replacement.as_binary::<i32>();
    let position = position.as_primitive::<Int64Type>();
    let length = args.get(3).map(|arg| arg.as_primitive::<Int64Type>());
    let result = (0..input.len())
        .map(|index| {
            if input.is_null(index)
                || replacement.is_null(index)
                || position.is_null(index)
                || length.is_some_and(|arg| arg.is_null(index))
            {
                return None;
            }
            let bytes = input.value(index);
            let replacement = replacement.value(index);
            let pos = position.value(index);
            let count = length.map_or(replacement.len() as i64, |arg| {
                let count = arg.value(index);
                if count < 0 {
                    replacement.len() as i64
                } else {
                    count
                }
            });
            let mut value = slice(bytes, 1, pos.saturating_sub(1));
            value.extend_from_slice(replacement);
            value.extend_from_slice(&slice(bytes, pos.saturating_add(count), i64::MAX));
            Some(value)
        })
        .collect::<BinaryArray>();
    Ok(Arc::new(result))
}
