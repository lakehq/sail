use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastStringToInt32 {
    signature: Signature,
}

impl Default for SparkCastStringToInt32 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCastStringToInt32 {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkCastStringToInt32 {
    fn name(&self) -> &str {
        "spark_cast_string_to_int32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(cast_string_array_to_int32, vec![])(&args.args)
    }
}

fn cast_string_array_to_int32(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_cast_string_to_int32",
            (1, 1),
            args.len(),
        ));
    };
    let result = match array.data_type() {
        DataType::Utf8 => parse_string_values(as_string_array(array)?.iter()),
        DataType::LargeUtf8 => parse_string_values(as_large_string_array(array)?.iter()),
        DataType::Utf8View => parse_string_values(as_string_view_array(array)?.iter()),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_cast_string_to_int32",
                "STRING",
                other,
            ));
        }
    };
    Ok(Arc::new(result))
}

fn parse_string_values<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Int32Array {
    values
        .map(|value| value.and_then(parse_spark_legacy_string_to_i32))
        .collect()
}

fn parse_spark_legacy_string_to_i32(value: &str) -> Option<i32> {
    let bytes = value.as_bytes();
    let mut offset = 0;
    while offset < bytes.len() && is_spark_trim_byte(bytes[offset]) {
        offset += 1;
    }
    if offset == bytes.len() {
        return None;
    }

    let mut end = bytes.len() - 1;
    while end > offset && is_spark_trim_byte(bytes[end]) {
        end -= 1;
    }

    let negative = bytes[offset] == b'-';
    if negative || bytes[offset] == b'+' {
        if offset == end {
            return None;
        }
        offset += 1;
    }

    let stop_value = i32::MIN / 10;
    let mut result = 0_i32;
    while offset <= end {
        let byte = bytes[offset];
        offset += 1;
        if byte == b'.' {
            break;
        }
        if !byte.is_ascii_digit() || result < stop_value {
            return None;
        }
        result = result.checked_mul(10)?.checked_sub((byte - b'0') as i32)?;
    }

    while offset <= end {
        if !bytes[offset].is_ascii_digit() {
            return None;
        }
        offset += 1;
    }

    if negative {
        Some(result)
    } else {
        result.checked_neg()
    }
}

fn is_spark_trim_byte(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    use datafusion_common::exec_err;

    use super::*;

    #[test]
    fn test_parse_spark_legacy_string_to_i32() {
        let cases = [
            ("100", Some(100)),
            (" +100 ", Some(100)),
            ("1.23", Some(1)),
            ("-4.56", Some(-4)),
            (".9", Some(0)),
            ("1.", Some(1)),
            (".", Some(0)),
            ("2147483647.999", Some(i32::MAX)),
            ("-2147483648.999", Some(i32::MIN)),
            ("2147483648", None),
            ("2178802287", None),
            ("-2147483649", None),
            ("2147483648.0", None),
            ("123.a", None),
            ("1e2", None),
            ("", None),
            ("+", None),
        ];

        for (input, expected) in cases {
            assert_eq!(
                parse_spark_legacy_string_to_i32(input),
                expected,
                "input: {input:?}"
            );
        }
    }

    #[test]
    fn test_cast_all_string_array_representations_to_nullable_int32() -> Result<()> {
        let values = vec![
            Some("100"),
            Some("1.23"),
            Some("-4.56"),
            Some("2147483647"),
            Some("2178802287"),
            Some("2147483648.0"),
            Some("bad"),
            None,
        ];
        let expected = vec![
            Some(100),
            Some(1),
            Some(-4),
            Some(i32::MAX),
            None,
            None,
            None,
            None,
        ];
        let inputs = [
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
            Arc::new(LargeStringArray::from(values.clone())) as ArrayRef,
            Arc::new(StringViewArray::from(values)) as ArrayRef,
        ];

        for input in inputs {
            let output = cast_string_array_to_int32(&[input])?;
            assert_eq!(output.data_type(), &DataType::Int32);
            let Some(output) = output.as_any().downcast_ref::<Int32Array>() else {
                return exec_err!("expected Int32Array");
            };
            assert_eq!(output.iter().collect::<Vec<_>>(), expected);
        }
        Ok(())
    }

    #[test]
    fn test_return_field_is_nullable_for_non_nullable_input() -> Result<()> {
        let udf = SparkCastStringToInt32::new();
        let input = Arc::new(Field::new("value", DataType::Utf8, false));
        let arg_fields = [input];
        let scalar_arguments = [None];
        let field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;

        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());
        Ok(())
    }
}
