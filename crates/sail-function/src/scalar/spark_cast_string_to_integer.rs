use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastStringToInteger {
    signature: Signature,
    data_type: DataType,
}

impl SparkCastStringToInteger {
    pub fn try_new(data_type: DataType) -> Result<Self> {
        if !data_type.is_integer() {
            return plan_err!("expected integer cast target, got {data_type}");
        }
        Ok(Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            data_type,
        })
    }

    pub fn target_type(&self) -> &DataType {
        &self.data_type
    }
}

impl ScalarUDFImpl for SparkCastStringToInteger {
    fn name(&self) -> &str {
        "spark_cast_string_to_integer"
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [arg] = args else {
            return plan_err!("{} expects one argument", self.name());
        };
        // Grouping and aggregate fields must distinguish casts to different types.
        Ok(format!(
            "{}({} AS {})",
            self.name(),
            arg.schema_name(),
            self.data_type
        ))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.data_type.clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let cast = match self.data_type {
            DataType::Int8 => cast_string_array::<Int8Type>,
            DataType::Int16 => cast_string_array::<Int16Type>,
            DataType::Int32 => cast_string_array::<Int32Type>,
            DataType::Int64 => cast_string_array::<Int64Type>,
            DataType::UInt8 => cast_string_array::<UInt8Type>,
            DataType::UInt16 => cast_string_array::<UInt16Type>,
            DataType::UInt32 => cast_string_array::<UInt32Type>,
            DataType::UInt64 => cast_string_array::<UInt64Type>,
            ref other => {
                return Err(unsupported_data_type_exec_err(
                    self.name(),
                    "integer",
                    other,
                ));
            }
        };
        make_scalar_function(cast, vec![])(&args.args)
    }
}

fn cast_string_array<T>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i128>,
{
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_cast_string_to_integer",
            (1, 1),
            args.len(),
        ));
    };
    let result = match array.data_type() {
        DataType::Utf8 => parse_string_values::<T>(as_string_array(array)?.iter()),
        DataType::LargeUtf8 => parse_string_values::<T>(as_large_string_array(array)?.iter()),
        DataType::Utf8View => parse_string_values::<T>(as_string_view_array(array)?.iter()),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_cast_string_to_integer",
                "STRING",
                other,
            ));
        }
    };
    Ok(Arc::new(result))
}

fn parse_string_values<'a, T>(values: impl Iterator<Item = Option<&'a str>>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i128>,
{
    values
        .map(|value| value.and_then(parse_spark_legacy_string_to_integer::<T::Native>))
        .collect()
}

// Spark's UTF8String.toInt/toLong allow decimals, validate the entire fraction,
// and trim ASCII whitespace/control bytes. Parse exactly before checking the
// target range; i128 also holds every value of Sail's unsigned integer types.
fn parse_spark_legacy_string_to_integer<T: TryFrom<i128>>(value: &str) -> Option<T> {
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

    let mut result = 0_i128;
    while offset <= end {
        let byte = bytes[offset];
        offset += 1;
        if byte == b'.' {
            break;
        }
        if !byte.is_ascii_digit() {
            return None;
        }
        result = result
            .checked_mul(10)?
            .checked_sub(i128::from(byte - b'0'))?;
    }

    while offset <= end {
        if !bytes[offset].is_ascii_digit() {
            return None;
        }
        offset += 1;
    }

    T::try_from(if negative {
        result
    } else {
        result.checked_neg()?
    })
    .ok()
}

fn is_spark_trim_byte(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    use datafusion_common::{ScalarValue, exec_err};

    use super::*;

    #[test]
    fn test_parse_spark_legacy_string_to_integer() {
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
                parse_spark_legacy_string_to_integer::<i32>(input),
                expected,
                "input: {input:?}"
            );
        }
    }

    #[test]
    fn test_parse_integer_ranges() {
        macro_rules! check {
            ($($integer:ty),*) => {
                $(
                    let min = <$integer>::MIN;
                    let max = <$integer>::MAX;
                    for (input, expected) in [
                        (min.to_string(), Some(min)),
                        (max.to_string(), Some(max)),
                        (format!("{min}.999"), Some(min)),
                        (format!("{max}.999"), Some(max)),
                        ((i128::from(min) - 1).to_string(), None),
                        ((i128::from(max) + 1).to_string(), None),
                        ("9".repeat(100), None),
                        ("USD".to_string(), None),
                    ] {
                        assert_eq!(
                            parse_spark_legacy_string_to_integer::<$integer>(&input),
                            expected,
                            "{}: {input:?}", stringify!($integer),
                        );
                    }
                )*
            };
        }
        check!(i8, i16, i32, i64, u8, u16, u32, u64);
    }

    #[test]
    fn test_cast_all_string_representations_and_integer_targets() -> Result<()> {
        fn check<T>() -> Result<()>
        where
            T: ArrowPrimitiveType,
            T::Native: TryFrom<i128> + std::fmt::Display,
        {
            for values in [vec![Some("100"), Some("1.23"), Some("USD"), None], vec![]] {
                let inputs = [
                    Arc::new(StringArray::from(values.clone())) as ArrayRef,
                    Arc::new(LargeStringArray::from(values.clone())) as ArrayRef,
                    Arc::new(StringViewArray::from(values.clone())) as ArrayRef,
                ];
                for input in inputs {
                    let output = cast_string_array::<T>(&[input])?;
                    assert_eq!(output.data_type(), &T::DATA_TYPE);
                    let Some(output) = output.as_any().downcast_ref::<PrimitiveArray<T>>() else {
                        return exec_err!("expected integer array");
                    };
                    let expected = if values.is_empty() {
                        vec![]
                    } else {
                        vec![Some("100".to_string()), Some("1".to_string()), None, None]
                    };
                    assert_eq!(
                        output
                            .iter()
                            .map(|v| v.map(|v| v.to_string()))
                            .collect::<Vec<_>>(),
                        expected,
                    );
                }
            }
            for value in [Some("1.23".to_string()), None] {
                for input in [
                    ScalarValue::Utf8(value.clone()),
                    ScalarValue::LargeUtf8(value.clone()),
                    ScalarValue::Utf8View(value.clone()),
                ] {
                    let output = make_scalar_function(cast_string_array::<T>, vec![])(&[
                        ColumnarValue::Scalar(input),
                    ])?;
                    let ColumnarValue::Scalar(output) = output else {
                        return exec_err!("expected scalar result");
                    };
                    assert_eq!(output.data_type(), T::DATA_TYPE);
                    assert_eq!(output.is_null(), value.is_none());
                    if value.is_some() {
                        assert_eq!(output.to_string(), "1");
                    }
                }
            }
            Ok(())
        }
        check::<Int8Type>()?;
        check::<Int16Type>()?;
        check::<Int32Type>()?;
        check::<Int64Type>()?;
        check::<UInt8Type>()?;
        check::<UInt16Type>()?;
        check::<UInt32Type>()?;
        check::<UInt64Type>()?;
        Ok(())
    }

    #[test]
    fn test_return_field_is_nullable_for_non_nullable_input() -> Result<()> {
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            let udf = SparkCastStringToInteger::try_new(data_type.clone())?;
            let input = Arc::new(Field::new("value", DataType::Utf8, false));
            let arg_fields = [input];
            let scalar_arguments = [None];
            let field = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })?;

            assert_eq!(field.data_type(), &data_type);
            assert!(field.is_nullable());
        }
        Ok(())
    }
}
