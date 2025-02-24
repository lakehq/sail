use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArrayType, BinaryBuilder, GenericBinaryBuilder,
    GenericStringBuilder, LargeBinaryBuilder, OffsetSizeTrait, StringArrayType,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::ScalarUDFImpl;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

#[derive(Debug)]
pub struct SparkEncode {
    signature: Signature,
}

impl Default for SparkEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkEncode {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkEncode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types[0], DataType::Null) || matches!(arg_types[1], DataType::Null) {
            Ok(DataType::Binary)
        } else {
            match &arg_types[0] {
                DataType::Null | DataType::Utf8 | DataType::Utf8View => Ok(DataType::Binary),
                DataType::LargeUtf8 => Ok(DataType::LargeBinary),
                other => {
                    exec_err!("Spark `encode` function: Expected a STRING type, got {other:?}")
                }
            }
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `encode` function requires 2 arguments, got {}",
                args.len()
            );
        }

        if matches!(
            args[0],
            ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
        ) || matches!(
            args[1],
            ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
        ) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(scalar_string), ColumnarValue::Scalar(char_set)) => {
                match (scalar_string, char_set) {
                    (
                        ScalarValue::Utf8(Some(string))
                        | ScalarValue::LargeUtf8(Some(string))
                        | ScalarValue::Utf8View(Some(string)),
                        ScalarValue::Utf8(Some(char_set))
                        | ScalarValue::LargeUtf8(Some(char_set))
                        | ScalarValue::Utf8View(Some(char_set)),
                    ) => {
                        let result = encode(string, char_set)?;
                        match scalar_string {
                            ScalarValue::LargeUtf8(_) => Ok(ColumnarValue::Scalar(
                                ScalarValue::LargeBinary(Some(result)),
                            )),
                            _ => Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(result)))),
                        }
                    }
                    _ => {
                        exec_err!("Spark `encode` function: Args must be STRING type, got {args:?}")
                    }
                }
            }
            (ColumnarValue::Array(string_array), ColumnarValue::Scalar(char_set)) => {
                match (string_array.data_type(), char_set) {
                    (
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                        ScalarValue::Utf8(Some(char_set))
                        | ScalarValue::LargeUtf8(Some(char_set))
                        | ScalarValue::Utf8View(Some(char_set)),
                    ) => {
                        let result = match string_array.data_type() {
                            DataType::Utf8 => {
                                let string_array = string_array.as_string::<i32>();
                                let mut builder = BinaryBuilder::with_capacity(
                                    string_array.len(),
                                    string_array.len(),
                                );
                                for string in string_array.iter() {
                                    if let Some(string) = string {
                                        builder.append_value(encode(string, char_set)?);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                Ok(Arc::new(builder.finish()) as ArrayRef)
                            }
                            DataType::LargeUtf8 => {
                                let string_array = string_array.as_string::<i64>();
                                let mut builder = LargeBinaryBuilder::with_capacity(
                                    string_array.len(),
                                    string_array.len(),
                                );
                                for string in string_array.iter() {
                                    if let Some(string) = string {
                                        builder.append_value(encode(string, char_set)?);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                Ok(Arc::new(builder.finish()) as ArrayRef)
                            }
                            DataType::Utf8View => {
                                let string_array = string_array.as_string_view();
                                let mut builder = BinaryBuilder::with_capacity(
                                    string_array.len(),
                                    string_array.len(),
                                );
                                for string in string_array.iter() {
                                    if let Some(string) = string {
                                        builder.append_value(encode(string, char_set)?);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                Ok(Arc::new(builder.finish()) as ArrayRef)
                            }
                            other => {
                                exec_err!("Spark `encode` function: First arg must be STRING, got {other:?}")
                            }
                        }?;
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                    _ => exec_err!("Spark `encode` function: Args must be STRING, got {args:?}"),
                }
            }
            (ColumnarValue::Array(string_array), ColumnarValue::Array(char_set_array)) => {
                let result = match (string_array.data_type(), char_set_array.data_type()) {
                    (DataType::Utf8, DataType::Utf8) => process_encode_arrays::<_, _, i32>(
                        string_array.as_string::<i32>(),
                        char_set_array.as_string::<i32>(),
                    ),
                    (DataType::Utf8, DataType::LargeUtf8) => process_encode_arrays::<_, _, i32>(
                        string_array.as_string::<i32>(),
                        char_set_array.as_string::<i64>(),
                    ),
                    (DataType::Utf8, DataType::Utf8View) => process_encode_arrays::<_, _, i32>(
                        string_array.as_string::<i32>(),
                        char_set_array.as_string_view(),
                    ),
                    (DataType::LargeUtf8, DataType::Utf8) => process_encode_arrays::<_, _, i64>(
                        string_array.as_string::<i64>(),
                        char_set_array.as_string::<i32>(),
                    ),
                    (DataType::LargeUtf8, DataType::LargeUtf8) => {
                        process_encode_arrays::<_, _, i64>(
                            string_array.as_string::<i64>(),
                            char_set_array.as_string::<i64>(),
                        )
                    }
                    (DataType::LargeUtf8, DataType::Utf8View) => {
                        process_encode_arrays::<_, _, i64>(
                            string_array.as_string::<i64>(),
                            char_set_array.as_string_view(),
                        )
                    }
                    (DataType::Utf8View, DataType::Utf8) => process_encode_arrays::<_, _, i32>(
                        string_array.as_string_view(),
                        char_set_array.as_string::<i32>(),
                    ),
                    (DataType::Utf8View, DataType::LargeUtf8) => {
                        process_encode_arrays::<_, _, i32>(
                            string_array.as_string_view(),
                            char_set_array.as_string::<i64>(),
                        )
                    }
                    (DataType::Utf8View, DataType::Utf8View) => process_encode_arrays::<_, _, i32>(
                        string_array.as_string_view(),
                        char_set_array.as_string_view(),
                    ),
                    (left, right) => {
                        exec_err!(
                            "Spark `encode` function: Args must be STRING, got {left:?}, {right:?}"
                        )
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            _ => exec_err!("Unsupported args {args:?} for Spark function `encode`"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `encode` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        arg_types
            .iter()
            .map(|arg_type| match arg_type {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                    Ok(arg_type.clone())
                }
                other => {
                    exec_err!("Spark `encode` function: Expected a STRING type, got {other:?}")
                }
            })
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Debug)]
pub struct SparkDecode {
    signature: Signature,
}

impl Default for SparkDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDecode {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDecode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types[0], DataType::Null) || matches!(arg_types[1], DataType::Null) {
            Ok(DataType::Utf8)
        } else {
            match &arg_types[0] {
                DataType::Null
                | DataType::Binary
                | DataType::FixedSizeBinary(_)
                | DataType::BinaryView
                | DataType::Utf8
                | DataType::Utf8View => Ok(DataType::Utf8),
                DataType::LargeUtf8 | DataType::LargeBinary => Ok(DataType::LargeUtf8),
                other => {
                    exec_err!("Spark `decode` function: Expected a BINARY type, got {other:?}")
                }
            }
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `decode` function requires 2 arguments, got {}",
                args.len()
            );
        }

        if matches!(
            args[0],
            ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Binary(None))
                | ColumnarValue::Scalar(ScalarValue::LargeBinary(None))
                | ColumnarValue::Scalar(ScalarValue::BinaryView(None))
        ) || matches!(
            args[1],
            ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
        ) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(scalar_bytes), ColumnarValue::Scalar(char_set)) => {
                match (scalar_bytes, char_set) {
                    (
                        ScalarValue::Binary(Some(bytes))
                        | ScalarValue::LargeBinary(Some(bytes))
                        | ScalarValue::BinaryView(Some(bytes)),
                        ScalarValue::Utf8(Some(char_set))
                        | ScalarValue::LargeUtf8(Some(char_set))
                        | ScalarValue::Utf8View(Some(char_set)),
                    ) => {
                        let result = decode(bytes, char_set)?;
                        match scalar_bytes {
                            ScalarValue::LargeBinary(_) => {
                                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                            }
                            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result)))),
                        }
                    }
                    _ => {
                        exec_err!("Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {args:?}")
                    }
                }
            }
            (ColumnarValue::Array(binary_array), ColumnarValue::Scalar(char_set)) => {
                match (binary_array.data_type(), char_set) {
                    (
                        DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView,
                        ScalarValue::Utf8(Some(char_set))
                        | ScalarValue::LargeUtf8(Some(char_set))
                        | ScalarValue::Utf8View(Some(char_set)),
                    ) => {
                        let result = match binary_array.data_type() {
                            DataType::Binary => {
                                let binary_array = binary_array.as_binary::<i32>();
                                let mut builder: GenericStringBuilder<i32> =
                                    GenericStringBuilder::new();
                                for bytes in binary_array.iter() {
                                    if let Some(bytes) = bytes {
                                        builder.append_value(decode(bytes, char_set)?);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                Ok(Arc::new(builder.finish()) as ArrayRef)
                            }
                            DataType::LargeBinary => {
                                let binary_array = binary_array.as_binary::<i64>();
                                let mut builder: GenericStringBuilder<i64> =
                                    GenericStringBuilder::new();
                                for bytes in binary_array.iter() {
                                    if let Some(bytes) = bytes {
                                        builder.append_value(decode(bytes, char_set)?);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                Ok(Arc::new(builder.finish()) as ArrayRef)
                            }
                            DataType::BinaryView => {
                                let binary_array = binary_array.as_binary_view();
                                let mut builder: GenericStringBuilder<i32> =
                                    GenericStringBuilder::new();
                                for bytes in binary_array.iter() {
                                    if let Some(bytes) = bytes {
                                        builder.append_value(decode(bytes, char_set)?);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                Ok(Arc::new(builder.finish()) as ArrayRef)
                            }
                            other => {
                                exec_err!("Spark `decode` function: First arg must be BINARY, got {other:?}")
                            }
                        }?;
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                    _ => exec_err!("Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {args:?}"),
                }
            }
            (ColumnarValue::Array(binary_array), ColumnarValue::Array(char_set_array)) => {
                let result = match (binary_array.data_type(), char_set_array.data_type()) {
                    (DataType::Binary, DataType::Utf8) => process_decode_arrays::<_, _, i32>(
                        binary_array.as_binary::<i32>(),
                        char_set_array.as_string::<i32>(),
                    ),
                    (DataType::Binary, DataType::LargeUtf8) => process_decode_arrays::<_, _, i32>(
                        binary_array.as_binary::<i32>(),
                        char_set_array.as_string::<i64>(),
                    ),
                    (DataType::Binary, DataType::Utf8View) => process_decode_arrays::<_, _, i32>(
                        binary_array.as_binary::<i32>(),
                        char_set_array.as_string_view(),
                    ),
                    (DataType::LargeBinary, DataType::Utf8) => process_decode_arrays::<_, _, i64>(
                        binary_array.as_binary::<i64>(),
                        char_set_array.as_string::<i32>(),
                    ),
                    (DataType::LargeBinary, DataType::LargeUtf8) => {
                        process_decode_arrays::<_, _, i64>(
                            binary_array.as_binary::<i64>(),
                            char_set_array.as_string::<i64>(),
                        )
                    }
                    (DataType::LargeBinary, DataType::Utf8View) => {
                        process_decode_arrays::<_, _, i64>(
                            binary_array.as_binary::<i64>(),
                            char_set_array.as_string_view(),
                        )
                    }
                    (DataType::BinaryView, DataType::Utf8) => process_decode_arrays::<_, _, i32>(
                        binary_array.as_binary_view(),
                        char_set_array.as_string::<i32>(),
                    ),
                    (DataType::BinaryView, DataType::LargeUtf8) => {
                        process_decode_arrays::<_, _, i32>(
                            binary_array.as_binary_view(),
                            char_set_array.as_string::<i64>(),
                        )
                    }
                    (DataType::BinaryView, DataType::Utf8View) => {
                        process_decode_arrays::<_, _, i32>(
                            binary_array.as_binary_view(),
                            char_set_array.as_string_view(),
                        )
                    }
                    (left, right) => {
                        exec_err!(
                            "Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {left:?}, {right:?}"
                        )
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            _ => exec_err!("Unsupported args {args:?} for Spark function `decode`"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `decode` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        let bytes = &arg_types[0];
        let char_set = &arg_types[1];

        let bytes = match bytes {
            DataType::Null | DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(bytes.clone())
            }
            DataType::FixedSizeBinary(_) => Ok(DataType::Binary),
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Binary),
            DataType::LargeUtf8 => Ok(DataType::LargeBinary),
            other => exec_err!("Spark `decode` function: Expected a BINARY type, got {other:?}"),
        }?;
        let char_set = match char_set {
            DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(char_set.clone())
            }
            other => exec_err!("Spark `decode` function: Expected a STRING type, got {other:?}"),
        }?;
        Ok(vec![bytes, char_set])
    }
}

fn encode(string: &str, char_set: &str) -> Result<Vec<u8>> {
    match char_set.trim().to_uppercase().as_str() {
        "US-ASCII" => {
            // 'US-ASCII': Seven-bit ASCII, ISO646-US.
            let bytes = if !string.is_ascii() {
                string.chars()
                    .map(|c| if c.is_ascii() { c as u8 } else { b'?' })
                    .collect()
            } else {
                string.as_bytes().to_vec()
            };
            Ok(bytes)
        }
        "ISO-8859-1" => {
            // 'ISO-8859-1': ISO Latin Alphabet No. 1, ISO-LATIN-1.
            Ok(string.chars()
                .map(|c| if c as u32 <= 0xFF { c as u8 } else { b'?' })
                .collect())
        }
        "UTF-8" => {
            // 'UTF-8': Eight-bit UCS Transformation Format.
            Ok(string.as_bytes().to_vec())
        }
        "UTF-16BE" => {
            // 'UTF-16BE': Sixteen-bit UCS Transformation Format, big-endian byte order.
            let mut bytes = Vec::new();
            for c in string.encode_utf16() {
                bytes.extend_from_slice(&c.to_be_bytes());
            }
            Ok(bytes)
        }
        "UTF-16LE" => {
            // 'UTF-16LE': Sixteen-bit UCS Transformation Format, little-endian byte order.
            let mut bytes = Vec::new();
            for c in string.encode_utf16() {
                bytes.extend_from_slice(&c.to_le_bytes());
            }
            Ok(bytes)
        }
        "UTF-16" => {
            // 'UTF-16': Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark.
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&[0xFE, 0xFF]); // Add BOM (0xFEFF) in BE format
            for c in string.encode_utf16() {
                bytes.extend_from_slice(&c.to_be_bytes());
            }
            Ok(bytes)
        }
        _ => exec_err!("Spark `encode` function: Unsupported charset {char_set}. Charset must be one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16'"),
    }
}

fn process_encode_arrays<'a, S, C, B>(
    string_array: &'a S,
    char_set_array: &'a C,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
    &'a C: StringArrayType<'a>,
    B: OffsetSizeTrait,
{
    let mut builder =
        GenericBinaryBuilder::<B>::with_capacity(string_array.len(), string_array.len());
    for (string, char_set) in string_array.iter().zip(char_set_array.iter()) {
        if let (Some(string), Some(char_set)) = (string, char_set) {
            builder.append_value(encode(string, char_set)?);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn decode(bytes: &[u8], char_set: &str) -> Result<String> {
    match char_set.trim().to_uppercase().as_str() {
        "US-ASCII" => {
            // 'US-ASCII': Seven-bit ASCII, ISO646-US.
            if !bytes.is_ascii() {
                exec_err!("Spark `decode` function: Invalid US-ASCII byte sequence: {bytes:?}")
            } else {
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| exec_datafusion_err!("Spark `decode` function: Failed to decode US-ASCII bytes: {e}"))
            }
        }
        "ISO-8859-1" => {
            // 'ISO-8859-1': ISO Latin Alphabet No. 1, ISO-LATIN-1.
            Ok(bytes.iter().map(|&b| b as char).collect())
        }
        "UTF-8" => {
            // 'UTF-8': Eight-bit UCS Transformation Format.
            String::from_utf8(bytes.to_vec())
                .map_err(|e| exec_datafusion_err!("Spark `decode` function: Failed to decode UTF-8 bytes: {e}"))
        }
        "UTF-16BE" => {
            // 'UTF-16BE': Sixteen-bit UCS Transformation Format, big-endian byte order.
            if bytes.len() % 2 != 0 {
                exec_err!("Spark `decode` function: Invalid UTF-16BE byte sequence: {bytes:?}")
            } else {
                let u16_words: Vec<u16> = bytes
                    .chunks_exact(2)
                    .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                    .collect();
                String::from_utf16(&u16_words)
                    .map_err(|e| exec_datafusion_err!("Spark `decode` function: Failed to decode UTF-16BE bytes: {e}"))
            }
        }
        "UTF-16LE" => {
            // 'UTF-16LE': Sixteen-bit UCS Transformation Format, little-endian byte order.
            if bytes.len() % 2 != 0 {
                exec_err!("Spark `decode` function: Invalid UTF-16LE byte sequence: {bytes:?}")
            } else {
                let u16_words: Vec<u16> = bytes
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                String::from_utf16(&u16_words)
                    .map_err(|e| exec_datafusion_err!("Spark `decode` function: Failed to decode UTF-16LE bytes: {e}"))
            }
        }
        "UTF-16" => {
            // 'UTF-16': Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark.
            if bytes.len() < 2 {
                exec_err!("Spark `decode` function: Invalid UTF-16 byte sequence: {bytes:?}")
            } else {
                let (bom, content) = match &bytes[0..2] {
                    [0xFE, 0xFF] => (true, &bytes[2..]), // BE BOM
                    [0xFF, 0xFE] => (false, &bytes[2..]), // LE BOM
                    _ => (true, bytes), // Default to BE if no BOM
                };

                if content.len() % 2 != 0 {
                    exec_err!("Spark `decode` function: Invalid UTF-16 byte sequence: {bytes:?}")
                } else {
                    let u16_words: Vec<u16> = content
                        .chunks_exact(2)
                        .map(|chunk| if bom {
                            u16::from_be_bytes([chunk[0], chunk[1]])
                        } else {
                            u16::from_le_bytes([chunk[0], chunk[1]])
                        })
                        .collect();
                    String::from_utf16(&u16_words)
                        .map_err(|e| exec_datafusion_err!("Spark `decode` function: Failed to decode UTF-16 bytes: {e}"))
                }
            }
        }
        _ => exec_err!("Spark `decode` function: Unsupported charset {char_set}. Charset must be one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16'"),
    }
}

fn process_decode_arrays<'b, 'c, B, C, T>(
    binary_array: &'b B,
    char_set_array: &'c C,
) -> Result<ArrayRef>
where
    &'b B: BinaryArrayType<'b>,
    &'c C: StringArrayType<'c>,
    T: OffsetSizeTrait,
{
    let mut builder: GenericStringBuilder<T> = GenericStringBuilder::new();
    for (bytes, char_set) in binary_array.iter().zip(char_set_array.iter()) {
        if let (Some(bytes), Some(char_set)) = (bytes, char_set) {
            builder.append_value(decode(bytes, char_set)?);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
