use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};

use crate::display::{ArrayFormatter, FormatOptions};

/// The partition value used by Spark and Hive for both NULL and empty strings.
pub const DEFAULT_PARTITION_NAME: &str = "__HIVE_DEFAULT_PARTITION__";

const HEX: &[u8; 16] = b"0123456789ABCDEF";

fn needs_escaping(character: char) -> bool {
    matches!(
        character,
        '\u{1}'
            ..='\u{1f}'
                | '"'
                | '#'
                | '%'
                | '\''
                | '*'
                | '/'
                | ':'
                | '='
                | '?'
                | '\\'
                | '\u{7f}'
                | '{'
                | '['
                | ']'
                | '^'
    ) || (cfg!(windows) && matches!(character, ' ' | '<' | '>' | '|'))
}

/// Escapes one Hive partition path component using Spark's `%HH` convention.
pub fn escape_path_name(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if needs_escaping(character) {
            let byte = character as u8;
            escaped.push('%');
            escaped.push(HEX[(byte >> 4) as usize] as char);
            escaped.push(HEX[(byte & 0x0f) as usize] as char);
        } else {
            escaped.push(character);
        }
    }
    escaped
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Reverses Hive `%HH` path escaping after a path segment has been isolated.
pub fn unescape_path_name(value: &str) -> Result<String> {
    let mut decoded = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '%' {
            decoded.push(character);
            continue;
        }

        let mut lookahead = characters.clone();
        let escaped = lookahead
            .next()
            .zip(lookahead.next())
            .and_then(|(high, low)| {
                let high = u8::try_from(high).ok().and_then(hex_value)?;
                let low = u8::try_from(low).ok().and_then(hex_value)?;
                Some(char::from((high << 4) | low))
            });
        match escaped {
            Some(character) => {
                decoded.push(character);
                characters.next();
                characters.next();
            }
            None => decoded.push('%'),
        }
    }
    Ok(decoded)
}

/// Formats an Arrow array using Spark SQL string semantics for Hive partition paths.
pub fn format_partition_values(array: &dyn Array) -> Result<Vec<String>> {
    let options = FormatOptions::new().with_null(DEFAULT_PARTITION_NAME);
    let formatter = ArrayFormatter::try_new(array, &options)?;
    (0..array.len())
        .map(|index| {
            let value = formatter.value(index).try_to_string()?;
            if value.is_empty() {
                Ok(DEFAULT_PARTITION_NAME.to_string())
            } else {
                Ok(value)
            }
        })
        .collect()
}

/// Formats a scalar for partition-prefix pruning using the same rules as file writes.
pub fn format_partition_scalar(value: &ScalarValue) -> Result<String> {
    let array = value.to_array()?;
    format_partition_values(array.as_ref())?
        .into_iter()
        .next()
        .ok_or_else(|| exec_datafusion_err!("partition scalar produced no values"))
}

/// Creates an escaped `column=value` path segment while preserving the delimiter.
pub fn partition_path_segment(column: &str, value: &str) -> String {
    format!("{}={}", escape_path_name(column), escape_path_name(value))
}

/// Decodes and casts one Hive partition value to the table partition type.
pub fn parse_partition_value(encoded: &str, data_type: &DataType) -> Result<ScalarValue> {
    if encoded.is_empty() {
        return Err(exec_datafusion_err!(
            "found an empty Hive partition column value"
        ));
    }
    if encoded == DEFAULT_PARTITION_NAME {
        return ScalarValue::try_new_null(data_type);
    }
    ScalarValue::try_from_string(unescape_path_name(encoded)?, data_type)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{
        Decimal128Array, Float64Array, StringArray, TimestampMicrosecondArray,
    };

    use super::*;

    #[test]
    fn escapes_spark_hive_path_characters() {
        assert_eq!(
            escape_path_name("a/b=c:d%#'*?\\\n"),
            "a%2Fb%3Dc%3Ad%25%23%27%2A%3F%5C%0A"
        );
        assert_eq!(escape_path_name("with space/雪"), "with space%2F雪");
        assert_eq!(
            unescape_path_name("a%2Fb%3Dc%3Ad%25%23%27%2A%3F%5C%0A").unwrap(),
            "a/b=c:d%#'*?\\\n"
        );
        assert_eq!(unescape_path_name("a%2").unwrap(), "a%2");
        assert_eq!(unescape_path_name("a%F ").unwrap(), "a%F ");
        assert_eq!(unescape_path_name("%FF").unwrap(), "ÿ");
    }

    #[test]
    fn formats_null_empty_float_and_decimal_values() {
        let strings = StringArray::from(vec![Some(""), None, Some("a/b")]);
        assert_eq!(
            format_partition_values(&strings).unwrap(),
            vec![DEFAULT_PARTITION_NAME, DEFAULT_PARTITION_NAME, "a/b"]
        );

        let floats = Float64Array::from(vec![3.0, 1.5]);
        assert_eq!(
            format_partition_values(&floats).unwrap(),
            vec!["3.0", "1.5"]
        );

        let decimals = Decimal128Array::from(vec![12340, -500])
            .with_precision_and_scale(8, 3)
            .unwrap();
        assert_eq!(
            format_partition_values(&decimals).unwrap(),
            vec!["12.340", "-0.500"]
        );

        let timestamps =
            TimestampMicrosecondArray::from(vec![1_704_164_645_123_456]).with_timezone("UTC");
        assert_eq!(
            format_partition_values(&timestamps).unwrap(),
            vec!["2024-01-02 03:04:05.123456"]
        );
    }

    #[test]
    fn parses_escaped_and_default_partition_values() {
        assert_eq!(
            parse_partition_value("a%2Fb", &DataType::Utf8).unwrap(),
            ScalarValue::Utf8(Some("a/b".to_string()))
        );
        assert_eq!(
            parse_partition_value(DEFAULT_PARTITION_NAME, &DataType::Int32).unwrap(),
            ScalarValue::Int32(None)
        );
        assert!(parse_partition_value("", &DataType::Utf8).is_err());
    }
}
