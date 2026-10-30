use std::borrow::Cow;

use datafusion_common::{DataFusionError, Result, plan_datafusion_err};
use encoding_rs::Encoding;
use serde_json::{Map, Value};

use crate::options::r#gen::JsonReadOptions;

/// The default name of the column for corrupt records, as in Spark
/// (`spark.sql.columnNameOfCorruptRecord`).
const DEFAULT_COLUMN_NAME_OF_CORRUPT_RECORD: &str = "_corrupt_record";

/// How corrupt records are handled, as in Spark's `ParseMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParseMode {
    Permissive,
    DropMalformed,
    FailFast,
}

/// A record in a JSON file.
#[derive(Debug, Clone)]
pub(super) enum JsonRecord {
    Valid(Map<String, Value>),
    /// The text of a record that is not a JSON object.
    Corrupt(String),
}

/// How the records of a JSON file are found, following Spark's JSON reader options.
#[derive(Debug, Clone)]
pub(super) struct JsonRecordOptions {
    line_sep: Option<String>,
    multi_line: bool,
    encoding: Option<&'static Encoding>,
    pub mode: ParseMode,
    pub column_name_of_corrupt_record: String,
}

impl JsonRecordOptions {
    pub fn try_new(options: &JsonReadOptions) -> Result<Self> {
        let encoding = options
            .encoding
            .as_deref()
            .map(|label| {
                Encoding::for_label(label.as_bytes())
                    .ok_or_else(|| plan_datafusion_err!("unsupported JSON encoding: {label}"))
            })
            .transpose()?;
        let mode = match options.mode.to_ascii_uppercase().as_str() {
            "PERMISSIVE" => ParseMode::Permissive,
            "DROPMALFORMED" => ParseMode::DropMalformed,
            "FAILFAST" => ParseMode::FailFast,
            other => return Err(plan_datafusion_err!("invalid JSON parse mode: {other}")),
        };
        if options.line_sep.as_deref() == Some("") {
            return Err(plan_datafusion_err!("'lineSep' cannot be an empty string"));
        }
        let column_name_of_corrupt_record = if options.column_name_of_corrupt_record.is_empty() {
            DEFAULT_COLUMN_NAME_OF_CORRUPT_RECORD.to_string()
        } else {
            options.column_name_of_corrupt_record.clone()
        };
        Ok(Self {
            line_sep: options.line_sep.clone(),
            multi_line: options.multi_line,
            encoding,
            mode,
            column_name_of_corrupt_record,
        })
    }

    /// Whether the records must be found by this module, rather than by the
    /// newline-delimited reader of DataFusion.
    pub fn is_required(options: &JsonReadOptions) -> bool {
        options.line_sep.is_some()
            || options.multi_line
            || options.encoding.is_some()
            || options.primitives_as_string
            || !options.mode.eq_ignore_ascii_case("PERMISSIVE")
    }

    /// Returns the records in the content of a JSON file.
    pub fn read_records(&self, content: &[u8]) -> Result<Vec<JsonRecord>> {
        let text = self.decode(content);
        if self.multi_line {
            return Ok(multi_line_records(&text));
        }
        let records = match &self.line_sep {
            Some(line_sep) => text
                .split(line_sep.as_str())
                .map(single_line_record)
                .collect(),
            // Without a line separator, Spark splits records on `\r`, `\r\n` and `\n`.
            None => text
                .split(['\n', '\r'])
                .map(single_line_record)
                .collect::<Vec<_>>(),
        };
        Ok(records.into_iter().flatten().collect())
    }

    fn decode<'a>(&self, content: &'a [u8]) -> Cow<'a, str> {
        match self.encoding {
            Some(encoding) => encoding.decode_with_bom_removal(content).0,
            None => {
                let content = content.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(content);
                String::from_utf8_lossy(content)
            }
        }
    }

    /// Returns the error for a corrupt record in the `FAILFAST` mode.
    pub fn malformed_record_error(&self, record: &str) -> DataFusionError {
        DataFusionError::Execution(format!(
            "Malformed records are detected in record parsing. Parse Mode: FAILFAST. \
             To process malformed records as null result, try setting the option 'mode' as \
             'PERMISSIVE'. Malformed record: {record}"
        ))
    }
}

/// Parses a record that is a line of the file. Blank lines are skipped. Like Spark's
/// Jackson parser, only the first JSON value of the line is read, and the rest is ignored.
fn single_line_record(line: &str) -> Option<JsonRecord> {
    if line.trim().is_empty() {
        return None;
    }
    let value = serde_json::Deserializer::from_str(line)
        .into_iter::<Value>()
        .next();
    Some(match value {
        Some(Ok(Value::Object(object))) => JsonRecord::Valid(object),
        _ => JsonRecord::Corrupt(line.to_string()),
    })
}

/// Parses the records of a file that is a single JSON value, which may span multiple lines.
/// An array has one record per element.
fn multi_line_records(text: &str) -> Vec<JsonRecord> {
    if text.trim().is_empty() {
        return vec![];
    }
    let value = serde_json::Deserializer::from_str(text)
        .into_iter::<Value>()
        .next();
    match value {
        Some(Ok(Value::Object(object))) => vec![JsonRecord::Valid(object)],
        Some(Ok(Value::Array(values))) => values
            .into_iter()
            .map(|value| match value {
                Value::Object(object) => JsonRecord::Valid(object),
                other => JsonRecord::Corrupt(other.to_string()),
            })
            .collect(),
        _ => vec![JsonRecord::Corrupt(text.to_string())],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(line_sep: Option<&str>, multi_line: bool) -> JsonRecordOptions {
        JsonRecordOptions {
            line_sep: line_sep.map(str::to_string),
            multi_line,
            encoding: None,
            mode: ParseMode::Permissive,
            column_name_of_corrupt_record: DEFAULT_COLUMN_NAME_OF_CORRUPT_RECORD.to_string(),
        }
    }

    fn describe(records: Vec<JsonRecord>) -> Vec<String> {
        records
            .into_iter()
            .map(|record| match record {
                JsonRecord::Valid(object) => Value::Object(object).to_string(),
                JsonRecord::Corrupt(text) => format!("corrupt: {text}"),
            })
            .collect()
    }

    #[test]
    fn records_are_split_on_the_line_separator() -> Result<()> {
        let content = b"{\"name\":\"Michael\"}\n{\"name\":\"Andy\", \"age\":30}\n";
        let records = options(Some(","), false).read_records(content)?;
        assert_eq!(
            describe(records),
            vec![
                "{\"name\":\"Michael\"}".to_string(),
                "corrupt:  \"age\":30}\n".to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn records_are_split_on_any_newline_by_default() -> Result<()> {
        let records = options(None, false).read_records(b"{\"a\":1}\r\n\r\n{\"a\":2}\r[1]")?;
        assert_eq!(
            describe(records),
            vec!["{\"a\":1}", "{\"a\":2}", "corrupt: [1]"]
        );
        Ok(())
    }

    #[test]
    fn a_multi_line_array_has_one_record_per_element() -> Result<()> {
        let records = options(None, true).read_records(b"[\n {\"a\": 1},\n {\"a\": 2}\n]\n")?;
        assert_eq!(describe(records), vec!["{\"a\":1}", "{\"a\":2}"]);
        Ok(())
    }

    #[test]
    fn utf16_content_is_decoded() -> Result<()> {
        let mut options = options(None, true);
        options.encoding = Encoding::for_label(b"UTF-16LE");
        let content = "{\"a\": \"é\"}"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            describe(options.read_records(&content)?),
            vec!["{\"a\":\"é\"}"]
        );
        Ok(())
    }
}
