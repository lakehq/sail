use std::io::Read;

use bytes::Bytes;
use csv_core::{ReadRecordResult, Reader, ReaderBuilder, Terminator};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::config::CsvOptions;
use datafusion_common::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt};

struct CsvHeaderValidator {
    reader: Reader,
    output: Vec<u8>,
    output_len: usize,
    ends: Vec<usize>,
    ends_len: usize,
    expected: Vec<String>,
    path: String,
    complete: bool,
}

impl CsvHeaderValidator {
    /// Creates a header validator configured with the same dialect as the CSV reader.
    fn new(schema: &SchemaRef, options: &CsvOptions, path: &str) -> Self {
        let mut builder = ReaderBuilder::new();
        builder.escape(options.escape);
        builder.comment(options.comment);
        builder.delimiter(options.delimiter);
        builder.quote(options.quote);
        if let Some(terminator) = options.terminator {
            builder.terminator(Terminator::Any(terminator));
        }

        Self {
            reader: builder.build(),
            output: vec![0; 1024],
            output_len: 0,
            ends: vec![0; schema.fields().len() + 1],
            ends_len: 0,
            expected: schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            path: path.to_string(),
            complete: false,
        }
    }

    /// Feeds CSV bytes until the first non-comment record is available for validation.
    fn feed(&mut self, input: &[u8]) -> Result<bool> {
        if self.complete {
            return Ok(true);
        }

        let mut consumed = 0;
        loop {
            let (result, read, written, ends) = self.reader.read_record(
                &input[consumed..],
                &mut self.output[self.output_len..],
                &mut self.ends[self.ends_len..],
            );
            consumed += read;
            self.output_len += written;
            self.ends_len += ends;

            match result {
                ReadRecordResult::InputEmpty => return Ok(false),
                ReadRecordResult::OutputFull => {
                    self.output.resize(self.output.len() * 2, 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    self.ends.resize(self.ends.len() * 2, 0);
                }
                ReadRecordResult::Record => {
                    self.validate()?;
                    self.complete = true;
                    return Ok(true);
                }
                ReadRecordResult::End => return Ok(false),
            }
        }
    }

    /// Finishes parsing and validates a final header without a record terminator.
    fn finish(&mut self) -> Result<()> {
        while !self.complete {
            if !self.feed(&[])? {
                break;
            }
        }
        Ok(())
    }

    /// Compares parsed header names with schema fields by position.
    fn validate(&self) -> Result<()> {
        let mut start = 0;
        let actual = self.ends[..self.ends_len]
            .iter()
            .map(|end| {
                let field = String::from_utf8_lossy(&self.output[start..*end]).into_owned();
                start = *end;
                field
            })
            .collect::<Vec<_>>();

        if actual == self.expected {
            return Ok(());
        }

        Err(DataFusionError::Execution(format!(
            "CSV header does not conform to the schema.\nHeader: [{}]\nSchema: [{}]\nExpected: {} but found: {}\nCSV file: {}",
            actual.join(", "),
            self.expected.join(", "),
            self.expected
                .iter()
                .zip(actual.iter())
                .find_map(|(expected, actual)| (expected != actual).then_some(expected.as_str()))
                .unwrap_or_else(|| self
                    .expected
                    .get(actual.len())
                    .map_or("<missing>", String::as_str)),
            self.expected
                .iter()
                .zip(actual.iter())
                .find_map(|(expected, actual)| (expected != actual).then_some(actual.as_str()))
                .unwrap_or_else(|| actual
                    .get(self.expected.len())
                    .map_or("<missing>", String::as_str)),
            self.path,
        )))
    }
}

/// Validates a reader's CSV header and returns every byte consumed while doing so.
pub(super) fn validate_reader_header<R: Read>(
    reader: &mut R,
    schema: &SchemaRef,
    options: &CsvOptions,
    path: &str,
) -> Result<Vec<u8>> {
    let mut validator = CsvHeaderValidator::new(schema, options, path);
    let mut prefix = Vec::new();
    let mut buffer = [0; 8 * 1024];

    while !validator.complete {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            validator.finish()?;
            break;
        }
        prefix.extend_from_slice(&buffer[..read]);
        validator.feed(&buffer[..read])?;
    }
    Ok(prefix)
}

/// Validates a stream's CSV header and replays the inspected byte chunks.
pub(super) async fn validate_stream_header(
    mut input: futures::stream::BoxStream<'static, Result<Bytes>>,
    schema: &SchemaRef,
    options: &CsvOptions,
    path: &str,
) -> Result<futures::stream::BoxStream<'static, Result<Bytes>>> {
    let mut validator = CsvHeaderValidator::new(schema, options, path);
    let mut prefix = Vec::new();

    while !validator.complete {
        match input.try_next().await? {
            Some(bytes) => {
                validator.feed(&bytes)?;
                prefix.push(bytes);
            }
            None => {
                validator.finish()?;
                break;
            }
        }
    }

    Ok(futures::stream::iter(prefix.into_iter().map(Ok))
        .chain(input)
        .boxed())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    /// Creates a CSV schema and options for header validation tests.
    fn validation_config(names: &[&str]) -> (SchemaRef, CsvOptions) {
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let options = CsvOptions {
            has_header: Some(true),
            ..Default::default()
        };
        (schema, options)
    }

    /// Verifies that bytes inspected through a reader can be replayed unchanged.
    #[test]
    fn matching_reader_header_is_replayed() -> Result<()> {
        let (schema, options) = validation_config(&["f1", "f2"]);
        let expected = b"f1,f2\n1,2\n";
        let mut input = Cursor::new(expected.to_vec());
        let prefix = validate_reader_header(&mut input, &schema, &options, "matching.csv")?;
        let mut replayed = Vec::new();
        Cursor::new(prefix)
            .chain(input)
            .read_to_end(&mut replayed)?;

        assert_eq!(replayed, expected);
        Ok(())
    }

    /// Verifies that header fields are checked against schema fields by position.
    #[test]
    fn mismatched_reader_header_returns_error() -> Result<()> {
        let (schema, options) = validation_config(&["f2", "f1"]);
        let mut input = Cursor::new(b"f1,f2\n1,2\n".to_vec());
        let error = validate_reader_header(&mut input, &schema, &options, "mismatched.csv")
            .err()
            .ok_or_else(|| {
                DataFusionError::Execution("reversed header was accepted".to_string())
            })?;

        assert!(
            error
                .to_string()
                .contains("CSV header does not conform to the schema")
        );
        assert!(error.to_string().contains("Expected: f2 but found: f1"));
        assert!(error.to_string().contains("mismatched.csv"));
        Ok(())
    }

    /// Verifies that streaming validation handles quoted fields split across chunks.
    #[tokio::test]
    async fn matching_stream_header_is_replayed() -> Result<()> {
        let (schema, options) = validation_config(&["first,name", "f2"]);
        let chunks = vec![
            Ok(Bytes::from_static(b"\"first")),
            Ok(Bytes::from_static(b",name\",f2\n1")),
            Ok(Bytes::from_static(b",2\n")),
        ];
        let input = futures::stream::iter(chunks).boxed();
        let output = validate_stream_header(input, &schema, &options, "stream.csv")
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(output.concat(), b"\"first,name\",f2\n1,2\n".as_slice());
        Ok(())
    }
}
