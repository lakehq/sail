use std::fmt::{self, Debug};
use std::io::{BufRead, BufReader as StdBufReader, Read};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, RecordBatch, RecordBatchOptions, RecordBatchReader, StringArray,
};
use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;

// TODO: CSV has `arrow_csv::reader::records::AVERAGE_FIELD_SIZE` set to 8.
//  Not sure if this is a reasonable value for text files or not.
const AVERAGE_LINE_SIZE: usize = 128;

const MIN_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Default)]
pub struct Format {
    whole_text: bool,
    // defaults to CRLF
    line_sep: Option<u8>,
}

impl Format {
    pub fn with_whole_text(mut self, whole_text: bool) -> Self {
        self.whole_text = whole_text;
        self
    }

    pub fn with_line_sep(mut self, line_sep: u8) -> Self {
        self.line_sep = Some(line_sep);
        self
    }
}

type Bounds = Option<(usize, usize)>;

pub type Reader<R> = BufReader<StdBufReader<R>>;

pub struct BufReader<R> {
    reader: R,
    decoder: Decoder,
}

impl<R> Debug for BufReader<R>
where
    R: BufRead,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("decoder", &self.decoder)
            .finish()
    }
}

impl<R: Read> Reader<R> {
    #[allow(unused)]
    pub fn schema(&self) -> SchemaRef {
        match &self.decoder.projection {
            Some(projection) => {
                let fields = self.decoder.schema.fields();
                let projected = projection.iter().map(|i| fields[*i].clone());
                Arc::new(Schema::new(projected.collect::<Fields>()))
            }
            None => self.decoder.schema.clone(),
        }
    }
}

impl<R: BufRead> BufReader<R> {
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            let buf = self.reader.fill_buf()?;
            let decoded = self.decoder.decode(buf)?;
            self.reader.consume(decoded);
            if decoded == 0 || self.decoder.capacity() == 0 {
                break;
            }
        }
        self.decoder.flush()
    }
}

impl<R: BufRead> Iterator for BufReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for BufReader<R> {
    fn schema(&self) -> SchemaRef {
        self.decoder.schema.clone()
    }
}

#[derive(Debug)]
pub struct Decoder {
    /// Schema for text files (single "value" column)
    schema: SchemaRef,

    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,

    /// Number of records per batch
    batch_size: usize,

    /// Rows to skip
    to_skip: usize,

    /// Current line number
    line_number: usize,

    /// End line number
    end: usize,

    /// A decoder for [`StringRecords`]
    record_decoder: RecordDecoder,
}

// [Credit]: https://github.com/apache/arrow-rs/blob/ebb6ede98b2b4d96a1a4f501a28ab42a3b937f73/arrow-csv/src/reader/mod.rs#L581-L634
impl Decoder {
    /// Decode records from `buf` returning the number of bytes read
    ///
    /// This method returns once `batch_size` objects have been parsed since the
    /// last call to [`Self::flush`], or `buf` is exhausted. Any remaining bytes
    /// should be included in the next call to [`Self::decode`]
    ///
    /// There is no requirement that `buf` contains a whole number of records, facilitating
    /// integration with arbitrary byte streams, such as that yielded by [`BufRead`] or
    /// network sources such as object storage
    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        if self.to_skip != 0 {
            // Skip in units of `to_read` to avoid over-allocating buffers
            let to_skip = self.to_skip.min(self.batch_size);
            let (skipped, bytes) = self.record_decoder.decode(buf, to_skip)?;
            self.to_skip -= skipped;
            self.record_decoder.clear();
            return Ok(bytes);
        }

        let to_read = self.batch_size.min(self.end - self.line_number) - self.record_decoder.len();
        let (_, bytes) = self.record_decoder.decode(buf, to_read)?;
        Ok(bytes)
    }

    /// Flushes the currently buffered data to a [`RecordBatch`]
    ///
    /// This should only be called after [`Self::decode`] has returned `Ok(0)`,
    /// otherwise may return an error if part way through decoding a record
    ///
    /// Returns `Ok(None)` if no buffered data
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.record_decoder.is_empty() && self.record_decoder.partial_line.is_empty() {
            return Ok(None);
        }

        let rows = self.record_decoder.flush()?;
        let batch = parse(&rows, self.schema.clone(), self.projection.as_ref())?;
        self.line_number += rows.len();
        Ok(Some(batch))
    }

    /// Returns the number of records that can be read before requiring a call to [`Self::flush`]
    pub fn capacity(&self) -> usize {
        self.batch_size - self.record_decoder.len()
    }
}

fn parse(
    rows: &StringRecords<'_>,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<RecordBatch, ArrowError> {
    if schema.fields().len() != 1 {
        return Err(ArrowError::ParseError(format!(
            "Text data source supports only a single column, and you have {} columns.",
            schema.fields().len()
        )));
    }

    let projection: Vec<usize> = match projection {
        Some(v) => v.clone(),
        None => vec![0], // Default to including the single column
    };
    let projected_fields: Fields = projection
        .iter()
        .map(|i| schema.fields()[*i].clone())
        .collect();
    let projected_schema = if schema.metadata.is_empty() {
        Arc::new(Schema::new(projected_fields))
    } else {
        Arc::new(Schema::new_with_metadata(
            projected_fields,
            schema.metadata.clone(),
        ))
    };

    let arrays: Vec<Arc<dyn Array>> = if projection == vec![0] {
        let array = StringArray::from_iter_values(rows.iter());
        vec![Arc::new(array)]
    } else if projection.is_empty() {
        vec![]
    } else {
        return Err(ArrowError::ParseError(format!(
            "Invalid projection {projection:?} for Text file with single column"
        )));
    };
    RecordBatch::try_new_with_options(
        projected_schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
}

#[derive(Debug)]
pub struct ReaderBuilder {
    /// Schema of the Text file
    schema: SchemaRef,
    /// Format of the Text file
    format: Format,
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// The bounds within which to scan the reader.
    /// If set to `None`, scanning starts at position 0 and continues until EOF.
    bounds: Bounds,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
}

impl ReaderBuilder {
    pub fn new(schema: SchemaRef) -> ReaderBuilder {
        Self {
            schema,
            format: Format::default(),
            batch_size: 1024,
            bounds: None,
            projection: None,
        }
    }

    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[allow(unused)]
    pub fn with_bounds(mut self, start: usize, end: usize) -> Self {
        self.bounds = Some((start, end));
        self
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn build<R: Read>(self, reader: R) -> Result<Reader<R>, ArrowError> {
        self.build_buffered(StdBufReader::new(reader))
    }

    pub fn build_buffered<R: BufRead>(self, reader: R) -> Result<BufReader<R>, ArrowError> {
        Ok(BufReader {
            reader,
            decoder: self.build_decoder(),
        })
    }

    pub fn build_decoder(self) -> Decoder {
        let record_decoder = RecordDecoder::new(self.format.line_sep, self.format.whole_text);
        let (start, end) = match self.bounds {
            Some((start, end)) => (start, end),
            None => (0, usize::MAX),
        };
        Decoder {
            schema: self.schema,
            projection: self.projection,
            batch_size: self.batch_size,
            to_skip: start,
            line_number: start,
            end,
            record_decoder,
        }
    }
}

#[derive(Debug)]
pub struct RecordDecoder {
    /// Line separator (None means use CRLF)
    line_sep: Option<u8>,

    /// Whether we're in whole text mode (read entire file as one record)
    whole_text: bool,

    /// The current line number
    line_number: usize,

    /// Offsets delimiting field start positions
    offsets: Vec<usize>,

    /// The current offset is tracked independently of Vec to avoid re-zeroing memory
    offsets_len: usize,

    /// The number of rows buffered
    num_rows: usize,

    /// Decoded line data
    data: Vec<u8>,

    /// Offsets into data are tracked independently of Vec to avoid re-zeroing memory
    data_len: usize,

    /// Partial line buffer for incomplete lines at buffer boundaries
    partial_line: Vec<u8>,
}

impl RecordDecoder {
    pub fn new(line_sep: Option<u8>, whole_text: bool) -> Self {
        Self {
            line_sep,
            whole_text,
            line_number: 1,
            offsets: vec![0], // First offset is always 0
            offsets_len: 1,   // The first offset is always 0
            num_rows: 0,
            data: vec![],
            data_len: 0,
            partial_line: vec![],
        }
    }

    /// Decodes records from `input` returning the number of records and bytes read.
    ///
    /// Note: this expects to be called with an empty `input` to signal EOF.
    pub fn decode(&mut self, input: &[u8], to_read: usize) -> Result<(usize, usize), ArrowError> {
        if to_read == 0 {
            return Ok((0, 0));
        }

        self.offsets.resize(self.offsets_len + to_read, 0);
        let mut input_offset = 0;
        let mut read = 0;

        if self.whole_text {
            if !input.is_empty() {
                let capacity = self.data_len + input.len();
                self.data.resize(capacity, 0);
                self.data[self.data_len..self.data_len + input.len()].copy_from_slice(input);
                self.data_len += input.len();
            }

            if self.num_rows == 0 && self.data_len > 0 {
                self.num_rows = 1;
                self.offsets[1] = self.data_len;
                self.offsets_len = 2;
                read = 1;
            } else if self.num_rows == 1 {
                self.offsets[1] = self.data_len;
            }

            return Ok((read.min(to_read), input.len()));
        }

        let estimated_size = to_read * AVERAGE_LINE_SIZE;
        let capacity = (self.data_len + estimated_size).max(MIN_CAPACITY);
        self.data.resize(capacity, 0);

        let is_line_end = |bytes: &[u8], pos: usize| -> (bool, usize) {
            if let Some(sep) = self.line_sep {
                if bytes[pos] == sep {
                    (true, 1)
                } else {
                    (false, 0)
                }
            } else if bytes[pos] == b'\r' {
                if pos + 1 < bytes.len() && bytes[pos + 1] == b'\n' {
                    (true, 2)
                } else {
                    (true, 1)
                }
            } else if bytes[pos] == b'\n' {
                (true, 1)
            } else {
                (false, 0)
            }
        };

        if !self.partial_line.is_empty() && input_offset < input.len() {
            let mut found_end = false;
            let mut end_pos = 0;
            let mut skip_bytes = 0;

            for i in 0..input.len() {
                let (is_end, skip) = is_line_end(input, i);
                if is_end {
                    found_end = true;
                    end_pos = i;
                    skip_bytes = skip;
                    break;
                }
            }

            if found_end {
                self.partial_line.extend_from_slice(&input[..end_pos]);

                if self.data_len + self.partial_line.len() > self.data.len() {
                    self.data
                        .resize(self.data_len + self.partial_line.len() + MIN_CAPACITY, 0);
                }

                self.data[self.data_len..self.data_len + self.partial_line.len()]
                    .copy_from_slice(&self.partial_line);
                self.data_len += self.partial_line.len();

                self.offsets[self.offsets_len] = self.data_len;
                self.offsets_len += 1;

                self.partial_line.clear();
                read += 1;
                self.num_rows += 1;
                self.line_number += 1;
                input_offset = end_pos + skip_bytes;

                if read >= to_read {
                    return Ok((read, input_offset));
                }
            } else {
                self.partial_line.extend_from_slice(input);
                return Ok((read, input.len()));
            }
        }

        let mut line_start = input_offset;
        let mut i = input_offset;

        while i < input.len() {
            let (is_end, skip_bytes) = is_line_end(input, i);

            if is_end {
                let line_len = i - line_start;

                if self.data_len + line_len > self.data.len() {
                    let new_capacity =
                        (self.data_len + line_len + estimated_size).max(self.data.len() * 2);
                    self.data.resize(new_capacity, 0);
                }

                if line_len > 0 {
                    self.data[self.data_len..self.data_len + line_len]
                        .copy_from_slice(&input[line_start..i]);
                    self.data_len += line_len;
                }

                if self.offsets_len >= self.offsets.len() {
                    self.offsets.resize(self.offsets_len + to_read, 0);
                }

                self.offsets[self.offsets_len] = self.data_len;
                self.offsets_len += 1;

                read += 1;
                self.num_rows += 1;
                self.line_number += 1;

                i += skip_bytes;
                line_start = i;
                input_offset = i;

                if read >= to_read {
                    return Ok((read, input_offset));
                }

                if input.len() == input_offset {
                    return Ok((read, input_offset));
                }
            } else {
                i += 1;
            }
        }

        if line_start < input.len() {
            self.partial_line.extend_from_slice(&input[line_start..]);
            input_offset = input.len();
        }

        Ok((read, input_offset))
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn clear(&mut self) {
        self.offsets_len = 1;
        self.data_len = 0;
        self.num_rows = 0;
    }

    pub fn flush(&mut self) -> Result<StringRecords<'_>, ArrowError> {
        if !self.partial_line.is_empty() {
            if self.data_len + self.partial_line.len() > self.data.len() {
                self.data.resize(self.data_len + self.partial_line.len(), 0);
            }

            self.data[self.data_len..self.data_len + self.partial_line.len()]
                .copy_from_slice(&self.partial_line);
            self.data_len += self.partial_line.len();

            if self.offsets_len >= self.offsets.len() {
                self.offsets.push(self.data_len);
            } else {
                self.offsets[self.offsets_len] = self.data_len;
            }
            self.offsets_len += 1;

            self.num_rows += 1;
            self.line_number += 1;
            self.partial_line.clear();
        }

        let data = std::str::from_utf8(&self.data[..self.data_len]).map_err(|e| {
            let valid_up_to = e.valid_up_to();
            let line_idx = self.offsets[..self.offsets_len]
                .iter()
                .rposition(|offset| *offset <= valid_up_to)
                .unwrap_or(0);
            let line_offset = self.line_number - self.num_rows;
            let line = line_offset + line_idx;
            ArrowError::ParseError(format!("Encountered invalid UTF-8 data at line {line}"))
        })?;

        let offsets = &self.offsets[..self.offsets_len];
        let num_rows = self.num_rows;

        self.offsets_len = 1;
        self.data_len = 0;
        self.num_rows = 0;

        Ok(StringRecords {
            num_rows,
            offsets,
            data,
        })
    }
}

#[derive(Debug)]
pub struct StringRecords<'a> {
    num_rows: usize,
    offsets: &'a [usize],
    data: &'a str,
}

impl<'a> StringRecords<'a> {
    fn get(&self, index: usize) -> &'a str {
        let start = self.offsets[index];
        let end = self.offsets[index + 1];
        // SAFETY: Parsing produces offsets at valid UTF-8 boundaries
        unsafe { self.data.get_unchecked(start..end) }
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a str> + '_ {
        (0..self.num_rows).map(|x| self.get(x))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_text() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let mut text = ReaderBuilder::new(schema)
            .build(Cursor::new(b"line1\nline2\nline3\nline4\nline5"))
            .unwrap();
        let batch = text.next().unwrap().unwrap();
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("line1", value.value(0));
        assert_eq!("line2", value.value(1));
        assert_eq!("line3", value.value(2));
        assert_eq!("line4", value.value(3));
        assert_eq!("line5", value.value(4));
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_text_schema_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("foo".to_owned(), "bar".to_owned());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new(
                "value",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            )],
            metadata.clone(),
        ));
        let mut text = ReaderBuilder::new(schema.clone())
            .build(Cursor::new(b"line1\nline2\nline3\nline4\nline5"))
            .unwrap();
        assert_eq!(schema, text.schema());
        let batch = text.next().unwrap().unwrap();
        assert_eq!(5, batch.num_rows());
        assert_eq!(1, batch.num_columns());
        assert_eq!(&metadata, batch.schema().metadata());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_text_builder_with_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let mut text = ReaderBuilder::new(schema)
            .with_bounds(0, 2)
            .build(Cursor::new(b"line1\nline2\nline3\nline4\nline5"))
            .unwrap();
        let batch = text.next().unwrap().unwrap();
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("line1", value.value(0));
        let result = std::panic::catch_unwind(|| value.value(3));
        assert!(
            result.is_err(),
            "Accessing value outside of bounds should panic"
        );
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_whole_text() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let mut text = ReaderBuilder::new(schema)
            .with_format(Format::default().with_whole_text(true))
            .build(Cursor::new(b"line1\nline2\nline3\nline4\nline5"))
            .unwrap();
        let batch = text.next().unwrap().unwrap();
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("line1\nline2\nline3\nline4\nline5", value.value(0));
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_line_sep() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let mut text = ReaderBuilder::new(schema)
            .with_format(Format::default().with_line_sep(b';'))
            .build(Cursor::new(b"line1;line2;line3;line4;line5"))
            .unwrap();
        let batch = text.next().unwrap().unwrap();
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("line1", value.value(0));
        assert_eq!("line2", value.value(1));
        assert_eq!("line3", value.value(2));
        assert_eq!("line4", value.value(3));
        assert_eq!("line5", value.value(4));
        assert_eq!(5, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}
