//! Projected CSV decoding for all-string file schemas.
//!
//! arrow-csv's `RecordDecoder` copies every field of every column through `csv_core`
//! and stores one end offset per field before the projection is applied
//! (arrow-csv `reader/records.rs`, `reader/mod.rs::parse`). For wide files read with
//! `inferSchema=false` (every column is `Utf8`), this decoder finds the end of records
//! that do not contain the quote byte with `memchr`, locates their delimiters with a
//! SWAR byte-equality scan (one 64-bit mask per 64-byte block) walking only the
//! delimiters up to the largest projected column, and delegates records that contain
//! the quote byte to `csv_core` (the parser arrow-csv uses) from the start of the
//! record. Field-count validation, header skipping, truncated-row padding and null
//! handling (empty field) follow arrow-csv.
//!
//! Differences from arrow-csv (both are `csv_core` DFA quirks; the Spark behaviour of
//! ignoring comment lines is kept, see `CSVExprUtils.filterCommentAndEmpty`):
//! - a comment line ends at the record terminator, also when `lineSep` is not `\n`
//!   (csv_core never leaves a comment when the terminator is another byte);
//! - a comment line without a trailing newline at the end of the file does not produce
//!   an empty record (csv_core `transition_final_dfa`, reader.rs:748-758).
//!
//! The input must be valid UTF-8; both Sail read paths pass the bytes through the lossy
//! UTF-8 decoder first. The projected values are validated again when the string arrays
//! are built (`StringArray::try_from_binary`), which only costs a pass over the
//! projected bytes.

use std::io::BufRead;
use std::sync::Arc;

use csv_core::{ReadRecordResult, Reader, ReaderBuilder, Terminator};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, RecordBatch, RecordBatchOptions, StringArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion_datasource::decoder::Decoder;

const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";

#[derive(Clone)]
pub struct ProjectedCsvOptions {
    pub schema: SchemaRef,
    pub projection: Vec<usize>,
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub terminator: Option<u8>,
    pub has_header: bool,
    pub truncated_rows: bool,
    pub batch_size: usize,
}

#[derive(Debug)]
pub struct ProjectedCsvDecoder {
    schema: SchemaRef,
    /// the delimiter splatted into every byte, for `eq_mask`
    delimiter_pattern: u64,
    quote: u8,
    comment: Option<u8>,
    terminator: Option<u8>,
    num_columns: usize,
    truncated_rows: bool,
    batch_size: usize,
    /// `(file column, output column)` sorted by file column
    wanted: Vec<(usize, usize)>,
    builders: Vec<BinaryBuilder>,
    rows: usize,
    to_skip: usize,
    /// arrow `RecordDecoder::line_number`: one plus the number of records decoded,
    /// header included
    line_number: usize,
    has_read: bool,
    in_comment: bool,
    /// bytes of an unterminated quote-free record carried across `decode` calls
    pending: Vec<u8>,
    /// end offsets of the delimiters of the current fast-path record up to the largest
    /// projected column
    ends: Vec<usize>,
    /// `csv_core` parser for records containing the quote byte; `slow` is set while it
    /// owns the current record. It is never cloned: `csv_core::Reader::clone` drops part
    /// of the DFA (csv-core reader.rs:1307-1313).
    slow: bool,
    slow_reader: Reader,
    slow_out: Vec<u8>,
    slow_out_len: usize,
    slow_ends: Vec<usize>,
    slow_ends_len: usize,
}

impl ProjectedCsvDecoder {
    pub fn try_new(options: ProjectedCsvOptions) -> Result<Self, ArrowError> {
        let ProjectedCsvOptions {
            schema,
            projection,
            delimiter,
            quote,
            escape,
            comment,
            terminator,
            has_header,
            truncated_rows,
            batch_size,
        } = options;
        let projected = Arc::new(schema.project(&projection)?);
        let mut wanted: Vec<(usize, usize)> = projection
            .iter()
            .enumerate()
            .map(|(output, file)| (*file, output))
            .collect();
        wanted.sort_unstable();

        // same parser configuration as arrow `Format::build_parser` (reader/mod.rs:426-441)
        let mut builder = ReaderBuilder::new();
        builder.escape(escape);
        builder.comment(comment);
        builder.delimiter(delimiter);
        builder.quote(quote);
        if let Some(t) = terminator {
            builder.terminator(Terminator::Any(t));
        }
        let mut slow_reader = builder.build();
        // Disarm csv_core's one-time BOM strip (reader.rs:607-619): the BOM at the start
        // of the file is handled in `decode`, and the fallback parser may first be used
        // in the middle of the file.
        let _ = slow_reader.read_record(&[], &mut [], &mut []);

        Ok(Self {
            schema: projected,
            delimiter_pattern: u64::from(delimiter) * 0x0101_0101_0101_0101,
            quote,
            comment,
            terminator,
            num_columns: schema.fields().len(),
            truncated_rows,
            batch_size,
            builders: projection
                .iter()
                .map(|_| BinaryBuilder::with_capacity(batch_size, batch_size * 16))
                .collect(),
            ends: Vec::with_capacity(wanted.last().map_or(0, |&(column, _)| column + 1)),
            wanted,
            rows: 0,
            to_skip: usize::from(has_header),
            line_number: 1,
            has_read: false,
            in_comment: false,
            pending: Vec::new(),
            slow: false,
            slow_reader,
            slow_out: vec![0; 4096],
            slow_out_len: 0,
            slow_ends: vec![0; schema.fields().len() + 1],
            slow_ends_len: 0,
        })
    }

    #[inline]
    fn is_terminator(&self, byte: u8) -> bool {
        match self.terminator {
            Some(t) => byte == t,
            None => byte == b'\n' || byte == b'\r',
        }
    }

    #[inline]
    fn find_terminator_or_quote(&self, haystack: &[u8]) -> Option<usize> {
        match self.terminator {
            Some(t) => memchr::memchr2(t, self.quote, haystack),
            None => memchr::memchr3(b'\n', b'\r', self.quote, haystack),
        }
    }

    /// Feeds bytes to the `csv_core` parser and returns the number of bytes consumed.
    /// Leaves the slow path once the record is complete.
    fn slow_feed(&mut self, input: &[u8]) -> Result<usize, ArrowError> {
        let mut consumed = 0;
        loop {
            let (result, read, written, ends) = self.slow_reader.read_record(
                &input[consumed..],
                &mut self.slow_out[self.slow_out_len..],
                &mut self.slow_ends[self.slow_ends_len..],
            );
            consumed += read;
            self.slow_out_len += written;
            self.slow_ends_len += ends;
            match result {
                ReadRecordResult::InputEmpty => return Ok(consumed),
                ReadRecordResult::OutputFull => {
                    let n = self.slow_out.len();
                    self.slow_out.resize(n * 2, 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    let n = self.slow_ends.len();
                    self.slow_ends.resize(n * 2, 0);
                }
                ReadRecordResult::Record => {
                    self.slow = false;
                    let out = std::mem::take(&mut self.slow_out);
                    let ends = std::mem::take(&mut self.slow_ends);
                    let num_fields = self.slow_ends_len;
                    let result = self.emit_record(num_fields, |_, field| {
                        let start = if field == 0 { 0 } else { ends[field - 1] };
                        &out[start..ends[field]]
                    });
                    self.slow_out = out;
                    self.slow_ends = ends;
                    result?;
                    return Ok(consumed);
                }
                ReadRecordResult::End => {
                    self.slow = false;
                    return Ok(consumed);
                }
            }
        }
    }

    /// Fast path: `line` contains neither a terminator nor the quote byte.
    #[inline]
    fn process_line(&mut self, line: &[u8]) -> Result<(), ArrowError> {
        let pattern = self.delimiter_pattern;
        // the delimiters closing the fields up to the largest projected column are needed
        let needed = self.wanted.last().map_or(0, |&(column, _)| column + 1);
        let mut ends = std::mem::take(&mut self.ends);
        ends.clear();
        let mut num_fields = 1;
        // one pass over the line: a 64-bit delimiter mask per 64-byte block counts all
        // delimiters, and its set bits are walked only until `needed` ends are known
        let (blocks, tail) = line.as_chunks::<64>();
        let tail_mask = (!tail.is_empty()).then(|| delimiter_mask(tail, pattern));
        let masks = blocks
            .iter()
            .map(|block| delimiter_mask(block, pattern))
            .chain(tail_mask);
        for (i, mut mask) in masks.enumerate() {
            let count = mask.count_ones() as usize;
            num_fields += count;
            for _ in 0..count.min(needed - ends.len()) {
                ends.push(i * 64 + mask.trailing_zeros() as usize);
                mask &= mask - 1;
            }
        }
        // a missing (truncated) field is not requested: `emit_record` pads it
        let result = self.emit_record(num_fields, |_, column| {
            let start = if column == 0 { 0 } else { ends[column - 1] + 1 };
            &line[start..ends.get(column).copied().unwrap_or(line.len())]
        });
        self.ends = ends;
        result
    }

    /// Validates the field count like arrow `RecordDecoder` (records.rs:136-149) and
    /// appends the projected fields; `field(w, column)` returns the bytes of file
    /// column `column` (the `w`-th projected column in file order).
    #[inline]
    fn emit_record<'a, F>(&mut self, num_fields: usize, field: F) -> Result<(), ArrowError>
    where
        F: Fn(usize, usize) -> &'a [u8],
    {
        if num_fields != self.num_columns && !(self.truncated_rows && num_fields < self.num_columns)
        {
            return Err(ArrowError::CsvError(format!(
                "incorrect number of fields for line {}, expected {} got {}",
                self.line_number, self.num_columns, num_fields
            )));
        }
        self.line_number += 1;
        if self.to_skip != 0 {
            self.to_skip -= 1;
            return Ok(());
        }
        for (w, &(column, output)) in self.wanted.iter().enumerate() {
            // arrow `parse` (reader/mod.rs:823-830): an empty field is null
            let value = if column < num_fields {
                field(w, column)
            } else {
                &[]
            };
            if value.is_empty() {
                self.builders[output].append_null();
            } else {
                self.builders[output].append_value(value);
            }
        }
        self.rows += 1;
        Ok(())
    }
}

const LOW_7_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;

/// Bit `i` of the result is set iff byte `i` (little-endian) of `word` equals the byte
/// splatted in `pattern`. SWAR zero-byte test on `word ^ pattern`: adding `0x7f` to the
/// low 7 bits of a byte sets bit 7 iff they are not all zero (no carry between bytes),
/// so bit 7 of `z` is set exactly where the byte is zero. The multiply by the "magic"
/// constant gathers the flags at bits 0, 8, ..., 56 into the top byte (bit `i` = byte `i`).
#[inline]
fn eq_mask(word: u64, pattern: u64) -> u8 {
    let x = word ^ pattern;
    let y = (x & LOW_7_BITS).wrapping_add(LOW_7_BITS);
    let z = !(y | x | LOW_7_BITS);
    ((z >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) as u8
}

/// Bit `i` of the result is set iff `bytes[i]` is the delimiter splatted in `pattern`;
/// `bytes` holds at most 64 bytes.
#[inline]
fn delimiter_mask(bytes: &[u8], pattern: u64) -> u64 {
    let (words, rest) = bytes.as_chunks::<8>();
    let mut mask = words.iter().enumerate().fold(0, |mask, (i, word)| {
        mask | (u64::from(eq_mask(u64::from_le_bytes(*word), pattern)) << (8 * i))
    });
    if !rest.is_empty() {
        // the last partial word is zero-padded; its pad bytes are masked off below since
        // the delimiter may be any byte, including 0x00
        let word = rest
            .iter()
            .enumerate()
            .fold(0, |word, (i, &b)| word | (u64::from(b) << (8 * i)));
        mask |= u64::from(eq_mask(word, pattern)) << (8 * words.len());
        mask &= (1 << bytes.len()) - 1;
    }
    mask
}

impl Decoder for ProjectedCsvDecoder {
    /// Decodes records from `buf` and returns the number of bytes consumed.
    ///
    /// Same contract as `arrow::csv::reader::Decoder::decode`: an empty `buf` signals
    /// the end of the input, and no more bytes are consumed once `batch_size` records
    /// are buffered.
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let mut pos = 0;
        if !self.has_read {
            // csv_core strips the BOM only on its first read and only when at least
            // three bytes are buffered (reader.rs:607-619)
            if buf.starts_with(UTF8_BOM) {
                pos = UTF8_BOM.len();
            }
            self.has_read = true;
        }
        if buf.is_empty() {
            if self.slow {
                self.slow_feed(&[])?;
            } else if !self.pending.is_empty() {
                let line = std::mem::take(&mut self.pending);
                self.process_line(&line)?;
            }
            self.in_comment = false;
            return Ok(0);
        }
        while pos < buf.len() {
            if self.rows == self.batch_size {
                break;
            }
            if self.slow {
                pos += self.slow_feed(&buf[pos..])?;
                continue;
            }
            if self.in_comment {
                match memchr::memchr(self.terminator.unwrap_or(b'\n'), &buf[pos..]) {
                    Some(i) => {
                        pos += i + 1;
                        self.in_comment = false;
                        continue;
                    }
                    None => return Ok(buf.len()),
                }
            }
            if self.pending.is_empty() {
                // record start (csv_core `StartRecord`): terminator bytes are discarded
                // and the comment byte starts a comment line
                while pos < buf.len() && self.is_terminator(buf[pos]) {
                    pos += 1;
                }
                if pos == buf.len() {
                    break;
                }
                if self.comment == Some(buf[pos]) {
                    self.in_comment = true;
                    pos += 1;
                    continue;
                }
            }
            match self.find_terminator_or_quote(&buf[pos..]) {
                None => {
                    self.pending.extend_from_slice(&buf[pos..]);
                    pos = buf.len();
                }
                Some(i) if buf[pos + i] == self.quote => {
                    // the record contains the quote byte: csv_core parses it from the
                    // record start, continuing with `buf[pos..]` on the next iteration
                    self.slow = true;
                    self.slow_out_len = 0;
                    self.slow_ends_len = 0;
                    if !self.pending.is_empty() {
                        let pending = std::mem::take(&mut self.pending);
                        self.slow_feed(&pending)?;
                    }
                }
                Some(i) => {
                    if self.pending.is_empty() {
                        self.process_line(&buf[pos..pos + i])?;
                    } else {
                        self.pending.extend_from_slice(&buf[pos..pos + i]);
                        let line = std::mem::take(&mut self.pending);
                        self.process_line(&line)?;
                        self.pending = line;
                        self.pending.clear();
                    }
                    pos += i + 1;
                }
            }
        }
        Ok(pos)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.rows == 0 {
            return Ok(None);
        }
        let rows = std::mem::take(&mut self.rows);
        let columns = self
            .builders
            .iter_mut()
            .map(
                |builder| Ok(Arc::new(StringArray::try_from_binary(builder.finish())?) as ArrayRef),
            )
            .collect::<Result<Vec<_>, ArrowError>>()?;
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new()
                .with_match_field_names(true)
                .with_row_count(Some(rows)),
        )?;
        Ok(Some(batch))
    }

    fn can_flush_early(&self) -> bool {
        self.rows == self.batch_size
    }
}

/// Drives a [`Decoder`] from a [`BufRead`] the way `arrow::csv::BufReader` does
/// (reader/mod.rs:526-544), yielding one batch per `batch_size` records.
pub struct DecoderBatchReader<R, D> {
    reader: R,
    decoder: D,
}

impl<R: BufRead, D: Decoder> DecoderBatchReader<R, D> {
    pub fn new(reader: R, decoder: D) -> Self {
        Self { reader, decoder }
    }

    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            let buf = self.reader.fill_buf()?;
            let decoded = self.decoder.decode(buf)?;
            self.reader.consume(decoded);
            if decoded == 0 || self.decoder.can_flush_early() {
                break;
            }
        }
        self.decoder.flush()
    }
}

impl<R: BufRead, D: Decoder> Iterator for DecoderBatchReader<R, D> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::csv::ReaderBuilder as ArrowReaderBuilder;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn options(num_columns: usize, projection: &[usize]) -> ProjectedCsvOptions {
        let fields = (0..num_columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>();
        ProjectedCsvOptions {
            schema: Arc::new(Schema::new(fields)),
            projection: projection.to_vec(),
            delimiter: b',',
            quote: b'"',
            escape: None,
            comment: None,
            terminator: None,
            has_header: true,
            truncated_rows: false,
            batch_size: 8192,
        }
    }

    fn arrow_decoder(o: &ProjectedCsvOptions) -> datafusion::arrow::csv::reader::Decoder {
        let mut builder = ArrowReaderBuilder::new(Arc::clone(&o.schema))
            .with_delimiter(o.delimiter)
            .with_batch_size(o.batch_size)
            .with_header(o.has_header)
            .with_quote(o.quote)
            .with_truncated_rows(o.truncated_rows)
            .with_projection(o.projection.clone());
        if let Some(t) = o.terminator {
            builder = builder.with_terminator(t);
        }
        if let Some(e) = o.escape {
            builder = builder.with_escape(e);
        }
        if let Some(c) = o.comment {
            builder = builder.with_comment(c);
        }
        builder.build_decoder()
    }

    fn run<D: Decoder>(
        mut decoder: D,
        input: &[u8],
        chunk: usize,
    ) -> Result<Vec<RecordBatch>, String> {
        let mut batches = vec![];
        for chunk in input.chunks(chunk) {
            let mut buf = chunk;
            while !buf.is_empty() {
                let decoded = decoder.decode(buf).map_err(|e| e.to_string())?;
                buf = &buf[decoded..];
                if decoded == 0 || decoder.can_flush_early() {
                    batches.extend(decoder.flush().map_err(|e| e.to_string())?);
                }
                assert!(decoded != 0 || buf.is_empty(), "no progress");
            }
        }
        assert_eq!(decoder.decode(&[]).map_err(|e| e.to_string())?, 0);
        batches.extend(decoder.flush().map_err(|e| e.to_string())?);
        Ok(batches)
    }

    /// arrow reports a record with too many fields as "got more than N" or "got N"
    /// depending on where the input chunks end (csv_core `OutputEndsFull` vs
    /// `InputEmpty`); the projected decoder always reports the exact count.
    fn normalize(result: Result<Vec<RecordBatch>, String>) -> Result<Vec<RecordBatch>, String> {
        result.map_err(|e| e.split(" got ").next().unwrap_or_default().to_string())
    }

    #[derive(Debug)]
    struct ArrowDecoder(datafusion::arrow::csv::reader::Decoder);

    impl Decoder for ArrowDecoder {
        fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
            self.0.decode(buf)
        }

        fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
            self.0.flush()
        }

        fn can_flush_early(&self) -> bool {
            self.0.capacity() == 0
        }
    }

    /// A header line and `num_rows` data rows of `num_columns` fields, joined by
    /// `delimiter` and `\n`; field `i` of data row `r` is `field(r, i)`.
    fn rows(
        num_columns: usize,
        num_rows: usize,
        delimiter: u8,
        field: impl Fn(usize, usize) -> Vec<u8>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        for r in 0..=num_rows {
            for i in 0..num_columns {
                if i != 0 {
                    out.push(delimiter);
                }
                if r == 0 {
                    out.extend_from_slice(format!("c{i}").as_bytes());
                } else {
                    out.extend_from_slice(&field(r - 1, i));
                }
            }
            out.push(b'\n');
        }
        out
    }

    fn assert_matches_arrow(input: &[u8], options: ProjectedCsvOptions) {
        for chunk in [1, 2, 3, 5, 7, 64] {
            for batch_size in [1, 2, 8192] {
                let o = ProjectedCsvOptions {
                    batch_size,
                    ..options.clone()
                };
                let expected = normalize(run(ArrowDecoder(arrow_decoder(&o)), input, chunk));
                let actual = normalize(
                    ProjectedCsvDecoder::try_new(o)
                        .map_err(|e| e.to_string())
                        .and_then(|d| run(d, input, chunk)),
                );
                assert_eq!(actual, expected, "chunk {chunk} batch size {batch_size}");
            }
        }
    }

    #[test]
    fn test_projected_decoder_matches_arrow() {
        let p = [2, 0, 1];
        for input in [
            &b"a,b,c\n1,2,3\n4,5,6\n"[..],
            b"a,b,c\r\n1,2,3\r\n4,5,6\r\n",
            b"a,b,c\r1,2,3\r4,5,6\r",
            b"a,b,c\n\n1,2,3\n\r\n\n4,5,6\n\n\n",
            b"a,b,c\n1,2,3\n4,5,6",
            b"a,b,c\n\"x,y\",\"he said \"\"hi\"\"\",3\n4,5,6\n",
            b"a,b,c\n\"line1\nline2\",2,3\n\"l1\r\nl2\",5,6\n",
            b"a,b,c\nab\"c,2,3\n4,5,6\n",
            b"a,b,c\n1,2,\"x\"",
            b"a,b,c\n1,2,\"x\ny",
            b"\xef\xbb\xbfa,b,c\n1,2,3\n",
            b"a,b,c\n\xef\xbb\xbfx,\"q\",3\n4,5,6\n",
            b"a,b,c\n,,\n1,,3\n,2,\n",
            b"a,b,c\n1,2,3,4,5,6,7\n",
            b"a,b,c\n\"1\",2,3,4,5,6,7\n",
            b"a,b,c\n1,2\n",
            b"a,b\n1,2,3\n",
            "a,b,c\ncaf\u{e9},\u{1F980},3\n".as_bytes(),
            b"",
            b"a,b,c\n",
            b"a,b,c",
        ] {
            assert_matches_arrow(input, options(3, &p));
        }
        assert_matches_arrow(b"a,b,c\n1,2,3\n", options(3, &[1]));
        assert_matches_arrow(b"a,b,c\n1,2,3\n", options(3, &[]));
        assert_matches_arrow(b"a,b,c\n1,2,3\n", options(3, &[0, 1, 2]));
        assert_matches_arrow(b"a\n1\n\n2\n", options(1, &[0]));
        assert_matches_arrow(
            b"a,b,c\n1\n1,2\n,\n\"q\"\n",
            ProjectedCsvOptions {
                truncated_rows: true,
                ..options(3, &p)
            },
        );
        assert_matches_arrow(
            b"a,b,c\n1,2,3,4\n",
            ProjectedCsvOptions {
                truncated_rows: true,
                ..options(3, &p)
            },
        );
        assert_matches_arrow(
            b"#c\na,b,c\n#x\n1,2,3\n#tail\n",
            ProjectedCsvOptions {
                comment: Some(b'#'),
                ..options(3, &p)
            },
        );
        assert_matches_arrow(
            b"a,b,c;1,2,3;4,5\n6;",
            ProjectedCsvOptions {
                terminator: Some(b';'),
                ..options(3, &p)
            },
        );
        assert_matches_arrow(
            b"a,b,c\n\"x\\\"y\",2,3\n",
            ProjectedCsvOptions {
                escape: Some(b'\\'),
                ..options(3, &p)
            },
        );
        assert_matches_arrow(
            b"1,2,3\n4,5,6\n",
            ProjectedCsvOptions {
                has_header: false,
                ..options(3, &p)
            },
        );
        // lines of exactly 63/64/65/127/128/129 bytes with delimiters at block positions
        // 0, 63 and 64 and at the end of the line, two rows so that batch boundaries fall
        // inside a block, also truncated (the last column missing on the first row)
        for len in [63, 64, 65, 127, 128, 129] {
            for positions in [
                vec![0],
                vec![63],
                vec![64],
                vec![0, 63, 64],
                vec![len - 1],
                vec![len - 2, len - 1],
                vec![0, len - 1],
            ] {
                let positions = positions
                    .into_iter()
                    .filter(|&p| p < len)
                    .collect::<Vec<_>>();
                let num_columns = positions.len() + 1;
                let mut line = vec![b'x'; len];
                for &p in &positions {
                    line[p] = b',';
                }
                let mut input = rows(num_columns, 0, b',', |_, _| vec![]);
                input.extend_from_slice(&line);
                input.push(b'\n');
                input.extend_from_slice(&line);
                input.push(b'\n');
                let all = (0..num_columns).collect::<Vec<_>>();
                assert_matches_arrow(&input, options(num_columns, &all));
                assert_matches_arrow(&input, options(num_columns, &[num_columns - 1]));
                assert_matches_arrow(&input, options(num_columns, &[0]));
                let mut truncated = rows(num_columns + 1, 0, b',', |_, _| vec![]);
                truncated.extend_from_slice(&line);
                truncated.push(b'\n');
                truncated.extend_from_slice(&line);
                truncated.extend_from_slice(b",tail\n");
                let all = (0..=num_columns).collect::<Vec<_>>();
                for projection in [&all[..], &[num_columns]] {
                    assert_matches_arrow(
                        &truncated,
                        ProjectedCsvOptions {
                            truncated_rows: true,
                            ..options(num_columns + 1, projection)
                        },
                    );
                }
            }
        }
        // 200 columns with every field empty (one delimiter per byte) or one byte long
        for width in [0, 1] {
            let input = rows(200, 5, b',', |r, i| {
                vec![b'0' + ((r + i) % 10) as u8; width]
            });
            let all = (0..200).collect::<Vec<_>>();
            for projection in [&all[..], &[199], &[199, 0, 100, 63, 64], &[]] {
                assert_matches_arrow(&input, options(200, projection));
            }
        }
        // 187 short columns like the Backblaze files, some empty, a few quoted rows; `-`
        // is `,` ^ 1, which a borrow-propagating zero-byte test would flag after a `,`
        let input = rows(187, 40, b',', |r, i| match (r + i) % 5 {
            0 => vec![],
            1 => format!("-{}", (r * 31 + i) % 1000).into_bytes(),
            2 if r % 13 == 0 => b"\"q,x\"".to_vec(),
            _ => b"ab".to_vec(),
        });
        let all = (0..187).collect::<Vec<_>>();
        for projection in [&all[..], &[186], &[0, 2, 4], &[186, 5, 100]] {
            assert_matches_arrow(&input, options(187, projection));
        }
        // multi-byte UTF-8 (lead and continuation bytes 0x80..0xf4) adjacent to delimiters
        let high = (0x80..=0x7ff)
            .filter_map(char::from_u32)
            .collect::<String>();
        let input = format!(
            "a,b,c,d\n\u{e9},\u{1F980}\u{7ff},\u{800}\u{ffff},\u{10ffff}\n\u{80}\u{bf},\u{c0}x,\u{10000},\u{fffd}\n{high},{high},{high},{high}\n"
        );
        for projection in [&[0, 1, 2, 3][..], &[3]] {
            assert_matches_arrow(input.as_bytes(), options(4, projection));
        }
        // delimiters other than the comma, including 0x00, the pad value of a partial word
        for delimiter in [b'|', 0x7f, 0x01, 0x00, b';'] {
            let input = rows(5, 70, delimiter, |r, i| {
                if (r + i) % 3 == 0 {
                    vec![]
                } else {
                    vec![b'a'; (r * 7 + i * 13) % 40]
                }
            });
            for projection in [&[4, 0, 2][..], &[0, 1, 2, 3, 4]] {
                assert_matches_arrow(
                    &input,
                    ProjectedCsvOptions {
                        delimiter,
                        terminator: (delimiter == b';').then_some(b'\n'),
                        ..options(5, projection)
                    },
                );
            }
        }
        // the quote byte right after the terminator of a record carried in `pending`, and
        // an extra delimiter in the second block of a record
        let mut long = vec![b'y'; 130];
        long[40] = b',';
        long[100] = b',';
        let mut input = b"a,b,c\n".to_vec();
        input.extend_from_slice(&long);
        input.extend_from_slice(b"\n\"q\",2,3\n");
        input.extend_from_slice(&long);
        input.extend_from_slice(b"\n\"\n");
        assert_matches_arrow(&input, options(3, &p));
        let mut input = b"a,b,c\n".to_vec();
        input.extend_from_slice(&long);
        input.extend_from_slice(b",\n");
        assert_matches_arrow(&input, options(3, &p));
    }

    #[test]
    fn test_eq_mask() {
        fn naive(word: u64, delimiter: u8) -> u8 {
            word.to_le_bytes()
                .iter()
                .enumerate()
                .filter(|&(_, &b)| b == delimiter)
                .fold(0, |mask, (i, _)| mask | (1 << i))
        }
        // xorshift64
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut random = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for delimiter in 0..=u8::MAX {
            let pattern = u64::from(delimiter) * 0x0101_0101_0101_0101;
            // every byte value in every position, the other bytes random
            for position in 0..8 {
                for value in 0..=u8::MAX {
                    let mut bytes = random().to_le_bytes();
                    bytes[position] = value;
                    let word = u64::from_le_bytes(bytes);
                    assert_eq!(eq_mask(word, pattern), naive(word, delimiter), "{word:#x}");
                }
            }
            for _ in 0..1000 {
                let word = random();
                assert_eq!(eq_mask(word, pattern), naive(word, delimiter), "{word:#x}");
            }
            let mut block = [0; 64];
            for _ in 0..64 {
                for chunk in block.chunks_exact_mut(8) {
                    chunk.copy_from_slice(&random().to_le_bytes());
                }
                let expected = block
                    .iter()
                    .enumerate()
                    .filter(|&(_, &b)| b == delimiter)
                    .fold(0u64, |mask, (i, _)| mask | (1 << i));
                assert_eq!(delimiter_mask(&block, pattern), expected);
                // a partial block (the tail of a line) with pad bytes masked off
                let len = 1 + (random() % 63) as usize;
                assert_eq!(
                    delimiter_mask(&block[..len], pattern),
                    expected & ((1 << len) - 1)
                );
            }
        }
    }

    #[test]
    fn test_projected_decoder_ignores_comment_lines_like_spark() -> Result<(), ArrowError> {
        // csv_core emits a bogus record for a comment at the end of the file without a
        // trailing newline, and never ends a comment when the terminator is not '\n'
        for (input, terminator) in [
            (&b"#c\na,b,c\n#x\n1,2,3\n#tail"[..], None),
            (&b"#x;a,b,c;1,2,3;"[..], Some(b';')),
        ] {
            let decoder = ProjectedCsvDecoder::try_new(ProjectedCsvOptions {
                comment: Some(b'#'),
                terminator,
                ..options(3, &[0, 1, 2])
            })?;
            let batches = run(decoder, input, 4096).map_err(ArrowError::CsvError)?;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1);
        }
        Ok(())
    }
}
