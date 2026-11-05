use std::io::{self, Read};

use bytes::Bytes;
use datafusion_common::Result;
use datafusion_common::config::CsvOptions;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};

const READ_BUFFER_SIZE: usize = 8 * 1024;
const REPLACEMENT_CHARACTER: &[u8] = "\u{FFFD}".as_bytes();

#[derive(Debug, PartialEq)]
enum Utf8Unit {
    Valid(usize),
    Malformed(usize),
    Incomplete,
}

fn is_continuation(byte: u8) -> bool {
    byte & 0xc0 == 0x80
}

/// Classifies the next UTF-8 unit and returns the maximal malformed prefix length.
fn classify_utf8_unit(input: &[u8], end_of_input: bool) -> Utf8Unit {
    let first = input[0];
    match first {
        0x00..=0x7f => Utf8Unit::Valid(1),
        0xc2..=0xdf => {
            if input.len() < 2 {
                return if end_of_input {
                    Utf8Unit::Malformed(1)
                } else {
                    Utf8Unit::Incomplete
                };
            }
            if is_continuation(input[1]) {
                Utf8Unit::Valid(2)
            } else {
                Utf8Unit::Malformed(1)
            }
        }
        0xe0..=0xef => {
            if input.len() < 2 {
                return if end_of_input {
                    Utf8Unit::Malformed(1)
                } else {
                    Utf8Unit::Incomplete
                };
            }

            let second = input[1];
            if !is_continuation(second) || (first == 0xe0 && second < 0xa0) {
                return Utf8Unit::Malformed(1);
            }
            if input.len() < 3 {
                return if end_of_input {
                    Utf8Unit::Malformed(2)
                } else {
                    Utf8Unit::Incomplete
                };
            }

            let third = input[2];
            if !is_continuation(third) {
                Utf8Unit::Malformed(2)
            } else if first == 0xed && second >= 0xa0 {
                // A complete UTF-16 surrogate sequence is one malformed three-byte unit.
                Utf8Unit::Malformed(3)
            } else {
                Utf8Unit::Valid(3)
            }
        }
        0xf0..=0xf4 => {
            if input.len() < 2 {
                return if end_of_input {
                    Utf8Unit::Malformed(1)
                } else {
                    Utf8Unit::Incomplete
                };
            }

            let second = input[1];
            if !is_continuation(second)
                || (first == 0xf0 && second < 0x90)
                || (first == 0xf4 && second > 0x8f)
            {
                return Utf8Unit::Malformed(1);
            }
            if input.len() < 3 {
                return if end_of_input {
                    Utf8Unit::Malformed(2)
                } else {
                    Utf8Unit::Incomplete
                };
            }

            if !is_continuation(input[2]) {
                return Utf8Unit::Malformed(2);
            }
            if input.len() < 4 {
                return if end_of_input {
                    Utf8Unit::Malformed(3)
                } else {
                    Utf8Unit::Incomplete
                };
            }

            if is_continuation(input[3]) {
                Utf8Unit::Valid(4)
            } else {
                Utf8Unit::Malformed(3)
            }
        }
        _ => Utf8Unit::Malformed(1),
    }
}

#[derive(Debug, Default)]
struct LossyUtf8Decoder {
    incomplete: Vec<u8>,
}

impl LossyUtf8Decoder {
    fn decode_bytes(&mut self, input: Bytes) -> Bytes {
        if self.incomplete.is_empty() && std::str::from_utf8(&input).is_ok() {
            return input;
        }
        self.decode(&input)
    }

    fn decode(&mut self, input: &[u8]) -> Bytes {
        let mut bytes = std::mem::take(&mut self.incomplete);
        bytes.extend_from_slice(input);
        self.repair(bytes, false)
    }

    fn finish(&mut self) -> Bytes {
        let bytes = std::mem::take(&mut self.incomplete);
        self.repair(bytes, true)
    }

    fn repair(&mut self, bytes: Vec<u8>, end_of_input: bool) -> Bytes {
        if std::str::from_utf8(&bytes).is_ok() {
            return Bytes::from(bytes);
        }

        let mut output = Vec::with_capacity(bytes.len());
        let mut offset = 0;

        while offset < bytes.len() {
            match classify_utf8_unit(&bytes[offset..], end_of_input) {
                Utf8Unit::Valid(length) => {
                    output.extend_from_slice(&bytes[offset..offset + length]);
                    offset += length;
                }
                Utf8Unit::Malformed(length) => {
                    output.extend_from_slice(REPLACEMENT_CHARACTER);
                    offset += length;
                }
                Utf8Unit::Incomplete => {
                    self.incomplete.extend_from_slice(&bytes[offset..]);
                    break;
                }
            }
        }

        debug_assert!(self.incomplete.len() <= 3);
        Bytes::from(output)
    }
}

/// Drops the lines that are empty after trimming, as Spark does before it parses CSV rows
/// line by line (`CSVExprUtils.filterCommentAndEmpty`, where `String.trim` strips every
/// character up to U+0020). Such characters are single bytes in UTF-8.
#[derive(Debug)]
struct BlankLineFilter {
    terminator: Option<u8>,
    /// The bytes of the current line, held back while they are all whitespace.
    blank: Vec<u8>,
    /// Whether the current line has a byte that is not whitespace.
    in_line: bool,
    after_cr: AfterCr,
}

/// Whether the previous line ended with `\r`, so that a `\n` right after it belongs to it.
#[derive(Debug, Default, Clone, Copy)]
enum AfterCr {
    #[default]
    No,
    Kept,
    Dropped,
}

impl BlankLineFilter {
    fn new(terminator: Option<u8>) -> Self {
        Self {
            terminator,
            blank: Vec::new(),
            in_line: false,
            after_cr: AfterCr::No,
        }
    }

    fn after_line_end(&self, byte: u8, after_cr: AfterCr) -> AfterCr {
        if self.terminator.is_none() && byte == b'\r' {
            after_cr
        } else {
            AfterCr::No
        }
    }

    /// Without a terminator, csv_core ends a record at `\n`, `\r` or `\r\n`.
    fn is_terminator(&self, byte: u8) -> bool {
        match self.terminator {
            Some(terminator) => byte == terminator,
            None => byte == b'\n' || byte == b'\r',
        }
    }

    fn find_terminator(&self, input: &[u8]) -> Option<usize> {
        match self.terminator {
            Some(terminator) => memchr::memchr(terminator, input),
            None => memchr::memchr2(b'\n', b'\r', input),
        }
    }

    /// Returns the input without its blank lines, or the input itself if it has none.
    /// A blank line that the input ends with is held back until the line ends.
    fn filter(&mut self, input: Bytes) -> Bytes {
        let mut output: Option<Vec<u8>> = None;
        // `input[kept..position]` is kept, but not copied into `output` yet.
        let mut kept = 0;
        let mut position = 0;
        while position < input.len() {
            if self.in_line {
                match self.find_terminator(&input[position..]) {
                    Some(offset) => {
                        position += offset;
                        self.after_cr = self.after_line_end(input[position], AfterCr::Kept);
                        self.in_line = false;
                        position += 1;
                    }
                    None => position = input.len(),
                }
                continue;
            }
            match std::mem::take(&mut self.after_cr) {
                AfterCr::Kept if input[position] == b'\n' => {
                    position += 1;
                    continue;
                }
                AfterCr::Dropped if input[position] == b'\n' => {
                    output
                        .get_or_insert_with(Vec::new)
                        .extend_from_slice(&input[kept..position]);
                    position += 1;
                    kept = position;
                    continue;
                }
                _ => {}
            }
            let end = position
                + input[position..]
                    .iter()
                    .take_while(|&&byte| byte <= b' ' && !self.is_terminator(byte))
                    .count();
            if end < input.len() && !self.is_terminator(input[end]) {
                if !self.blank.is_empty() {
                    let output = output.get_or_insert_with(Vec::new);
                    output.extend_from_slice(&input[kept..position]);
                    output.append(&mut self.blank);
                    kept = position;
                }
                self.in_line = true;
                position = end + 1;
                continue;
            }
            if end == position && self.blank.is_empty() {
                // An empty line is kept: the parsers skip it between records,
                // and keep it inside a quoted value.
                self.after_cr = self.after_line_end(input[position], AfterCr::Kept);
                position += 1;
                continue;
            }
            output
                .get_or_insert_with(Vec::new)
                .extend_from_slice(&input[kept..position]);
            if end < input.len() {
                self.blank.clear();
                self.after_cr = self.after_line_end(input[end], AfterCr::Dropped);
                position = end + 1;
            } else {
                self.blank.extend_from_slice(&input[position..end]);
                position = end;
            }
            kept = position;
        }
        match output {
            None => input,
            Some(mut output) => {
                output.extend_from_slice(&input[kept..]);
                Bytes::from(output)
            }
        }
    }
}

/// Prepares the bytes of a CSV file for the parser: invalid UTF-8 is replaced, and
/// blank lines are dropped unless values may span lines.
#[derive(Debug, Default)]
pub(super) struct CsvInputDecoder {
    utf8: LossyUtf8Decoder,
    blank_lines: Option<BlankLineFilter>,
}

impl CsvInputDecoder {
    pub(super) fn new(options: &CsvOptions) -> Self {
        // With `multiLine`, Spark parses the file as one stream and keeps blank lines as records.
        let multi_line = options.newlines_in_values.unwrap_or(false);
        Self {
            utf8: LossyUtf8Decoder::default(),
            blank_lines: (!multi_line).then(|| BlankLineFilter::new(options.terminator)),
        }
    }

    fn decode_bytes(&mut self, input: Bytes) -> Bytes {
        let decoded = self.utf8.decode_bytes(input);
        self.filter(decoded)
    }

    fn decode(&mut self, input: &[u8]) -> Bytes {
        let decoded = self.utf8.decode(input);
        self.filter(decoded)
    }

    fn finish(&mut self) -> Bytes {
        let decoded = self.utf8.finish();
        self.filter(decoded)
    }

    fn filter(&mut self, input: Bytes) -> Bytes {
        match &mut self.blank_lines {
            Some(filter) => filter.filter(input),
            None => input,
        }
    }
}

pub(super) struct CsvInputReader<R> {
    input: R,
    decoder: CsvInputDecoder,
    decoded: Bytes,
    decoded_offset: usize,
    finished: bool,
}

impl<R> CsvInputReader<R> {
    pub(super) fn new(input: R, decoder: CsvInputDecoder) -> Self {
        Self {
            input,
            decoder,
            decoded: Bytes::new(),
            decoded_offset: 0,
            finished: false,
        }
    }
}

impl<R: Read> Read for CsvInputReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }

        loop {
            if self.decoded_offset < self.decoded.len() {
                let available = &self.decoded[self.decoded_offset..];
                let length = available.len().min(output.len());
                output[..length].copy_from_slice(&available[..length]);
                self.decoded_offset += length;
                return Ok(length);
            }

            if self.finished {
                return Ok(0);
            }

            let mut input = [0; READ_BUFFER_SIZE];
            let length = self.input.read(&mut input)?;
            self.decoded = if length == 0 {
                self.finished = true;
                self.decoder.finish()
            } else {
                self.decoder.decode(&input[..length])
            };
            self.decoded_offset = 0;
        }
    }
}

pub(super) fn decode_csv_input_stream<'a>(
    input: BoxStream<'a, Result<Bytes>>,
    decoder: CsvInputDecoder,
) -> BoxStream<'a, Result<Bytes>> {
    stream::unfold(
        (input, decoder, false),
        |(mut input, mut decoder, mut finished)| async move {
            loop {
                if finished {
                    return None;
                }

                match input.try_next().await {
                    Ok(Some(bytes)) => {
                        let decoded = decoder.decode_bytes(bytes);
                        if !decoded.is_empty() {
                            return Some((Ok(decoded), (input, decoder, finished)));
                        }
                    }
                    Ok(None) => {
                        finished = true;
                        let decoded = decoder.finish();
                        if !decoded.is_empty() {
                            return Some((Ok(decoded), (input, decoder, finished)));
                        }
                    }
                    Err(error) => {
                        return Some((Err(error), (input, decoder, finished)));
                    }
                }
            }
        },
    )
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use bytes::Bytes;
    use datafusion_common::Result;
    use datafusion_common::config::CsvOptions;
    use futures::{StreamExt, stream};

    use super::{
        BlankLineFilter, CsvInputDecoder, CsvInputReader, LossyUtf8Decoder, decode_csv_input_stream,
    };

    fn split_bytes(input: &[u8], boundary_mask: usize) -> Vec<Bytes> {
        let mut chunks = Vec::new();
        let mut start = 0;
        for boundary in 1..input.len() {
            if boundary_mask & (1 << (boundary - 1)) != 0 {
                chunks.push(Bytes::copy_from_slice(&input[start..boundary]));
                start = boundary;
            }
        }
        chunks.push(Bytes::copy_from_slice(&input[start..]));
        chunks
    }

    fn assert_decoder_output_for_all_chunkings(input: &[u8], expected: &[u8]) {
        let boundary_count = input.len().saturating_sub(1);
        for boundary_mask in 0..(1 << boundary_count) {
            let mut decoder = LossyUtf8Decoder::default();
            let mut output = Vec::new();
            for chunk in split_bytes(input, boundary_mask) {
                output.extend_from_slice(&decoder.decode_bytes(chunk));
            }
            output.extend_from_slice(&decoder.finish());
            assert_eq!(output, expected, "boundary mask {boundary_mask:#b}");
        }
    }

    #[test]
    fn test_lossy_utf8_decoder_replaces_invalid_sequences() {
        let mut decoder = LossyUtf8Decoder::default();
        let mut output = decoder.decode(b"caf\xe9_bad_\xff").to_vec();
        output.extend_from_slice(&decoder.finish());

        assert_eq!(output, "caf\u{FFFD}_bad_\u{FFFD}".as_bytes());
    }

    #[test]
    fn test_lossy_utf8_decoder_preserves_multibyte_characters_across_chunks() {
        let input = "caf\u{e9}_\u{1F980}".as_bytes();
        let chunks = [&input[..4], &input[4..7], &input[7..9], &input[9..]];
        let mut decoder = LossyUtf8Decoder::default();
        let mut output = Vec::new();

        for chunk in chunks {
            output.extend_from_slice(&decoder.decode(chunk));
        }
        output.extend_from_slice(&decoder.finish());

        assert_eq!(output, input);
    }

    #[test]
    fn test_lossy_utf8_decoder_matches_jdk_malformed_units_for_all_chunkings() {
        let replacement = "\u{FFFD}".as_bytes();
        let triple_replacement = "\u{FFFD}\u{FFFD}\u{FFFD}".as_bytes();

        for input in [b"\xed\xa0\x80".as_slice(), b"\xed\xbf\xbf", b"\xed\xa0"] {
            assert_decoder_output_for_all_chunkings(input, replacement);
        }
        assert_decoder_output_for_all_chunkings(b"\xed\xa0x", "\u{FFFD}x".as_bytes());
        assert_decoder_output_for_all_chunkings(b"\xe0\x80\x80", triple_replacement);
        assert_decoder_output_for_all_chunkings(b"\xf0\x90\x80x", "\u{FFFD}x".as_bytes());
        assert_decoder_output_for_all_chunkings(
            "\u{e9}\u{1F980}".as_bytes(),
            "\u{e9}\u{1F980}".as_bytes(),
        );
    }

    fn assert_filter_output_for_all_chunkings(
        terminator: Option<u8>,
        input: &[u8],
        expected: &[u8],
    ) {
        let boundary_count = input.len().saturating_sub(1);
        for boundary_mask in 0..(1 << boundary_count) {
            let mut filter = BlankLineFilter::new(terminator);
            let output = split_bytes(input, boundary_mask)
                .into_iter()
                .map(|chunk| filter.filter(chunk))
                .collect::<Vec<_>>()
                .concat();
            assert_eq!(
                String::from_utf8_lossy(&output),
                String::from_utf8_lossy(expected),
                "input {:?}, boundary mask {boundary_mask:#b}",
                String::from_utf8_lossy(input),
            );
        }
    }

    #[test]
    fn test_blank_line_filter_drops_lines_that_are_empty_after_trimming() {
        assert_filter_output_for_all_chunkings(None, b"a\n   \nb\n", b"a\nb\n");
        assert_filter_output_for_all_chunkings(None, b"a\r\n \t\r\nb\r\n", b"a\r\nb\r\n");
        assert_filter_output_for_all_chunkings(None, b"a\r \rb\r", b"a\rb\r");
        assert_filter_output_for_all_chunkings(None, b"\n\x0c\n\na\n", b"\n\na\n");
        assert_filter_output_for_all_chunkings(None, b"a\n \t", b"a\n");
    }

    #[test]
    fn test_blank_line_filter_keeps_empty_lines() {
        // The parsers skip empty lines between records, and keep them inside quoted values.
        assert_filter_output_for_all_chunkings(
            None,
            b"a\n\n\"p\n\nq\"\r\n\r\n",
            b"a\n\n\"p\n\nq\"\r\n\r\n",
        );
    }

    #[test]
    fn test_blank_line_filter_keeps_lines_with_content() {
        assert_filter_output_for_all_chunkings(None, b"a\r\nb\r\n", b"a\r\nb\r\n");
        assert_filter_output_for_all_chunkings(None, b"  a,\t\n \t b\n", b"  a,\t\n \t b\n");
        assert_filter_output_for_all_chunkings(None, "\u{a0}\n".as_bytes(), "\u{a0}\n".as_bytes());
    }

    #[test]
    fn test_blank_line_filter_splits_lines_at_the_terminator() {
        assert_filter_output_for_all_chunkings(Some(b';'), b"a; \n;b\n ;", b"a;b\n ;");
    }

    #[test]
    fn test_csv_input_decoder_keeps_blank_lines_for_multi_line_values() {
        for (newlines_in_values, expected) in [
            (None, b"a\nb\n".as_slice()),
            (Some(false), b"a\nb\n"),
            (Some(true), b"a\n  \nb\n"),
        ] {
            let options = CsvOptions {
                newlines_in_values,
                ..CsvOptions::default()
            };
            let mut decoder = CsvInputDecoder::new(&options);
            let mut output = decoder.decode(b"a\n  \nb\n").to_vec();
            output.extend_from_slice(&decoder.finish());
            assert_eq!(
                output, expected,
                "newlines_in_values {newlines_in_values:?}"
            );
        }
    }

    #[test]
    fn test_lossy_utf8_reader_replaces_incomplete_sequence_at_end_of_input() -> std::io::Result<()>
    {
        let mut reader =
            CsvInputReader::new(Cursor::new(b"value\xf0\x9f"), CsvInputDecoder::default());
        let mut output = String::new();
        reader.read_to_string(&mut output)?;

        assert_eq!(output, "value\u{FFFD}");
        Ok(())
    }

    #[test]
    fn test_lossy_utf8_reader_replaces_surrogate_unit_once() -> std::io::Result<()> {
        let input = b"value_\xed\xa0\x80_done";
        let chunks = Cursor::new(&input[..7]).chain(Cursor::new(&input[7..]));
        let mut reader = CsvInputReader::new(chunks, CsvInputDecoder::default());
        let mut output = String::new();
        reader.read_to_string(&mut output)?;

        assert_eq!(output, "value_\u{FFFD}_done");
        Ok(())
    }

    #[tokio::test]
    async fn test_lossy_utf8_stream_preserves_split_multibyte_character() -> Result<()> {
        let input = stream::iter([
            Ok(Bytes::from_static(b"caf\xc3")),
            Ok(Bytes::from_static(b"\xa9\xf0\x9f")),
            Ok(Bytes::from_static(b"\xa6\x80")),
        ])
        .boxed();
        let output = decode_csv_input_stream(input, CsvInputDecoder::default())
            .collect::<Vec<Result<Bytes>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .concat();

        assert_eq!(output, "caf\u{e9}\u{1F980}".as_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn test_lossy_utf8_stream_replaces_surrogate_unit_once_for_all_chunkings() -> Result<()> {
        let input = b"\xed\xa0\x80";
        for boundary_mask in 0..(1 << (input.len() - 1)) {
            let stream = stream::iter(
                split_bytes(input, boundary_mask)
                    .into_iter()
                    .map(Ok::<_, datafusion_common::DataFusionError>),
            )
            .boxed();
            let output = decode_csv_input_stream(stream, CsvInputDecoder::default())
                .collect::<Vec<Result<Bytes>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?
                .concat();

            assert_eq!(
                output,
                "\u{FFFD}".as_bytes(),
                "boundary mask {boundary_mask:#b}"
            );
        }
        Ok(())
    }
}
