use std::io::{self, Read};

use bytes::Bytes;
use datafusion_common::Result;
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

pub(super) struct LossyUtf8Reader<R> {
    input: R,
    decoder: LossyUtf8Decoder,
    decoded: Bytes,
    decoded_offset: usize,
    finished: bool,
}

impl<R> LossyUtf8Reader<R> {
    pub(super) fn new(input: R) -> Self {
        Self {
            input,
            decoder: LossyUtf8Decoder::default(),
            decoded: Bytes::new(),
            decoded_offset: 0,
            finished: false,
        }
    }
}

impl<R: Read> Read for LossyUtf8Reader<R> {
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

pub(super) fn decode_utf8_lossy_stream<'a>(
    input: BoxStream<'a, Result<Bytes>>,
) -> BoxStream<'a, Result<Bytes>> {
    stream::unfold(
        (input, LossyUtf8Decoder::default(), false),
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
    use futures::{StreamExt, stream};

    use super::{LossyUtf8Decoder, LossyUtf8Reader, decode_utf8_lossy_stream};

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

    #[test]
    fn test_lossy_utf8_reader_replaces_incomplete_sequence_at_end_of_input() -> std::io::Result<()>
    {
        let mut reader = LossyUtf8Reader::new(Cursor::new(b"value\xf0\x9f"));
        let mut output = String::new();
        reader.read_to_string(&mut output)?;

        assert_eq!(output, "value\u{FFFD}");
        Ok(())
    }

    #[test]
    fn test_lossy_utf8_reader_replaces_surrogate_unit_once() -> std::io::Result<()> {
        let input = b"value_\xed\xa0\x80_done";
        let chunks = Cursor::new(&input[..7]).chain(Cursor::new(&input[7..]));
        let mut reader = LossyUtf8Reader::new(chunks);
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
        let output = decode_utf8_lossy_stream(input)
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
            let output = decode_utf8_lossy_stream(stream)
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
