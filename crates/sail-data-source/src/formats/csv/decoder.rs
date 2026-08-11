use std::io::{self, Read};

use bytes::Bytes;
use datafusion_common::Result;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};

const READ_BUFFER_SIZE: usize = 8 * 1024;
const REPLACEMENT_CHARACTER: &[u8] = "\u{FFFD}".as_bytes();

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
        let mut output = Vec::with_capacity(bytes.len());
        let mut offset = 0;

        while offset < bytes.len() {
            match std::str::from_utf8(&bytes[offset..]) {
                Ok(valid) => {
                    output.extend_from_slice(valid.as_bytes());
                    break;
                }
                Err(error) => {
                    let valid_end = offset + error.valid_up_to();
                    output.extend_from_slice(&bytes[offset..valid_end]);

                    match error.error_len() {
                        Some(invalid_length) => {
                            output.extend_from_slice(REPLACEMENT_CHARACTER);
                            offset = valid_end + invalid_length;
                        }
                        None if end_of_input => {
                            output.extend_from_slice(REPLACEMENT_CHARACTER);
                            break;
                        }
                        None => {
                            self.incomplete.extend_from_slice(&bytes[valid_end..]);
                            break;
                        }
                    }
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
    fn test_lossy_utf8_reader_replaces_incomplete_sequence_at_end_of_input() -> std::io::Result<()>
    {
        let mut reader = LossyUtf8Reader::new(Cursor::new(b"value\xf0\x9f"));
        let mut output = String::new();
        reader.read_to_string(&mut output)?;

        assert_eq!(output, "value\u{FFFD}");
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
}
