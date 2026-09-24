//! Shared codecs for ordered on-disk system store keys and serialized values.
//!
//! All disk KV backends use these codecs so that durable key layouts remain independent of the
//! backend implementation. Memory storage keeps typed values directly and does not need them.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::model::{
    JobPrimaryKey, MetricAttributeKey, MetricSeriesKey, MetricSeriesKind, OptionPrimaryKey,
    SessionPrimaryKey, StagePrimaryKey, TaskPrimaryKey, WorkerPrimaryKey,
};
use crate::predicate::TimestampMicros;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct CodecError {
    message: String,
}

impl CodecError {
    fn invalid_key() -> Self {
        Self {
            message: "invalid system store key".to_string(),
        }
    }

    fn invalid_value(message: String) -> Self {
        Self {
            message: format!("invalid system store value: {message}"),
        }
    }
}

pub type CodecResult<T> = Result<T, CodecError>;

/// The fixed durable key for the next metric series ID metadata entry.
pub(crate) const NEXT_METRIC_SERIES_ID_KEY: &[u8] = b"next_metric_series_id";

/// Codec for keys whose byte ordering must preserve their Rust ordering.
pub trait OrderedKeyCodec: Sized {
    fn encode_key(&self, output: &mut Vec<u8>);
    fn decode_key(input: &[u8]) -> CodecResult<Self>;
}

/// Convenience methods for [`OrderedKeyCodec`].
pub trait OrderedKeyCodecExt: OrderedKeyCodec {
    fn encoded_key(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_key(&mut output);
        output
    }
}

impl<T: OrderedKeyCodec> OrderedKeyCodecExt for T {}

/// Codec for values stored in on-disk KV backends.
pub trait ValueCodec: Sized {
    fn encode_value(&self) -> CodecResult<Vec<u8>>;
    fn decode_value(input: &[u8]) -> CodecResult<Self>;
}

impl<T> ValueCodec for T
where
    T: DeserializeOwned + serde::Serialize,
{
    fn encode_value(&self) -> CodecResult<Vec<u8>> {
        postcard::to_stdvec(self).map_err(|error| {
            CodecError::invalid_value(format!("failed to encode system store value: {error}"))
        })
    }

    fn decode_value(input: &[u8]) -> CodecResult<Self> {
        postcard::from_bytes(input).map_err(|error| {
            CodecError::invalid_value(format!("failed to decode system store value: {error}"))
        })
    }
}

impl OrderedKeyCodec for String {
    fn encode_key(&self, output: &mut Vec<u8>) {
        for byte in self.bytes() {
            if byte == 0 {
                output.extend_from_slice(&[0, 0xff]);
            } else {
                output.push(byte);
            }
        }
        output.extend_from_slice(&[0, 0]);
    }

    fn decode_key(input: &[u8]) -> CodecResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let value = decoder.string()?;
        decoder.finish()?;
        Ok(value)
    }
}

impl OrderedKeyCodec for u64 {
    fn encode_key(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.to_be_bytes());
    }

    fn decode_key(input: &[u8]) -> CodecResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let value = decoder.u64()?;
        decoder.finish()?;
        Ok(value)
    }
}

impl OrderedKeyCodec for TimestampMicros {
    fn encode_key(&self, output: &mut Vec<u8>) {
        ((self.0 as u64) ^ (1_u64 << 63)).encode_key(output);
    }

    fn decode_key(input: &[u8]) -> CodecResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let timestamp = decoder.u64()? ^ (1_u64 << 63);
        decoder.finish()?;
        Ok(Self(timestamp as i64))
    }
}

impl OrderedKeyCodec for (u64, TimestampMicros) {
    fn encode_key(&self, output: &mut Vec<u8>) {
        self.0.encode_key(output);
        self.1.encode_key(output);
    }

    fn decode_key(input: &[u8]) -> CodecResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let id = decoder.u64()?;
        let timestamp = decoder.u64()? ^ (1_u64 << 63);
        decoder.finish()?;
        Ok((id, TimestampMicros(timestamp as i64)))
    }
}

impl OrderedKeyCodec for (String, u64) {
    fn encode_key(&self, output: &mut Vec<u8>) {
        self.0.encode_key(output);
        self.1.encode_key(output);
    }

    fn decode_key(input: &[u8]) -> CodecResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let first = decoder.string()?;
        let second = decoder.u64()?;
        decoder.finish()?;
        Ok((first, second))
    }
}

impl OrderedKeyCodec for (MetricAttributeKey, u64) {
    fn encode_key(&self, output: &mut Vec<u8>) {
        self.0.encode_key(output);
        self.1.encode_key(output);
    }

    fn decode_key(input: &[u8]) -> CodecResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let key = MetricAttributeKey {
            key: decoder.string()?,
            value: decoder.string()?,
        };
        let value = decoder.u64()?;
        decoder.finish()?;
        Ok((key, value))
    }
}

macro_rules! ordered_key {
    ($type:ty, $encode:expr, $decode:expr) => {
        impl OrderedKeyCodec for $type {
            fn encode_key(&self, output: &mut Vec<u8>) {
                $encode(self, output)
            }

            fn decode_key(input: &[u8]) -> CodecResult<Self> {
                let mut decoder = KeyDecoder::new(input);
                let decode: fn(&mut KeyDecoder<'_>) -> CodecResult<$type> = $decode;
                let value = decode(&mut decoder)?;
                decoder.finish()?;
                Ok(value)
            }
        }
    };
}

ordered_key!(
    OptionPrimaryKey,
    |key: &OptionPrimaryKey, output: &mut Vec<u8>| key.key.encode_key(output),
    |decoder: &mut KeyDecoder<'_>| Ok(OptionPrimaryKey {
        key: decoder.string()?
    })
);
ordered_key!(
    SessionPrimaryKey,
    |key: &SessionPrimaryKey, output: &mut Vec<u8>| key.session_id.encode_key(output),
    |decoder: &mut KeyDecoder<'_>| Ok(SessionPrimaryKey {
        session_id: decoder.string()?
    })
);
ordered_key!(
    JobPrimaryKey,
    |key: &JobPrimaryKey, output: &mut Vec<u8>| {
        key.session_id.encode_key(output);
        key.job_id.encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| Ok(JobPrimaryKey {
        session_id: decoder.string()?,
        job_id: decoder.u64()?
    })
);
ordered_key!(
    StagePrimaryKey,
    |key: &StagePrimaryKey, output: &mut Vec<u8>| {
        key.session_id.encode_key(output);
        key.job_id.encode_key(output);
        key.stage.encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| Ok(StagePrimaryKey {
        session_id: decoder.string()?,
        job_id: decoder.u64()?,
        stage: decoder.u64()?
    })
);
ordered_key!(
    TaskPrimaryKey,
    |key: &TaskPrimaryKey, output: &mut Vec<u8>| {
        key.session_id.encode_key(output);
        key.job_id.encode_key(output);
        key.stage.encode_key(output);
        key.partition.encode_key(output);
        key.attempt.encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| Ok(TaskPrimaryKey {
        session_id: decoder.string()?,
        job_id: decoder.u64()?,
        stage: decoder.u64()?,
        partition: decoder.u64()?,
        attempt: decoder.u64()?
    })
);
ordered_key!(
    WorkerPrimaryKey,
    |key: &WorkerPrimaryKey, output: &mut Vec<u8>| {
        key.session_id.encode_key(output);
        key.worker_id.encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| Ok(WorkerPrimaryKey {
        session_id: decoder.string()?,
        worker_id: decoder.u64()?
    })
);
ordered_key!(
    MetricSeriesKey,
    |key: &MetricSeriesKey, output: &mut Vec<u8>| {
        key.name.encode_key(output);
        (key.attributes.len() as u64).encode_key(output);
        for (name, value) in &key.attributes {
            name.encode_key(output);
            value.encode_key(output);
        }
        metric_series_kind_tag(key.kind).encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| {
        let name = decoder.string()?;
        let count = decoder.u64()?;
        let mut attributes = BTreeMap::new();
        for _ in 0..count {
            attributes.insert(decoder.string()?, decoder.string()?);
        }
        let kind = metric_series_kind_from_tag(decoder.u64()?)?;
        Ok(MetricSeriesKey {
            name,
            attributes,
            kind,
        })
    }
);
ordered_key!(
    MetricAttributeKey,
    |key: &MetricAttributeKey, output: &mut Vec<u8>| {
        key.key.encode_key(output);
        key.value.encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| Ok(MetricAttributeKey {
        key: decoder.string()?,
        value: decoder.string()?
    })
);
fn metric_series_kind_tag(kind: MetricSeriesKind) -> u64 {
    match kind {
        MetricSeriesKind::IntegerCount => 0,
        MetricSeriesKind::FloatCount => 1,
        MetricSeriesKind::IntegerGauge => 2,
        MetricSeriesKind::FloatGauge => 3,
        MetricSeriesKind::Histogram => 4,
    }
}

fn metric_series_kind_from_tag(tag: u64) -> CodecResult<MetricSeriesKind> {
    match tag {
        0 => Ok(MetricSeriesKind::IntegerCount),
        1 => Ok(MetricSeriesKind::FloatCount),
        2 => Ok(MetricSeriesKind::IntegerGauge),
        3 => Ok(MetricSeriesKind::FloatGauge),
        4 => Ok(MetricSeriesKind::Histogram),
        _ => Err(CodecError::invalid_key()),
    }
}

struct KeyDecoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> KeyDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn u64(&mut self) -> CodecResult<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| CodecError::invalid_key())?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn string(&mut self) -> CodecResult<String> {
        let mut output = Vec::new();
        loop {
            let byte = *self.take(1)?.first().ok_or_else(CodecError::invalid_key)?;
            if byte != 0 {
                output.push(byte);
                continue;
            }
            match *self.take(1)?.first().ok_or_else(CodecError::invalid_key)? {
                0 => break,
                0xff => output.push(0),
                _ => return Err(CodecError::invalid_key()),
            }
        }
        String::from_utf8(output).map_err(|_| CodecError::invalid_key())
    }

    fn take(&mut self, length: usize) -> CodecResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(CodecError::invalid_key)?;
        let bytes = self
            .input
            .get(self.position..end)
            .ok_or_else(CodecError::invalid_key)?;
        self.position = end;
        Ok(bytes)
    }

    fn finish(&self) -> CodecResult<()> {
        if self.position == self.input.len() {
            Ok(())
        } else {
            Err(CodecError::invalid_key())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use super::{CodecResult, OrderedKeyCodec, OrderedKeyCodecExt, ValueCodec};
    use crate::model::{
        JobPrimaryKey, MetricAttributeKey, MetricSeriesKey, MetricSeriesKind, MetricSeriesMetadata,
        OptionPrimaryKey,
    };

    fn assert_round_trip<T>(value: T) -> CodecResult<()>
    where
        T: Debug + Eq + OrderedKeyCodec,
    {
        assert_eq!(T::decode_key(&value.encoded_key())?, value);
        Ok(())
    }

    #[test]
    fn ordered_keys_round_trip_and_preserve_component_order() -> CodecResult<()> {
        let option = OptionPrimaryKey {
            key: "key\0value".to_string(),
        };
        assert_eq!(option.encoded_key(), b"key\0\xffvalue\0\0");
        assert_round_trip(option)?;
        assert_round_trip(JobPrimaryKey {
            session_id: "session".to_string(),
            job_id: 42,
        })?;
        assert_round_trip(MetricSeriesKey {
            name: "metric".to_string(),
            attributes: BTreeMap::from([
                ("cluster".to_string(), "a".to_string()),
                ("worker".to_string(), "b".to_string()),
            ]),
            kind: MetricSeriesKind::IntegerGauge,
        })?;
        assert_round_trip(MetricAttributeKey {
            key: "worker".to_string(),
            value: "a".to_string(),
        })?;
        let first = ("metric".to_string(), 1_u64).encoded_key();
        let second = ("metric".to_string(), 2_u64).encoded_key();
        assert!(first < second);
        Ok(())
    }

    #[test]
    fn serialized_values_round_trip() -> CodecResult<()> {
        let value = MetricSeriesMetadata {
            id: 3,
            name: "metric".to_string(),
            attributes: BTreeMap::from([("worker".to_string(), "a".to_string())]),
            kind: MetricSeriesKind::IntegerGauge,
        };
        let decoded = MetricSeriesMetadata::decode_value(&value.encode_value()?)?;
        assert_eq!(decoded.id, value.id);
        assert_eq!(decoded.name, value.name);
        assert_eq!(decoded.attributes, value.attributes);
        assert_eq!(decoded.kind, value.kind);
        Ok(())
    }
}
