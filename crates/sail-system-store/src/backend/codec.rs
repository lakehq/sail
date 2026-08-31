//! Shared codecs for ordered on-disk system store keys and serialized values.
//!
//! All disk KV backends use these codecs so that durable key layouts remain independent of the
//! backend implementation. Memory storage keeps typed values directly and does not need them.

use std::collections::BTreeMap;

use sail_common_datafusion::system::predicate::TimestampMicros;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::model::{
    JobPrimaryKey, MetricAttributeKey, MetricPointKey, MetricPointOrdinalKey, MetricSeriesKey,
    OptionPrimaryKey, SessionPrimaryKey, StagePrimaryKey, TaskPrimaryKey, WorkerPrimaryKey,
};
use crate::{SystemStoreError, SystemStoreResult};

/// The fixed durable key for the next metric series ID metadata entry.
pub(crate) const NEXT_METRIC_SERIES_ID_KEY: &[u8] = b"next_metric_series_id";

/// Codec for keys whose byte ordering must preserve their Rust ordering.
pub trait OrderedKeyCodec: Sized {
    fn encode_key(&self, output: &mut Vec<u8>);
    fn decode_key(input: &[u8]) -> SystemStoreResult<Self>;
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
    fn encode_value(&self) -> SystemStoreResult<Vec<u8>>;
    fn decode_value(input: &[u8]) -> SystemStoreResult<Self>;
}

impl<T> ValueCodec for T
where
    T: DeserializeOwned + Serialize,
{
    fn encode_value(&self) -> SystemStoreResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|error| invalid_value(format!("failed to encode system store value: {error}")))
    }

    fn decode_value(input: &[u8]) -> SystemStoreResult<Self> {
        serde_json::from_slice(input)
            .map_err(|error| invalid_value(format!("failed to decode system store value: {error}")))
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

    fn decode_key(input: &[u8]) -> SystemStoreResult<Self> {
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

    fn decode_key(input: &[u8]) -> SystemStoreResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let value = decoder.u64()?;
        decoder.finish()?;
        Ok(value)
    }
}

impl OrderedKeyCodec for (String, u64) {
    fn encode_key(&self, output: &mut Vec<u8>) {
        self.0.encode_key(output);
        self.1.encode_key(output);
    }

    fn decode_key(input: &[u8]) -> SystemStoreResult<Self> {
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

    fn decode_key(input: &[u8]) -> SystemStoreResult<Self> {
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

impl OrderedKeyCodec for (u64, MetricPointKey) {
    fn encode_key(&self, output: &mut Vec<u8>) {
        self.0.encode_key(output);
        self.1.encode_key(output);
    }

    fn decode_key(input: &[u8]) -> SystemStoreResult<Self> {
        let mut decoder = KeyDecoder::new(input);
        let series = decoder.u64()?;
        let timestamp = decoder.u64()? ^ (1_u64 << 63);
        let point = MetricPointKey {
            timestamp: TimestampMicros(timestamp as i64),
            ordinal: decoder.u64()?,
        };
        decoder.finish()?;
        Ok((series, point))
    }
}

macro_rules! ordered_key {
    ($type:ty, $encode:expr, $decode:expr) => {
        impl OrderedKeyCodec for $type {
            fn encode_key(&self, output: &mut Vec<u8>) {
                $encode(self, output)
            }

            fn decode_key(input: &[u8]) -> SystemStoreResult<Self> {
                let mut decoder = KeyDecoder::new(input);
                let decode: fn(&mut KeyDecoder<'_>) -> SystemStoreResult<$type> = $decode;
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
    },
    |decoder: &mut KeyDecoder<'_>| {
        let name = decoder.string()?;
        let count = decoder.u64()?;
        let mut attributes = BTreeMap::new();
        for _ in 0..count {
            attributes.insert(decoder.string()?, decoder.string()?);
        }
        Ok(MetricSeriesKey { name, attributes })
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
ordered_key!(
    MetricPointKey,
    |key: &MetricPointKey, output: &mut Vec<u8>| {
        ((key.timestamp.0 as u64) ^ (1_u64 << 63)).encode_key(output);
        key.ordinal.encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| {
        let timestamp = decoder.u64()? ^ (1_u64 << 63);
        Ok(MetricPointKey {
            timestamp: TimestampMicros(timestamp as i64),
            ordinal: decoder.u64()?,
        })
    }
);
ordered_key!(
    MetricPointOrdinalKey,
    |key: &MetricPointOrdinalKey, output: &mut Vec<u8>| {
        key.series.encode_key(output);
        ((key.timestamp.0 as u64) ^ (1_u64 << 63)).encode_key(output);
    },
    |decoder: &mut KeyDecoder<'_>| {
        let series = decoder.u64()?;
        let timestamp = decoder.u64()? ^ (1_u64 << 63);
        Ok(MetricPointOrdinalKey {
            series,
            timestamp: TimestampMicros(timestamp as i64),
        })
    }
);

struct KeyDecoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> KeyDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn u64(&mut self) -> SystemStoreResult<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().map_err(|_| invalid_key())?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn string(&mut self) -> SystemStoreResult<String> {
        let mut output = Vec::new();
        loop {
            let byte = *self.take(1)?.first().ok_or_else(invalid_key)?;
            if byte != 0 {
                output.push(byte);
                continue;
            }
            match *self.take(1)?.first().ok_or_else(invalid_key)? {
                0 => break,
                0xff => output.push(0),
                _ => return Err(invalid_key()),
            }
        }
        String::from_utf8(output).map_err(|_| invalid_key())
    }

    fn take(&mut self, length: usize) -> SystemStoreResult<&'a [u8]> {
        let end = self.position.checked_add(length).ok_or_else(invalid_key)?;
        let bytes = self.input.get(self.position..end).ok_or_else(invalid_key)?;
        self.position = end;
        Ok(bytes)
    }

    fn finish(&self) -> SystemStoreResult<()> {
        if self.position == self.input.len() {
            Ok(())
        } else {
            Err(invalid_key())
        }
    }
}

fn invalid_key() -> SystemStoreError {
    SystemStoreError::InvalidKey
}

fn invalid_value(message: String) -> SystemStoreError {
    SystemStoreError::invalid_value(message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use sail_common_datafusion::system::predicate::TimestampMicros;

    use super::{OrderedKeyCodec, OrderedKeyCodecExt, ValueCodec};
    use crate::SystemStoreResult;
    use crate::model::{
        JobPrimaryKey, MetricAttributeKey, MetricPointKey, MetricSeriesKey, MetricSeriesMetadata,
        OptionPrimaryKey,
    };

    fn assert_round_trip<T>(value: T) -> SystemStoreResult<()>
    where
        T: Debug + Eq + OrderedKeyCodec,
    {
        assert_eq!(T::decode_key(&value.encoded_key())?, value);
        Ok(())
    }

    #[test]
    fn ordered_keys_round_trip_and_preserve_component_order() -> SystemStoreResult<()> {
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
        })?;
        assert_round_trip(MetricAttributeKey {
            key: "worker".to_string(),
            value: "a".to_string(),
        })?;
        assert_round_trip(MetricPointKey {
            timestamp: TimestampMicros(-1),
            ordinal: 7,
        })?;

        let first = ("metric".to_string(), 1_u64).encoded_key();
        let second = ("metric".to_string(), 2_u64).encoded_key();
        assert!(first < second);
        Ok(())
    }

    #[test]
    fn metric_point_storage_key_round_trips() -> SystemStoreResult<()> {
        let key = (
            5_u64,
            MetricPointKey {
                timestamp: TimestampMicros(-1),
                ordinal: 7,
            },
        );
        assert_eq!(
            key.encoded_key(),
            [
                0, 0, 0, 0, 0, 0, 0, 5, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0,
                0, 7,
            ]
        );
        assert_eq!(
            <(u64, MetricPointKey)>::decode_key(&key.encoded_key())?,
            key
        );
        Ok(())
    }

    #[test]
    fn serialized_values_round_trip() -> SystemStoreResult<()> {
        let value = MetricSeriesMetadata {
            id: 3,
            name: "metric".to_string(),
            attributes: BTreeMap::from([("worker".to_string(), "a".to_string())]),
        };
        let decoded = MetricSeriesMetadata::decode_value(&value.encode_value()?)?;
        assert_eq!(decoded.id, value.id);
        assert_eq!(decoded.name, value.name);
        assert_eq!(decoded.attributes, value.attributes);
        Ok(())
    }
}
