use chrono::{DateTime, Utc};
use datafusion_common::{plan_datafusion_err, plan_err, Result};
use prost::Message;

use crate::streaming::event::gen;

/// A marker injected in a streaming data flow for various purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowMarker {
    /// A latency tracking marker emitted by each data source.
    /// The latency can be measured by downstream operators by comparing
    /// the current time with the timestamp in the marker.
    LatencyTracker {
        source: String,
        id: u64,
        timestamp: DateTime<Utc>,
    },
    /// A watermark marker emitted by a time-based source to indicate
    /// that no data with a timestamp older than the watermark timestamp
    /// will be emitted in the future.
    /// Downstream operators can use the watermark to discard state
    /// that is no longer needed for processing late data.
    Watermark {
        source: String,
        timestamp: DateTime<Utc>,
    },
    /// A checkpoint marker emitted by each data source to start a new
    /// checkpoint.
    Checkpoint { id: u64 },
    /// An indicator that the flow event stream will contain no more data events.
    /// A bounded source cannot close the stream to indicate end of data, since
    /// it is supposed to still wait for another checkpoint marker before finishing.
    /// When a sink receives this marker from all its input, it should wait for
    /// the next checkpoint marker and then the job can finish.
    EndOfData,
}

impl FlowMarker {
    pub fn encode(self) -> Result<Vec<u8>> {
        let kind = match self {
            FlowMarker::LatencyTracker {
                source,
                id,
                timestamp,
            } => gen::flow_marker::Kind::LatencyTracker(gen::LatencyTracker {
                source,
                id,
                timestamp_secs: timestamp.timestamp(),
                timestamp_nanos: timestamp.timestamp_subsec_nanos(),
            }),
            FlowMarker::Watermark { source, timestamp } => {
                gen::flow_marker::Kind::Watermark(gen::Watermark {
                    source,
                    timestamp_secs: timestamp.timestamp(),
                    timestamp_nanos: timestamp.timestamp_subsec_nanos(),
                })
            }
            FlowMarker::Checkpoint { id } => {
                gen::flow_marker::Kind::Checkpoint(gen::Checkpoint { id })
            }
            FlowMarker::EndOfData => gen::flow_marker::Kind::EndOfData(gen::EndOfData {}),
        };
        let message = gen::FlowMarker { kind: Some(kind) };
        Ok(message.encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let message = gen::FlowMarker::decode(bytes)
            .map_err(|e| plan_datafusion_err!("failed to decode marker: {e}"))?;
        match message.kind {
            Some(gen::flow_marker::Kind::LatencyTracker(gen::LatencyTracker {
                source,
                id,
                timestamp_secs,
                timestamp_nanos,
            })) => Ok(FlowMarker::LatencyTracker {
                source,
                id,
                timestamp: <DateTime<Utc>>::from_timestamp(timestamp_secs, timestamp_nanos)
                    .ok_or_else(|| plan_datafusion_err!("invalid latency tracker timestamp"))?,
            }),
            Some(gen::flow_marker::Kind::Watermark(gen::Watermark {
                source,
                timestamp_secs,
                timestamp_nanos,
            })) => Ok(FlowMarker::Watermark {
                source,
                timestamp: <DateTime<Utc>>::from_timestamp(timestamp_secs, timestamp_nanos)
                    .ok_or_else(|| plan_datafusion_err!("invalid watermark timestamp"))?,
            }),
            Some(gen::flow_marker::Kind::Checkpoint(gen::Checkpoint { id })) => {
                Ok(FlowMarker::Checkpoint { id })
            }
            Some(gen::flow_marker::Kind::EndOfData(gen::EndOfData {})) => Ok(FlowMarker::EndOfData),
            None => plan_err!("missing marker kind"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_latency_tracker() -> Result<()> {
        let marker = FlowMarker::LatencyTracker {
            source: "s".to_string(),
            id: 42,
            timestamp: Utc::now(),
        };
        let encoded = marker.clone().encode()?;
        let decoded = FlowMarker::decode(&encoded)?;
        assert_eq!(marker, decoded);
        Ok(())
    }

    #[test]
    fn test_encode_decode_watermark() -> Result<()> {
        let marker = FlowMarker::Watermark {
            source: "s".to_string(),
            timestamp: Utc::now(),
        };
        let encoded = marker.clone().encode()?;
        let decoded = FlowMarker::decode(&encoded)?;
        assert_eq!(marker, decoded);
        Ok(())
    }

    #[test]
    fn test_encode_decode_checkpoint() -> Result<()> {
        let marker = FlowMarker::Checkpoint { id: 42 };
        let encoded = marker.clone().encode()?;
        let decoded = FlowMarker::decode(&encoded)?;
        assert_eq!(marker, decoded);
        Ok(())
    }

    #[test]
    fn test_encode_decode_end_of_data() -> Result<()> {
        let marker = FlowMarker::EndOfData;
        let encoded = marker.clone().encode()?;
        let decoded = FlowMarker::decode(&encoded)?;
        assert_eq!(marker, decoded);
        Ok(())
    }
}
