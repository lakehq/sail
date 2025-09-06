use chrono::{DateTime, Utc};
use datafusion_common::{internal_datafusion_err, plan_datafusion_err, plan_err, Result};
use prost::bytes::BytesMut;
use prost::Message;

use crate::event::gen;

#[derive(Debug, Clone)]
pub enum FlowMarker {
    LatencyTracker {
        source: String,
        id: u64,
        timestamp: DateTime<Utc>,
    },
    Watermark {
        source: String,
        timestamp: DateTime<Utc>,
    },
    Checkpoint {
        id: u64,
    },
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
        };
        let message = gen::FlowMarker { kind: Some(kind) };
        let mut buffer = BytesMut::new();
        message
            .encode(&mut buffer)
            .map_err(|e| internal_datafusion_err!("failed to encode marker: {e}"))?;
        Ok(buffer.freeze().into())
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
            None => plan_err!("missing marker kind"),
        }
    }
}
