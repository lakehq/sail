//! Experimental, process-wide execution event collection.
//!
//! Set `SAIL_PROFILE_ENABLED=1` and `SAIL_PROFILE_LOCATION` to a directory,
//! `file://` directory URL, or `s3://bucket/prefix` to write gzip JSONL files.
//! Kubernetes workers need an S3 location or a directory on a shared persistent volume.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::MetricValue;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{Attribute, ObjectStore, PutOptions};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::id::TaskKey;

pub(crate) const ENABLED: &str = "SAIL_PROFILE_ENABLED";
pub(crate) const LOCATION: &str = "SAIL_PROFILE_LOCATION";

static SENDER: Mutex<Option<mpsc::UnboundedSender<Message>>> = Mutex::new(None);
static EVENTS: LazyLock<Mutex<HashMap<Arc<str>, Vec<RecordedEvent>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static DRIVERS: LazyLock<Mutex<HashMap<u64, ProfileHandle>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static HAS_DRIVERS: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
pub(crate) struct ProfileHandle {
    sender: mpsc::UnboundedSender<Message>,
    source: Arc<str>,
    location: String,
}

#[derive(Serialize)]
struct RecordedEvent {
    timestamp_us: u128,
    #[serde(flatten)]
    event: ProfileEvent,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ProfileEvent {
    Diagnostic {
        kind: &'static str,
        message: String,
    },
    JobGraph {
        job_id: u64,
        graph: String,
    },
    Rpc {
        service: &'static str,
        operation: &'static str,
        phase: &'static str,
        detail: String,
        duration_us: Option<u128>,
    },
    SystemSample {
        pid: u32,
        process_cpu_percent: Option<f64>,
        process_rss_bytes: u64,
        host_network_tx_bytes: u64,
        host_network_tx_delta_bytes: Option<u64>,
    },
    TaskStarted {
        job_id: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
    },
    TaskFinished {
        job_id: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
        status: String,
        duration_us: u128,
    },
    TaskPreparation {
        job_id: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
        wait_us: u128,
        duration_us: u128,
        success: bool,
    },
    TaskDefinition {
        job_id: u64,
        stage: usize,
        duration_us: u128,
        success: bool,
    },
    ShuffleRead {
        job_id: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
        input_stage: usize,
        batches: u64,
        rows: u64,
        poll_wait_us: u128,
        success: bool,
    },
    ShuffleWrite {
        job_id: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
        channels: usize,
        batches: u64,
        rows: u64,
        elapsed_us: u128,
        open_us: u128,
        input_wait_us: u128,
        partition_us: u128,
        sink_write_us: u128,
        finalize_us: u128,
        outcome: String,
        success: bool,
    },
    OperatorMetrics {
        job_id: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
        operators: Vec<OperatorMetricSnapshot>,
        success: bool,
    },
}

#[derive(Serialize)]
pub(crate) struct OperatorMetricSnapshot {
    pub(crate) index: usize,
    pub(crate) name: String,
    pub(crate) metrics: Vec<MetricSnapshot>,
}

#[derive(Serialize)]
pub(crate) struct MetricSnapshot {
    name: String,
    value: String,
}

impl MetricSnapshot {
    pub(crate) fn from_value(value: &MetricValue) -> Self {
        Self {
            name: value.name().to_owned(),
            value: value.to_string(),
        }
    }
}

impl ProfileEvent {
    pub(crate) fn task_started(key: &TaskKey) -> Self {
        Self::TaskStarted {
            job_id: key.job_id.into(),
            stage: key.stage,
            partition: key.partition,
            attempt: key.attempt,
        }
    }

    pub(crate) fn task_finished(key: &TaskKey, status: String, duration_us: u128) -> Self {
        Self::TaskFinished {
            job_id: key.job_id.into(),
            stage: key.stage,
            partition: key.partition,
            attempt: key.attempt,
            status,
            duration_us,
        }
    }
}

enum Message {
    Events {
        source: Arc<str>,
        batch: Vec<RecordedEvent>,
    },
    Flush {
        source: Arc<str>,
        result: oneshot::Sender<Vec<RecordedEvent>>,
    },
}

impl ProfileHandle {
    pub(crate) fn start(session_id: &str, role: &str, id: impl std::fmt::Display) -> Option<Self> {
        if !matches!(std::env::var(ENABLED).as_deref(), Ok("1" | "true")) {
            return None;
        }
        let location = match std::env::var(LOCATION) {
            Ok(value) if !value.is_empty() => value,
            _ => return None,
        };
        let mut guard = SENDER.lock().unwrap_or_else(|error| error.into_inner());
        let sender = match guard.as_ref() {
            Some(sender) if !sender.is_closed() => sender.clone(),
            _ => {
                let (sender, receiver) = mpsc::unbounded_channel();
                tokio::spawn(collect(receiver));
                *guard = Some(sender.clone());
                sender
            }
        };
        // Keep user-supplied session IDs inside the selected directory or S3 prefix.
        let session_id: String = session_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let source: Arc<str> = if role == "driver" {
            format!("{session_id}-driver")
        } else {
            format!("{session_id}-{role}-{id}")
        }
        .into();
        Some(Self {
            sender,
            source,
            location,
        })
    }

    pub(crate) fn register_driver(&self, id: u64) {
        let mut drivers = DRIVERS.lock().unwrap_or_else(|error| error.into_inner());
        drivers.insert(id, self.clone());
        HAS_DRIVERS.store(true, Ordering::Release);
    }

    pub(crate) fn unregister_driver(id: u64) {
        let mut drivers = DRIVERS.lock().unwrap_or_else(|error| error.into_inner());
        drivers.remove(&id);
        HAS_DRIVERS.store(!drivers.is_empty(), Ordering::Release);
    }

    pub(crate) fn for_driver(id: u64) -> Option<Self> {
        if !HAS_DRIVERS.load(Ordering::Acquire) {
            return None;
        }
        DRIVERS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&id)
            .cloned()
    }

    pub(crate) fn diagnostic(&self, kind: &'static str, message: impl FnOnce() -> String) {
        self.record(ProfileEvent::Diagnostic {
            kind,
            message: message(),
        });
    }

    pub(crate) fn rpc(
        &self,
        service: &'static str,
        operation: &'static str,
        phase: &'static str,
        detail: impl FnOnce() -> String,
        duration_us: Option<u128>,
    ) {
        self.record(ProfileEvent::Rpc {
            service,
            operation,
            phase,
            detail: detail(),
            duration_us,
        });
    }

    pub(crate) fn record(&self, event: ProfileEvent) {
        self.record_batch([(now_us(), event)]);
    }

    pub(crate) fn record_batch(&self, events: impl IntoIterator<Item = (u128, ProfileEvent)>) {
        let events = events
            .into_iter()
            .map(|(timestamp_us, event)| RecordedEvent {
                timestamp_us,
                event,
            })
            .collect();
        let _ = self.sender.send(Message::Events {
            source: self.source.clone(),
            batch: events,
        });
    }

    pub(crate) async fn finish(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (result, receiver) = oneshot::channel();
        self.sender
            .send(Message::Flush {
                source: self.source.clone(),
                result,
            })
            .map_err(|_| "profile collector stopped")?;
        let events = receiver.await?;
        let bytes = encode(events).await?;
        let filename = format!("{}.jsonl.gz", self.source);
        if self.location.starts_with("s3://") {
            let url = Url::parse(&self.location)?;
            let store = sail_object_store::get_s3_object_store(&url).await?;
            let path = Path::from_url_path(url.path())?.join(filename.as_str());
            let mut options = PutOptions::default();
            options
                .attributes
                .insert(Attribute::ContentType, "application/x-ndjson".into());
            options
                .attributes
                .insert(Attribute::ContentEncoding, "gzip".into());
            store.put_opts(&path, bytes.into(), options).await?;
        } else {
            let directory = if self.location.starts_with("file://") {
                Url::parse(&self.location)?
                    .to_file_path()
                    .map_err(|_| "invalid file URL")?
            } else {
                PathBuf::from(&self.location)
            };
            tokio::fs::create_dir_all(&directory).await?;
            tokio::fs::write(directory.join(filename), bytes).await?;
        }
        Ok(())
    }
}

async fn collect(mut receiver: mpsc::UnboundedReceiver<Message>) {
    while let Some(message) = receiver.recv().await {
        let mut events = EVENTS.lock().unwrap_or_else(|error| error.into_inner());
        match message {
            Message::Events { source, batch } => events.entry(source).or_default().extend(batch),
            Message::Flush { source, result } => {
                // The flush operation is ordered after all event messages from the actor.
                let _ = result.send(events.remove(&source).unwrap_or_default());
            }
        }
    }
}

async fn encode(
    events: Vec<RecordedEvent>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut jsonl = Vec::new();
    for event in events {
        serde_json::to_writer(&mut jsonl, &event)?;
        jsonl.push(b'\n');
    }
    let input = stream::once(async { Ok::<_, DataFusionError>(Bytes::from(jsonl)) }).boxed();
    let compressed = FileCompressionType::GZIP.convert_to_compress_stream(input)?;
    let bytes = compressed
        .try_fold(Vec::new(), |mut output, chunk| async move {
            output.extend_from_slice(&chunk);
            Ok(output)
        })
        .await?;
    Ok(bytes)
}

pub(crate) fn now_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::metrics::{Count, PruningMetrics, RatioMetrics, Time};

    use super::*;

    #[test]
    fn operator_metrics_serialize_as_records() -> serde_json::Result<()> {
        let rows = Count::new();
        rows.add(7);
        let time = Time::new();
        time.add_duration(std::time::Duration::from_nanos(12));
        let pruning = PruningMetrics::new();
        pruning.add_pruned(3);
        pruning.add_matched(4);
        pruning.add_fully_matched(2);
        let ratio = RatioMetrics::new();
        ratio.add_part(2);
        ratio.add_total(5);
        let metrics = [
            MetricValue::OutputRows(rows),
            MetricValue::ElapsedCompute(time),
            MetricValue::PruningMetrics {
                name: "pruning".into(),
                pruning_metrics: pruning,
            },
            MetricValue::Ratio {
                name: "hit_rate".into(),
                ratio_metrics: ratio,
            },
        ]
        .iter()
        .map(MetricSnapshot::from_value)
        .collect();
        let event = ProfileEvent::OperatorMetrics {
            job_id: 1,
            stage: 2,
            partition: 3,
            attempt: 4,
            operators: vec![OperatorMetricSnapshot {
                index: 5,
                name: "TestExec".into(),
                metrics,
            }],
            success: true,
        };
        let json = serde_json::to_value(event)?;
        assert_eq!(json["operators"][0]["index"], 5);
        assert_eq!(json["operators"][0]["name"], "TestExec");
        assert_eq!(
            json["operators"][0]["metrics"][0],
            serde_json::json!({
                "name": "output_rows", "value": "7"
            })
        );
        assert_eq!(
            json["operators"][0]["metrics"][1]["name"],
            "elapsed_compute"
        );
        assert!(json["operators"][0]["metrics"][1]["value"].is_string());
        assert_eq!(json["operators"][0]["metrics"][2]["name"], "pruning");
        assert!(json["operators"][0]["metrics"][2]["value"].is_string());
        assert_eq!(json["operators"][0]["metrics"][3]["name"], "hit_rate");
        assert!(json["operators"][0]["metrics"][3]["value"].is_string());
        Ok(())
    }

    #[tokio::test]
    async fn flush_keeps_profiles_separate() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(collect(receiver));
        let directory = std::env::temp_dir().join(format!(
            "sail-profile-test-{}-{}",
            std::process::id(),
            now_us()
        ));
        let location = directory.to_string_lossy().into_owned();
        let first = ProfileHandle {
            sender: sender.clone(),
            source: "test-driver-1".into(),
            location: location.clone(),
        };
        let second = ProfileHandle {
            sender,
            source: "test-worker-2".into(),
            location,
        };
        first.record(ProfileEvent::TaskStarted {
            job_id: 1,
            stage: 2,
            partition: 3,
            attempt: 4,
        });
        second.record(ProfileEvent::TaskStarted {
            job_id: 5,
            stage: 6,
            partition: 7,
            attempt: 8,
        });
        first.finish().await?;
        second.finish().await?;
        for (filename, job_id) in [("test-driver-1.jsonl.gz", 1), ("test-worker-2.jsonl.gz", 5)] {
            let bytes = tokio::fs::read(directory.join(filename)).await?;
            let input =
                stream::once(async { Ok::<_, DataFusionError>(Bytes::from(bytes)) }).boxed();
            let decoded = FileCompressionType::GZIP.convert_stream(input)?;
            let jsonl = decoded
                .try_fold(Vec::new(), |mut output, chunk| async move {
                    output.extend_from_slice(&chunk);
                    Ok(output)
                })
                .await?;
            let jsonl = String::from_utf8(jsonl)?;
            let lines = jsonl.lines().collect::<Vec<_>>();
            assert_eq!(lines.len(), 1);
            let event: serde_json::Value = serde_json::from_str(lines[0])?;
            assert_eq!(event["job_id"], job_id);
            assert_eq!(event["type"], "task_started");
            assert!(event.get("source").is_none());
        }
        tokio::fs::remove_dir_all(directory).await?;
        Ok(())
    }
}
