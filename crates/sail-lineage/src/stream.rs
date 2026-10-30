use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

use crate::service::LineageRun;

/// A record batch stream that completes the lineage run when the stream ends.
///
/// The run is marked as complete when the stream is exhausted, and as failed
/// when the stream returns an error. If the stream is dropped before either happens
/// (e.g. the query is cancelled), the run is aborted.
pub struct LineageStream {
    inner: SendableRecordBatchStream,
    run: Option<LineageRun>,
}

impl LineageStream {
    pub fn new(inner: SendableRecordBatchStream, run: LineageRun) -> Self {
        Self {
            inner,
            run: Some(run),
        }
    }
}

impl Stream for LineageStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(None) => {
                if let Some(run) = self.run.take() {
                    run.complete();
                }
            }
            Poll::Ready(Some(Err(e))) => {
                if let Some(run) = self.run.take() {
                    run.fail(e.to_string());
                }
            }
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {}
        }
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for LineageStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use serde_json::Value;
    use tokio::sync::mpsc;

    use super::*;
    use crate::dataset::LineageDataset;
    use crate::options::LineageSessionOptions;
    use crate::plan::LineagePlan;
    use crate::service::LineageService;

    fn stream(items: Vec<Result<RecordBatch>>) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::empty());
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(items),
        ))
    }

    /// Returns the event type and error message of the received query run events.
    fn received(receiver: &mut mpsc::Receiver<String>) -> Vec<(String, Option<String>)> {
        let mut events = vec![];
        while let Ok(event) = receiver.try_recv() {
            let event: Value = serde_json::from_str(&event).unwrap_or_default();
            if event["job"]["facets"]["jobType"]["jobType"] == "APPLICATION" {
                continue;
            }
            events.push((
                event["eventType"].as_str().unwrap_or_default().to_string(),
                event["run"]["facets"]["errorMessage"]["message"]
                    .as_str()
                    .map(|x| x.to_string()),
            ));
        }
        events
    }

    /// The end state of a run depends on how its result stream ends.
    /// The event content is covered by the Python tests.
    #[tokio::test]
    async fn test_run_state_transitions() {
        let (sender, mut receiver) = mpsc::channel(16);
        let service = Arc::new(LineageService::with_sender("sail", sender));
        let start = || {
            let plan = LineagePlan {
                inputs: vec![LineageDataset {
                    location: Some("/data".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            service.start_run("session", &LineageSessionOptions::default(), plan)
        };
        let event = |event_type: &str, error: Option<&str>| {
            (event_type.to_string(), error.map(|x| x.to_string()))
        };

        if let Some(run) = start() {
            let mut stream = LineageStream::new(stream(vec![]), run);
            while stream.next().await.is_some() {}
        }
        assert_eq!(
            received(&mut receiver),
            vec![event("START", None), event("COMPLETE", None)]
        );

        if let Some(run) = start() {
            let error = Err(DataFusionError::Execution("boom".to_string()));
            let mut stream = LineageStream::new(stream(vec![error]), run);
            while stream.next().await.is_some() {}
        }
        assert_eq!(
            received(&mut receiver),
            vec![
                event("START", None),
                event("FAIL", Some("Execution error: boom"))
            ]
        );

        // The stream is dropped before it ends, e.g. when the query is cancelled.
        drop(start().map(|x| LineageStream::new(stream(vec![]), x)));
        assert_eq!(
            received(&mut receiver),
            vec![event("START", None), event("ABORT", None)]
        );
    }
}
