use std::collections::HashMap;
use std::time::Duration;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{PlanType, StringifiedPlan};
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::{oneshot, watch};

use crate::error::{SparkError, SparkResult, SparkThrowable};
use crate::spark::connect;

pub struct StreamingQuery {
    name: String,
    info: Vec<StringifiedPlan>,
    error: watch::Receiver<Option<SparkThrowable>>,
    stopped: watch::Receiver<bool>,
    signal: Option<oneshot::Sender<()>>,
    awaitable: bool,
}

impl StreamingQuery {
    pub fn new(
        name: String,
        info: Vec<StringifiedPlan>,
        stream: SendableRecordBatchStream,
    ) -> Self {
        let (signal_tx, signal_rx) = oneshot::channel();
        let (error_tx, error_rx) = watch::channel(None);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        tokio::spawn(async move {
            Self::run(signal_rx, error_tx, stopped_tx, stream).await;
        });
        Self {
            name,
            info,
            error: error_rx,
            stopped: stopped_rx,
            signal: Some(signal_tx),
            awaitable: true,
        }
    }

    pub fn status(&self) -> StreamingQueryStatus {
        let stopped = *self.stopped.borrow();
        let default_message = if stopped {
            "The query is not active"
        } else {
            "The query is active"
        };
        StreamingQueryStatus {
            name: self.name.clone(),
            message: self
                .error
                .borrow()
                .as_ref()
                .map(|x| x.message().to_string())
                .unwrap_or_else(|| default_message.to_string()),
            is_active: !stopped,
        }
    }

    async fn run(
        signal: oneshot::Receiver<()>,
        error: watch::Sender<Option<SparkThrowable>>,
        stopped: watch::Sender<bool>,
        mut stream: SendableRecordBatchStream,
    ) {
        let task = async move {
            while let Some(x) = stream.next().await {
                match x {
                    Ok(_) => {}
                    Err(e) => {
                        let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                        let _ = error.send(Some(cause.into()));
                    }
                }
            }
        };
        tokio::select! {
            _ = signal => {}
            _ = task => {}
        }
        let _ = stopped.send(true);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamingQueryId {
    pub query_id: String,
    pub run_id: String,
}

impl From<connect::StreamingQueryInstanceId> for StreamingQueryId {
    fn from(value: connect::StreamingQueryInstanceId) -> Self {
        Self {
            query_id: value.id,
            run_id: value.run_id,
        }
    }
}

impl From<StreamingQueryId> for connect::StreamingQueryInstanceId {
    fn from(value: StreamingQueryId) -> Self {
        Self {
            id: value.query_id,
            run_id: value.run_id,
        }
    }
}

pub struct StreamingQueryManager {
    queries: HashMap<StreamingQueryId, StreamingQuery>,
}

impl StreamingQueryManager {
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
        }
    }

    pub fn add_query(&mut self, id: StreamingQueryId, query: StreamingQuery) {
        self.queries.insert(id, query);
    }

    pub fn stop_query(&mut self, id: &StreamingQueryId) -> SparkResult<()> {
        let Some(query) = self.queries.get_mut(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        if let Some(signal) = query.signal.take() {
            let _ = signal.send(());
        };
        Ok(())
    }

    pub fn explain_query(&self, id: &StreamingQueryId, extended: bool) -> SparkResult<String> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        let mut result = String::new();
        let mut write = |kind: &'static str, t: &PlanType| {
            for item in query.info.iter() {
                if &item.plan_type == t {
                    result.push_str("== ");
                    result.push_str(kind);
                    result.push_str(" ==\n");
                    result.push_str(item.plan.trim_end_matches('\n'));
                    result.push_str("\n\n");
                }
            }
        };
        if extended {
            write("Initial Logical Plan", &PlanType::InitialLogicalPlan);
            write("Final Logical Plan", &PlanType::FinalLogicalPlan);
        }
        write("Final Physical Plan", &PlanType::FinalPhysicalPlan);
        Ok(result)
    }

    pub fn get_query_status(&self, id: &StreamingQueryId) -> SparkResult<StreamingQueryStatus> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.status())
    }

    pub fn get_query_error(&self, id: &StreamingQueryId) -> SparkResult<Option<SparkThrowable>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.error.borrow().clone())
    }

    pub fn find_query_by_query_id(
        &self,
        query_id: &str,
    ) -> SparkResult<(StreamingQueryId, StreamingQueryStatus)> {
        for (id, query) in self.queries.iter() {
            if id.query_id == query_id {
                return Ok((id.clone(), query.status()));
            }
        }
        Err(SparkError::invalid(format!(
            "streaming query not found by query id: {query_id}"
        )))
    }

    pub fn list_active_queries(&self) -> Vec<(StreamingQueryId, StreamingQueryStatus)> {
        self.queries
            .iter()
            .filter_map(|(id, query)| {
                if !*query.stopped.borrow() {
                    Some((id.clone(), query.status()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn await_query(
        &self,
        id: &StreamingQueryId,
    ) -> SparkResult<Option<StreamingQueryAwaitHandle>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        if !query.awaitable {
            Ok(None)
        } else {
            let stopped = query.stopped.clone();
            Ok(Some(StreamingQueryAwaitHandle { stopped }))
        }
    }

    pub fn await_queries(&self) -> SparkResult<StreamingQueryAwaitHandleSet> {
        let mut handles = Vec::new();
        for query in self.queries.values() {
            if query.awaitable {
                handles.push(StreamingQueryAwaitHandle {
                    stopped: query.stopped.clone(),
                });
            }
        }
        Ok(StreamingQueryAwaitHandleSet::new(handles))
    }

    pub fn reset_stopped_queries(&mut self) {
        for query in self.queries.values_mut() {
            if *query.stopped.borrow() {
                query.awaitable = false;
            }
        }
    }
}

pub struct StreamingQueryStatus {
    pub name: String,
    pub message: String,
    pub is_active: bool,
}

pub fn timeout_millis(value: i64) -> SparkResult<Duration> {
    if value < 0 {
        return Err(SparkError::invalid(format!(
            "invalid timeout value: {value}"
        )));
    }
    Ok(Duration::from_millis(value as u64))
}

pub struct StreamingQueryAwaitHandle {
    stopped: watch::Receiver<bool>,
}

impl StreamingQueryAwaitHandle {
    async fn wait(mut self) -> () {
        // We ignore the receiver error since the streaming query must have been
        // terminated in that case.
        let _ = self.stopped.wait_for(|x| *x).await;
    }

    pub async fn terminated(self, timeout: Option<Duration>) -> SparkResult<bool> {
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, self.wait()).await {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            self.wait().await;
            Ok(true)
        }
    }
}

pub struct StreamingQueryAwaitHandleSet {
    handles: Vec<StreamingQueryAwaitHandle>,
}

impl StreamingQueryAwaitHandleSet {
    pub fn new(handles: Vec<StreamingQueryAwaitHandle>) -> Self {
        Self { handles }
    }

    pub async fn any_terminated(self, timeout: Option<Duration>) -> SparkResult<bool> {
        let mut join_set = tokio::task::JoinSet::new();
        for handle in self.handles {
            join_set.spawn(handle.wait());
        }
        let next = async move {
            match join_set.join_next().await {
                Some(Ok(())) => Ok(true),
                Some(Err(e)) => Err(SparkError::internal(format!(
                    "failed to await any termination for streaming queries: {e}"
                ))),
                None => Ok(false),
            }
        };
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, next)
                .await
                .unwrap_or_else(|_| Ok(false))
        } else {
            next.await
        }
    }
}
