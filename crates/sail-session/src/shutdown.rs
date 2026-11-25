// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;

use datafusion::common::runtime::SpawnedTask;
use futures::Stream;
use sail_common_datafusion::session::job::QueryCancellation;
use tonic::Status;

fn query_cancelled() -> Status {
    Status::cancelled("query cancelled because the server is shutting down")
}

/// Run RPC work in a separate task so the RPC can observe shutdown independently.
pub async fn run<T: Send + 'static>(
    cancellation: &QueryCancellation,
    rpc: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> Result<T, Status> {
    let run = async move {
        SpawnedTask::spawn(rpc)
            .await
            .map_err(|e| Status::internal(format!("query task failed: {e}")))?
    };
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(query_cancelled()),
        result = run => result,
    }
}

/// Cancel an outgoing RPC response stream when the server shuts down.
pub fn cancel_stream<T, S>(
    cancellation: &Arc<QueryCancellation>,
    stream: S,
) -> impl Stream<Item = Result<T, Status>> + use<T, S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    Arc::clone(cancellation).cancel_stream(stream, || Err(query_cancelled()))
}
