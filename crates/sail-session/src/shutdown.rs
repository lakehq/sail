// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;

use datafusion::common::runtime::SpawnedTask;
use futures::Stream;
use sail_common_datafusion::session::job::QueryCancellation;
use tokio::runtime::Handle;
use tonic::Status;

fn query_cancelled() -> Status {
    Status::cancelled("query cancelled because the server is shutting down")
}

/// Run query work on compute while the RPC observes shutdown on the primary runtime.
pub(crate) async fn run<T: Send + 'static>(
    compute: &Handle,
    cancellation: &QueryCancellation,
    rpc: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> Result<T, Status> {
    let run = async move {
        let query = {
            let _guard = compute.enter();
            SpawnedTask::spawn(rpc)
        };
        query
            .await
            .map_err(|e| Status::internal(format!("query task failed: {e}")))?
    };
    until_shutdown(cancellation, run).await
}

/// Keep control RPCs responsive to shutdown on the server runtime.
pub async fn until_shutdown<T>(
    cancellation: &QueryCancellation,
    rpc: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(query_cancelled()),
        result = rpc => result,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::runtime::Builder;
    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn shutdown_interrupts_rpc_during_a_non_yielding_query()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary = Builder::new_multi_thread().worker_threads(1).build()?;
        let compute = Builder::new_multi_thread().worker_threads(1).build()?;
        let compute_handle = compute.handle().clone();
        let (started, ready) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let cancellation = Arc::new(QueryCancellation::default());
        let rpc = {
            let cancellation = cancellation.clone();
            primary.spawn(async move {
                run(&compute_handle, &cancellation, async move {
                    let _ = started.send(Handle::current().id());
                    blocked
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|error| Status::internal(error.to_string()))?;
                    Ok(())
                })
                .await
            })
        };

        assert_eq!(primary.block_on(ready)?, compute.handle().id());
        primary.block_on(primary.spawn(async move { cancellation.cancel() }))?;
        let result = primary.block_on(rpc)?;
        release.send(())?;
        assert!(matches!(result, Err(status) if status.code() == tonic::Code::Cancelled));
        Ok(())
    }
}
