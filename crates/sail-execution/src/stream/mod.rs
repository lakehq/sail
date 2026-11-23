// SPDX-License-Identifier: Apache-2.0

pub(crate) mod accessor;
pub(crate) mod celeborn;
pub mod error;
pub(crate) mod local;
pub mod merge;
pub mod reader;
pub(crate) mod service;
pub(crate) mod storage;
pub mod writer;

use datafusion::common::exec_err;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{TryStreamExt, stream};
use tokio_util::sync::CancellationToken;

#[expect(clippy::allow_attributes)]
pub mod r#gen {
    tonic::include_proto!("sail.stream");
}

pub(crate) fn cancel_job_stream_on_stop(
    input: SendableRecordBatchStream,
    stopped: CancellationToken,
) -> SendableRecordBatchStream {
    let schema = input.schema();
    let stream = stream::try_unfold((input, stopped), |(mut input, stopped)| async move {
        tokio::select! {
            biased;
            () = stopped.cancelled() => exec_err!("query cancelled because the job runner stopped"),
            batch = input.try_next() => Ok(batch?.map(|batch| (batch, (input, stopped)))),
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
