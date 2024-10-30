use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::oneshot;

use crate::error::ExecutionResult;

pub(super) enum JobOutput {
    Pending {
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    Running {},
    Stopped,
}
