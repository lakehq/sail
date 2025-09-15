pub mod encoding;
pub mod marker;
pub mod schema;
pub mod stream;

use datafusion::arrow::array::{BooleanArray, BooleanBuilder, RecordBatch};

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.streaming");
}

/// An event in a streaming data flow.
#[derive(Debug, Clone)]
pub enum FlowEvent {
    /// A retractable batch of data.
    Data {
        /// The data batch.
        batch: RecordBatch,
        /// A boolean array indicating whether each row is retracted.
        /// The array has the same length as the data batch.
        retracted: BooleanArray,
    },
    /// A marker injected in the data flow.
    Marker(marker::FlowMarker),
}

impl FlowEvent {
    /// Create a new retractable data batch from append-only data.
    pub fn append_only_data(batch: RecordBatch) -> Self {
        let len = batch.num_rows();
        let retracted = {
            let mut builder = BooleanBuilder::with_capacity(len);
            builder.append_n(len, false);
            builder.finish()
        };
        Self::Data { batch, retracted }
    }
}
