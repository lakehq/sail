mod encoding;
mod marker;
mod stream;

use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, BooleanBuilder, RecordBatch};
pub use encoding::{DecodedFlowEventStream, EncodedFlowEventStream};
pub use marker::FlowMarker;
pub use stream::{FlowEventStream, FlowEventStreamAdapter, SendableFlowEventStream};

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.streaming");
}

#[derive(Debug, Clone)]
pub enum FlowEvent {
    Data {
        batch: RecordBatch,
        retracted: Arc<BooleanArray>,
    },
    Marker(FlowMarker),
}

impl FlowEvent {
    pub fn append_only_data(batch: RecordBatch) -> Self {
        let len = batch.num_rows();
        let retracted = {
            let mut builder = BooleanBuilder::with_capacity(len);
            builder.append_n(len, false);
            Arc::new(builder.finish())
        };
        Self::Data { batch, retracted }
    }
}
