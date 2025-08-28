mod encoding;
mod marker;
mod stream;

use datafusion::arrow::array::RecordBatch;
pub use encoding::{DecodedFlowEventStream, EncodedFlowEventStream};
pub use marker::FlowMarker;
pub use stream::{FlowEventStream, FlowEventStreamAdapter, SendableFlowEventStream};

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.stream");
}

#[derive(Debug, Clone)]
pub enum FlowEvent {
    Data(RecordBatch),
    Marker(FlowMarker),
}
