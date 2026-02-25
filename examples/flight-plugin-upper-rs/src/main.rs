//! Minimal Arrow Flight plugin server that provides an `upper_flight` UDF.
//!
//! Usage:
//!     cd examples/flight-plugin-upper-rs
//!     cargo run
//!
//! Then start Sail with:
//!     cargo run -p sail-cli -- spark server --plugin-endpoint grpc://localhost:50052
//!
//! Test with PySpark:
//!     spark.sql("SELECT upper_flight('hello world')").show()

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

struct UpperPluginServer;

#[tonic::async_trait]
impl FlightService for UpperPluginServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "udf.upper_flight".to_string(),
            description: r#"{"volatility":"immutable"}"#.to_string(),
        })];
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        if action.r#type.starts_with("return_type.") {
            // Return Utf8 for any input
            let schema = Schema::new(vec![Field::new("result", DataType::Utf8, true)]);
            let ipc_bytes = serialize_schema_to_ipc(&schema)
                .map_err(|e| Status::internal(format!("schema serialization error: {e}")))?;
            let result = arrow_flight::Result {
                body: ipc_bytes.into(),
            };
            return Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))));
        }
        if action.r#type == "ping" {
            return Ok(Response::new(Box::pin(stream::empty())));
        }
        Err(Status::unimplemented(format!(
            "unknown action: {}",
            action.r#type
        )))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let input_stream = request.into_inner();
        let flight_stream = FlightRecordBatchStream::new_from_flight_data(
            input_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );
        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("failed to read input batches: {e}")))?;

        if batches.is_empty() {
            return Err(Status::invalid_argument("no input batches received"));
        }

        // Process: convert first column to uppercase
        let input = &batches[0];
        let col = input.column(0);
        let string_array: &StringArray = col.as_string();
        let result: StringArray = string_array
            .iter()
            .map(|opt| opt.map(|s| s.to_uppercase()))
            .collect();
        let result_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Utf8,
            true,
        )]));
        let result_batch =
            RecordBatch::try_new(result_schema, vec![Arc::new(result) as ArrayRef])
                .map_err(|e| Status::internal(format!("failed to create result batch: {e}")))?;

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream::once(async { Ok(result_batch) }))
            .map(|item| match item {
                Ok(data) => Ok(data),
                Err(e) => Err(Status::internal(format!("encoding error: {e}"))),
            });

        Ok(Response::new(Box::pin(out_stream)))
    }
}

fn serialize_schema_to_ipc(schema: &Schema) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)?;
    writer.finish()?;
    Ok(buf)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = "0.0.0.0:50052".parse()?;
    println!("Flight plugin server listening on {addr}");
    println!("Providing UDF: upper_flight(string) -> string");
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(UpperPluginServer))
        .serve(addr)
        .await?;
    Ok(())
}
