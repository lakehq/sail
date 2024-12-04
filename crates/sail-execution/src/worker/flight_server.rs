use std::io::Cursor;
use std::pin::Pin;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::TryStreamExt;
use log::debug;
use prost::Message;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;
use tonic::codegen::tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::ExecutionError;
use crate::worker::gen::TaskStreamTicket;
use crate::worker::WorkerActor;

pub struct WorkerFlightServer {
    handle: ActorHandle<WorkerActor>,
}

impl WorkerFlightServer {
    pub fn new(handle: ActorHandle<WorkerActor>) -> Self {
        Self { handle }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for WorkerFlightServer {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
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

    type DoGetStream = BoxedFlightStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let Ticket { ticket } = request.into_inner();
        let ticket = {
            let mut buf = Cursor::new(&ticket);
            TaskStreamTicket::decode(&mut buf)
                .map_err(|e| Status::invalid_argument(e.to_string()))?
        };
        debug!("{:?}", ticket);
        let TaskStreamTicket { channel } = ticket;
        let (tx, rx) = oneshot::channel();
        let event = crate::worker::WorkerEvent::FetchThisWorkerStream {
            channel: channel.into(),
            result: tx,
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let stream = rx
            .await
            .map_err(|_| Status::internal("failed to receive task stream"))??;
        let stream = stream.map_err(|e| FlightError::from_external_error(Box::new(e)));
        let stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(|e| Status::from_error(Box::new(e)));
        Ok(Response::new(Box::pin(stream) as Self::DoGetStream))
    }

    type DoPutStream = BoxedFlightStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    type ListActionsStream = BoxedFlightStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}
