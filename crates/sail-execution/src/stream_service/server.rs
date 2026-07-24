use std::fmt::Debug;
use std::pin::Pin;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, TryStreamExt};
use log::debug;
use prost::Message;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, Streaming, async_trait};

use crate::error::ExecutionResult;
use crate::id::TaskStreamKey;
use crate::stream::r#gen::TaskStreamTicket;
use crate::stream::reader::TaskStreamSource;

pub trait TaskStreamKeyDecoder: Debug + Send + 'static {
    fn decode(bytes: &[u8]) -> Result<Self, Status>
    where
        Self: Sized;
}

impl From<TaskStreamTicket> for TaskStreamKey {
    fn from(ticket: TaskStreamTicket) -> Self {
        let TaskStreamTicket {
            job_id,
            stage,
            partition,
            attempt,
            channel,
        } = ticket;
        Self {
            job_id: job_id.into(),
            stage: stage as usize,
            partition: partition as usize,
            attempt: attempt as usize,
            channel: channel as usize,
        }
    }
}

impl TaskStreamKeyDecoder for TaskStreamKey {
    fn decode(bytes: &[u8]) -> Result<Self, Status> {
        TaskStreamTicket::decode(bytes)
            .map(Into::into)
            .map_err(|e| Status::invalid_argument(e.to_string()))
    }
}

#[async_trait]
pub trait TaskStreamFetcher<K>: Send + Sync {
    async fn fetch(
        &self,
        key: K,
        sender: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()>;
}

pub struct TaskStreamFlightServer<K> {
    fetcher: Box<dyn TaskStreamFetcher<K>>,
}

impl<K> TaskStreamFlightServer<K> {
    pub fn new(fetcher: Box<dyn TaskStreamFetcher<K>>) -> Self {
        Self { fetcher }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[async_trait]
impl<K> FlightService for TaskStreamFlightServer<K>
where
    K: TaskStreamKeyDecoder,
{
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
        let key = K::decode(&ticket)?;
        debug!("{key:?}");
        let (tx, rx) = oneshot::channel();
        self.fetcher.fetch(key, tx).await?;
        let stream = rx
            .await
            .map_err(|_| Status::internal("failed to receive task stream"))??;
        let stream = stream.map_err(|e| FlightError::Tonic(Box::new(e.into())));
        let stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(Status::from);
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
