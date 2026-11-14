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
use crate::id::{TaskStreamKey, TaskStreamKeySet};
use crate::shuffle::ShuffleCompression;
use crate::stream::r#gen::TaskStreamTicket;
use crate::stream::reader::TaskStreamSource;

pub trait TaskStreamKeyDecoder: Debug + Send + 'static {
    fn decode(bytes: &[u8]) -> Result<Self, Status>
    where
        Self: Sized;
}

impl TryFrom<TaskStreamTicket> for TaskStreamKeySet {
    type Error = Status;

    fn try_from(ticket: TaskStreamTicket) -> Result<Self, Status> {
        let TaskStreamTicket {
            job_id,
            stage,
            partition,
            attempt,
            channel,
            release,
        } = ticket;
        if partition.is_empty() || partition.len() != attempt.len() || channel.is_empty() {
            return Err(Status::invalid_argument(
                "task stream ticket must have channels, and tasks with a partition and an attempt each",
            ));
        }
        Ok(Self {
            job_id: job_id.into(),
            stage: stage as usize,
            tasks: partition
                .into_iter()
                .zip(attempt)
                .map(|(partition, attempt)| (partition as usize, attempt as usize))
                .collect(),
            channels: channel.into_iter().map(|x| x as usize).collect(),
            release,
        })
    }
}

impl TryFrom<TaskStreamTicket> for TaskStreamKey {
    type Error = Status;

    fn try_from(ticket: TaskStreamTicket) -> Result<Self, Status> {
        let keys = TaskStreamKeySet::try_from(ticket)?;
        match (keys.tasks.as_slice(), keys.channels.as_slice()) {
            (&[(partition, attempt)], &[channel]) => Ok(Self {
                job_id: keys.job_id,
                stage: keys.stage,
                partition,
                attempt,
                channel,
            }),
            _ => Err(Status::invalid_argument(
                "task stream ticket must have a single stream",
            )),
        }
    }
}

impl TaskStreamKeyDecoder for TaskStreamKeySet {
    fn decode(bytes: &[u8]) -> Result<Self, Status> {
        TaskStreamTicket::decode(bytes)
            .map_err(|e| Status::invalid_argument(e.to_string()))?
            .try_into()
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
    compression: ShuffleCompression,
}

impl<K> TaskStreamFlightServer<K> {
    pub fn new(fetcher: Box<dyn TaskStreamFetcher<K>>, compression: ShuffleCompression) -> Self {
        Self {
            fetcher,
            compression,
        }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Encodes a task stream as Flight data. The stream can have the data of several
/// tasks in arbitrary order, since the encoder keeps no state from one batch to the
/// next other than the schema, which is the same for all tasks of a stage.
pub(super) fn encode_task_stream(
    stream: TaskStreamSource,
    compression: ShuffleCompression,
) -> Result<impl Stream<Item = Result<FlightData, Status>> + Send + 'static, Status> {
    let stream = stream.map_err(|e| FlightError::Tonic(Box::new(e.into())));
    let options = compression
        .ipc_write_options()
        .map_err(|e| Status::internal(e.to_string()))?;
    Ok(FlightDataEncoderBuilder::new()
        .with_options(options)
        .build(stream)
        .map_err(Status::from))
}

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
        let stream = encode_task_stream(stream, self.compression)?;
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
