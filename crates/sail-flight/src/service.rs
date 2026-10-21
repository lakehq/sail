use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, Command, DoPutPreparedStatementResult,
    DoPutUpdateResult, ProstMessageExt, TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use prost::Message;
use sail_plan::config::PlanConfig;
use sail_session::error::SessionError;
use sail_session::session_manager::SessionManager;
use sail_telemetry::metrics::MetricRegistry;
use sail_telemetry::telemetry::global_metrics;
use tonic::{Request, Response, Status, Streaming};

use crate::metadata::metadata_batch;
use crate::protocol::{CloseSessionResult, SessionOptions};
use crate::query::{dataset_schema, internal, invalid};
use crate::state::{
    BoundParameters, FlightSession, PreparedStatement, QueryResult, SailFlightSqlState, new_handle,
};

type FlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

pub struct SailFlightSqlService {
    pub(super) session_manager: SessionManager,
    pub(super) config: Arc<PlanConfig>,
    pub(super) metrics: Option<Arc<MetricRegistry>>,
    pub(super) state: Arc<Mutex<SailFlightSqlState>>,
}

impl SailFlightSqlService {
    pub fn new(session_manager: SessionManager) -> Self {
        let state = Arc::new(Mutex::new(SailFlightSqlState::default()));
        let weak = Arc::downgrade(&state);
        session_manager.add_session_cleanup(move |id| {
            if let Some(state) = weak.upgrade() {
                state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove_session(id);
            }
        });
        Self {
            session_manager,
            config: Arc::new(PlanConfig::default()),
            metrics: global_metrics().map(|m| m.registry),
            state,
        }
    }

    async fn statement(
        &self,
        session: &FlightSession,
        handle: &[u8],
    ) -> Result<(String, PreparedStatement), Status> {
        let handle = std::str::from_utf8(handle).map_err(invalid)?.to_string();
        let state = self.state.lock().map_err(internal)?;
        let statement = state
            .statements
            .get(&handle)
            .filter(|s| s.session_id == session.id)
            .ok_or_else(|| Status::not_found("prepared statement handle not found"))?;
        Ok((handle, statement.clone()))
    }

    fn save_statement(&self, handle: String, statement: PreparedStatement) -> Result<(), Status> {
        let mut state = self.state.lock().map_err(internal)?;
        if !state.sessions.contains_key(&statement.session_id) {
            return Err(Status::not_found(
                "Flight SQL session has expired or closed",
            ));
        }
        state.statements.insert(handle, statement);
        Ok(())
    }

    async fn flight_info(
        &self,
        session: &FlightSession,
        descriptor: FlightDescriptor,
        stream: SendableRecordBatchStream,
    ) -> Result<FlightInfo, Status> {
        let handle = new_handle();
        let schema = stream.schema();
        let ticket = TicketStatementQuery {
            statement_handle: handle.as_bytes().to_vec().into(),
        }
        .as_any()
        .encode_to_vec();
        let info = FlightInfo::new()
            .with_descriptor(descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket)))
            .try_with_schema(&schema)
            .map_err(internal)?;
        let mut state = self.state.lock().map_err(internal)?;
        if !state.sessions.contains_key(&session.id) {
            return Err(Status::not_found(
                "Flight SQL session has expired or closed",
            ));
        }
        state.results.insert(
            handle,
            QueryResult {
                session_id: session.id.clone(),
                stream,
            },
        );
        Ok(info)
    }

    async fn command_statement(
        &self,
        session: &FlightSession,
        command: Command,
    ) -> Result<PreparedStatement, Status> {
        match command {
            Command::CommandStatementQuery(query) => {
                reject_transaction(query.transaction_id.as_deref())?;
                self.prepare(session, &query.query).await
            }
            Command::CommandPreparedStatementQuery(query) => {
                let (_, statement) = self
                    .statement(session, &query.prepared_statement_handle)
                    .await?;
                self.refresh_statement(session, statement).await
            }
            _ => Err(Status::unimplemented("unsupported statement command")),
        }
    }

    async fn bind(
        &self,
        session: &FlightSession,
        handle: &[u8],
        data: Vec<FlightData>,
    ) -> Result<(String, PreparedStatement), Status> {
        let (handle, mut statement) = self.statement(session, handle).await?;
        if data.iter().any(|message| !message.data_header.is_empty()) {
            let mut reader = FlightRecordBatchStream::new_from_flight_data(stream::iter(
                data.into_iter().map(Ok),
            ));
            let mut batches = Vec::new();
            while let Some(batch) = reader.try_next().await.map_err(invalid)? {
                batches.push(batch);
            }
            let schema = reader
                .schema()
                .ok_or_else(|| Status::invalid_argument("missing parameter schema"))?;
            if schema.fields().len() != statement.parameter_schema.fields().len() {
                return Err(Status::invalid_argument(
                    "incorrect number of prepared statement parameters",
                ));
            }
            statement.parameters = Some(BoundParameters {
                schema: schema.clone(),
                batches,
            });
        } else if !statement.parameter_schema.fields().is_empty() && statement.parameters.is_none()
        {
            return Err(Status::invalid_argument(
                "missing parameter schema and batches",
            ));
        }
        Ok((handle, statement))
    }
}

fn reject_transaction(id: Option<&[u8]>) -> Result<(), Status> {
    if id.is_some_and(|id| !id.is_empty()) {
        Err(Status::unimplemented(
            "Flight SQL transactions are not supported",
        ))
    } else {
        Ok(())
    }
}

fn command(descriptor: &FlightDescriptor) -> Result<Command, Status> {
    if descriptor.r#type != arrow_flight::flight_descriptor::DescriptorType::Cmd as i32 {
        return Err(Status::invalid_argument(
            "Flight SQL requires a CMD descriptor",
        ));
    }
    Command::try_from(Any::decode(descriptor.cmd.clone()).map_err(invalid)?).map_err(invalid)
}

fn batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        batch.schema(),
        stream::once(async { Ok(batch) }),
    ))
}

fn schema_result(schema: &SchemaRef) -> Result<SchemaResult, Status> {
    SchemaAsIpc::new(schema, &IpcWriteOptions::default())
        .try_into()
        .map_err(internal)
}

fn action_result(body: Vec<u8>) -> FlightStream<arrow_flight::Result> {
    Box::pin(stream::once(async {
        Ok(arrow_flight::Result { body: body.into() })
    }))
}

#[tonic::async_trait]
impl FlightService for SailFlightSqlService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;
    type DoExchangeStream = FlightStream<FlightData>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let session = self.session(&request, true, true).await?;
        let output: Self::HandshakeStream = Box::pin(stream::once(async {
            Ok(HandshakeResponse {
                protocol_version: 0,
                payload: Default::default(),
            })
        }));
        let mut response = self.response(&session, output)?;
        response.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", session.id).parse().map_err(internal)?,
        );
        Ok(response)
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let session = self.session(&request, false, true).await?;
        let descriptor = request.into_inner();
        let command = command(&descriptor)?;
        let stream = match command {
            query @ (Command::CommandStatementQuery(_)
            | Command::CommandPreparedStatementQuery(_)) => {
                let statement = self.command_statement(&session, query).await?;
                self.execute_statement(&session, &statement).await?
            }
            metadata => batch_stream(metadata_batch(&session, metadata).await?),
        };
        let info = self.flight_info(&session, descriptor, stream).await?;
        self.response(&session, info)
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let session = self.session(&request, false, true).await?;
        let schema = match command(request.get_ref())? {
            query @ (Command::CommandStatementQuery(_)
            | Command::CommandPreparedStatementQuery(_)) => {
                let statement = self.command_statement(&session, query).await?;
                dataset_schema(&statement)?
            }
            metadata => metadata_batch(&session, metadata).await?.schema(),
        };
        self.response(&session, schema_result(&schema)?)
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let session = self.session(&request, false, true).await?;
        let ticket = Any::decode(request.into_inner().ticket).map_err(invalid)?;
        let ticket: TicketStatementQuery = ticket
            .unpack()
            .map_err(invalid)?
            .ok_or_else(|| Status::invalid_argument("invalid result ticket"))?;
        let handle = std::str::from_utf8(&ticket.statement_handle).map_err(invalid)?;
        let result = {
            let mut state = self.state.lock().map_err(internal)?;
            if !state
                .results
                .get(handle)
                .is_some_and(|r| r.session_id == session.id)
            {
                return Err(Status::not_found(
                    "result handle not found or already consumed",
                ));
            }
            state
                .results
                .remove(handle)
                .ok_or_else(|| Status::not_found("result handle not found"))?
        };
        let schema = result.stream.schema();
        let output = result.stream.map(|item| {
            item.map_err(|error| arrow_flight::error::FlightError::ExternalError(Box::new(error)))
        });
        let output: Self::DoGetStream = Box::pin(
            FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(output)
                .map(|item| item.map_err(internal)),
        );
        self.response(&session, output)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let session = self.session(&request, false, true).await?;
        // Finish reading and validate parameter data before executing side effects.
        let data = request.into_inner().try_collect::<Vec<_>>().await?;
        let descriptor = data
            .first()
            .and_then(|message| message.flight_descriptor.as_ref())
            .ok_or_else(|| Status::invalid_argument("missing Flight SQL command descriptor"))?;
        let metadata = match command(descriptor)? {
            Command::CommandStatementUpdate(query) => {
                reject_transaction(query.transaction_id.as_deref())?;
                let statement = self.prepare(&session, &query.query).await?;
                DoPutUpdateResult {
                    record_count: self.execute_update(&session, &statement).await?,
                }
                .encode_to_vec()
            }
            Command::CommandPreparedStatementQuery(query) => {
                let (handle, statement) = self
                    .bind(&session, &query.prepared_statement_handle, data)
                    .await?;
                self.save_statement(handle, statement)?;
                DoPutPreparedStatementResult {
                    prepared_statement_handle: None,
                }
                .encode_to_vec()
            }
            Command::CommandPreparedStatementUpdate(query) => {
                let (handle, statement) = self
                    .bind(&session, &query.prepared_statement_handle, data)
                    .await?;
                let statement = self.refresh_statement(&session, statement).await?;
                let count = self.execute_update(&session, &statement).await?;
                self.save_statement(handle, statement)?;
                DoPutUpdateResult {
                    record_count: count,
                }
                .encode_to_vec()
            }
            _ => {
                return Err(Status::unimplemented(
                    "unsupported Flight SQL upload command",
                ));
            }
        };
        let output: Self::DoPutStream = Box::pin(stream::once(async {
            Ok(PutResult {
                app_metadata: metadata.into(),
            })
        }));
        self.response(&session, output)
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let create = !matches!(
            request.get_ref().r#type.as_str(),
            "GetSessionOptions" | "CloseSession"
        );
        let session = self.session(&request, false, create).await?;
        let action = request.into_inner();
        let output = match action.r#type.as_str() {
            "CreatePreparedStatement" => {
                let query: ActionCreatePreparedStatementRequest = Any::decode(action.body)
                    .map_err(invalid)?
                    .unpack()
                    .map_err(invalid)?
                    .ok_or_else(|| Status::invalid_argument("invalid prepared statement action"))?;
                reject_transaction(query.transaction_id.as_deref())?;
                let statement = self.prepare(&session, &query.query).await?;
                let result = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: new_handle().as_bytes().to_vec().into(),
                    dataset_schema: schema_result(&dataset_schema(&statement)?)?.schema,
                    parameter_schema: schema_result(&statement.parameter_schema)?.schema,
                };
                let handle = std::str::from_utf8(&result.prepared_statement_handle)
                    .map_err(internal)?
                    .to_string();
                self.save_statement(handle, statement)?;
                action_result(result.as_any().encode_to_vec())
            }
            "ClosePreparedStatement" => {
                let query: ActionClosePreparedStatementRequest = Any::decode(action.body)
                    .map_err(invalid)?
                    .unpack()
                    .map_err(invalid)?
                    .ok_or_else(|| Status::invalid_argument("invalid prepared statement action"))?;
                let (handle, _) = self
                    .statement(&session, &query.prepared_statement_handle)
                    .await?;
                self.state
                    .lock()
                    .map_err(internal)?
                    .statements
                    .remove(&handle);
                Box::pin(stream::empty()) as Self::DoActionStream
            }
            "SetSessionOptions" => {
                let options = SessionOptions::decode(action.body).map_err(invalid)?;
                action_result(Self::set_options(&session, options).await?.encode_to_vec())
            }
            "GetSessionOptions" => {
                Empty::decode(action.body).map_err(invalid)?;
                action_result(Self::get_options(&session).await?.encode_to_vec())
            }
            "CloseSession" => {
                Empty::decode(action.body).map_err(invalid)?;
                self.session_manager
                    .delete_session(session.id.clone())
                    .await
                    .map_err(|error| match error {
                        SessionError::InvalidArgument(_) => {
                            Status::not_found("Flight SQL session has expired or closed")
                        }
                        error => internal(error),
                    })?;
                action_result(CloseSessionResult { status: 1 }.encode_to_vec())
            }
            _ => {
                return Err(Status::unimplemented(format!(
                    "unsupported Flight SQL action: {}",
                    action.r#type
                )));
            }
        };
        self.response(&session, output)
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let session = self.session(&request, false, true).await?;
        let actions = [
            "CreatePreparedStatement",
            "ClosePreparedStatement",
            "SetSessionOptions",
            "GetSessionOptions",
            "CloseSession",
        ]
        .into_iter()
        .map(|name| {
            Ok(ActionType {
                r#type: name.into(),
                description: name.into(),
            })
        });
        let output: Self::ListActionsStream = Box::pin(stream::iter(actions));
        self.response(&session, output)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("use Flight SQL catalog discovery"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "asynchronous query polling is not supported",
        ))
    }
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not supported"))
    }
}
