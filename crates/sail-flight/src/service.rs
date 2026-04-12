use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, Stream, StreamExt};
use log::{debug, info};
use prost::Message;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::config::PlanConfig;
use sail_plan::resolve_and_execute_plan;
use sail_session::session_manager::SessionManager;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;
use sail_telemetry::metrics::MetricRegistry;
use sail_telemetry::telemetry::global_metrics;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};

use crate::state::{QueryTicket, SailFlightSqlState};
use crate::stream::{MetricContext, MetricsRecordingStream};

pub struct SailFlightSqlService {
    session_manager: SessionManager,
    config: Arc<PlanConfig>,
    metrics: Option<Arc<MetricRegistry>>,
    state: Arc<Mutex<SailFlightSqlState>>,
}

impl SailFlightSqlService {
    pub fn new(session_manager: SessionManager) -> Self {
        let config = Arc::new(PlanConfig::default());
        let metrics = global_metrics().map(|m| m.registry);
        if metrics.is_some() {
            info!("OTLP metrics enabled for Flight SQL service");
        }
        SailFlightSqlService {
            session_manager,
            config,
            metrics,
            state: Arc::new(Mutex::new(SailFlightSqlState::new())),
        }
    }

    const DEFAULT_SESSION_ID: &'static str = "flight-default";
    const DEFAULT_USER_ID: &'static str = "flight-user";

    async fn get_session_context(&self) -> Result<datafusion::prelude::SessionContext, Status> {
        self.session_manager
            .get_or_create_session_context(
                Self::DEFAULT_SESSION_ID.to_string(),
                Self::DEFAULT_USER_ID.to_string(),
            )
            .await
            .map_err(|e| Status::internal(format!("session error: {e}")))
    }
}

#[tonic::async_trait]
impl FlightSqlService for SailFlightSqlService {
    type FlightService = SailFlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        debug!("handshake received from client");
        if let Some(ref m) = self.metrics {
            m.flight_connection_total_count.adder(1u64).emit();
        }
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let output = stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        debug!("get_flight_info_statement: {sql}");

        let stmt = parse_one_statement(&sql)
            .map_err(|e| Status::invalid_argument(format!("parse error: {e}")))?;
        let plan = from_ast_statement(stmt)
            .map_err(|e| Status::internal(format!("plan conversion error: {e}")))?;
        let is_command = matches!(plan, spec::Plan::Command(_));
        let statement_type = if is_command { "command" } else { "query" };

        let ctx = self.get_session_context().await?;
        let (plan, _) = resolve_and_execute_plan(&ctx, self.config.clone(), plan)
            .await
            .map_err(|e| Status::internal(format!("plan error: {e}")))?;
        let schema = plan.schema();
        let service = ctx
            .extension::<JobService>()
            .map_err(|e| Status::internal(format!("job service not found: {e}")))?;
        let mut raw = service
            .runner()
            .execute(&ctx, plan)
            .await
            .map_err(|e| Status::internal(format!("execution error: {e}")))?;

        let stream: SendableRecordBatchStream = if is_command {
            let mut batches = Vec::new();
            while let Some(result) = raw.next().await {
                batches
                    .push(result.map_err(|e| Status::internal(format!("execution error: {e}")))?);
            }
            Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                stream::iter(batches.into_iter().map(Ok)),
            ))
        } else {
            raw
        };

        let stream: SendableRecordBatchStream = if let Some(ref m) = self.metrics {
            Box::pin(MetricsRecordingStream::new(
                stream,
                m.clone(),
                MetricContext { statement_type },
            ))
        } else {
            stream
        };

        let ticket = QueryTicket::new();
        self.state.lock().await.insert(ticket.clone(), stream);

        let ticket = TicketStatementQuery {
            statement_handle: ticket.as_bytes().to_vec().into(),
        };
        let ticket = ticket.as_any().encode_to_vec();

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket.into(),
            }),
            location: vec![],
            expiration_time: None,
            app_metadata: Default::default(),
        };

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner())
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema error: {e}")))?;

        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ticket = QueryTicket::try_from(ticket.statement_handle.as_ref())?;
        debug!("do_get_statement: {ticket:?}");

        let stream = self
            .state
            .lock()
            .await
            .take(&ticket)
            .ok_or_else(|| Status::not_found("query ticket not found or already consumed"))?;
        let schema = stream.schema();

        let output = stream.map(|result| {
            result.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))
        });

        let output = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(output)
            .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
