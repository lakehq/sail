use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

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
use log::{debug, error, info};
use prost::Message;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::config::PlanConfig;
use sail_plan::resolve_and_execute_plan;
use sail_session::session_manager::SessionManager;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;
use sail_telemetry::metrics::{MetricAttribute, MetricRegistry};
use sail_telemetry::telemetry::global_metrics;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};

use crate::state::{QueryTicket, SailFlightSqlState};

pub struct SailFlightSqlService {
    session_manager: SessionManager,
    config: Arc<PlanConfig>,
    /// Optional metric registry for OTLP metrics (None if telemetry disabled)
    metrics: Option<Arc<MetricRegistry>>,
    state: Arc<Mutex<SailFlightSqlState>>,
}

impl SailFlightSqlService {
    pub fn new(session_manager: SessionManager) -> Self {
        let config = Arc::new(PlanConfig::default());

        // Get global metric registry if telemetry is enabled
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

    /// Default session ID for Flight SQL connections
    const DEFAULT_SESSION_ID: &'static str = "flight-default";
    /// Default user ID for Flight SQL connections
    const DEFAULT_USER_ID: &'static str = "flight-user";

    /// Get or create a session context for this request
    async fn get_session_context(&self) -> Result<datafusion::prelude::SessionContext, Status> {
        self.session_manager
            .get_or_create_session_context(
                Self::DEFAULT_SESSION_ID.to_string(),
                Self::DEFAULT_USER_ID.to_string(),
            )
            .await
            .map_err(|e| Status::internal(format!("session error: {e}")))
    }

    // =========================================================================
    // Metrics helpers
    // =========================================================================

    /// Record query execution metrics
    fn record_query_metrics(
        &self,
        query_type: &'static str,
        status: &'static str,
        duration_secs: f64,
        rows: usize,
    ) {
        if let Some(ref m) = self.metrics {
            let type_attr = (
                MetricAttribute::FLIGHT_QUERY_TYPE,
                Cow::Borrowed(query_type),
            );
            let status_attr = (MetricAttribute::FLIGHT_QUERY_STATUS, Cow::Borrowed(status));

            // Counter: total queries
            m.flight_query_count
                .adder(1u64)
                .with_attribute(type_attr.clone())
                .with_attribute(status_attr.clone())
                .emit();

            // Histogram: duration
            m.flight_query_duration
                .recorder(duration_secs)
                .with_attribute(type_attr.clone())
                .with_attribute(status_attr)
                .emit();

            // Histogram: rows
            if let Ok(r) = u64::try_from(rows) {
                m.flight_query_rows
                    .recorder(r)
                    .with_attribute(type_attr)
                    .emit();
            }
        }
    }

    /// Track active query count
    fn record_active_query(&self, delta: i64) {
        if let Some(ref m) = self.metrics {
            m.flight_query_active.adder(delta).emit();
        }
    }

    /// Record new connection
    fn record_connection(&self) {
        if let Some(ref m) = self.metrics {
            m.flight_connections.adder(1u64).emit();
        }
    }

    /// Wrap an async operation with metrics reporting (active query count, duration, status).
    async fn execute_with_metrics_reporting<F, Fut, T>(
        &self,
        query_type: &'static str,
        f: F,
    ) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let start = Instant::now();
        self.record_active_query(1);
        let result = f().await;
        self.record_active_query(-1);
        let duration = start.elapsed().as_secs_f64();
        match result {
            Ok(val) => {
                self.record_query_metrics(query_type, "success", duration, 0);
                Ok(val)
            }
            Err(e) => {
                self.record_query_metrics(query_type, "error", duration, 0);
                Err(e)
            }
        }
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
        debug!("Handshake received from client");
        self.record_connection();
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

        let statement = parse_one_statement(&sql)
            .map_err(|e| Status::invalid_argument(format!("parse error: {e}")))?;
        let spec_plan = from_ast_statement(statement)
            .map_err(|e| Status::internal(format!("AST conversion error: {e}")))?;
        let is_command = matches!(spec_plan, spec::Plan::Command(_));
        let query_type = if is_command { "COMMAND" } else { "SELECT" };
        let config = self.config.clone();

        let (stream, schema) = self
            .execute_with_metrics_reporting(query_type, || async {
                let ctx = self.get_session_context().await?;
                let (execution_plan, _) =
                    resolve_and_execute_plan(&ctx, config, spec_plan)
                        .await
                        .map_err(|e| Status::internal(format!("plan error: {e}")))?;
                let schema = execution_plan.schema();
                let service = ctx
                    .extension::<JobService>()
                    .map_err(|e| Status::internal(format!("job service not found: {e}")))?;
                let mut raw_stream = service
                    .runner()
                    .execute(&ctx, execution_plan)
                    .await
                    .map_err(|e| Status::internal(format!("execution error: {e}")))?;
                let stream: SendableRecordBatchStream = if is_command {
                    // Eagerly drain command (DDL/DML) so side effects take place
                    // before the client issues the next statement.
                    while let Some(result) = raw_stream.next().await {
                        result.map_err(|e| Status::internal(format!("execution error: {e}")))?;
                    }
                    Box::pin(RecordBatchStreamAdapter::new(
                        schema.clone(),
                        stream::empty(),
                    ))
                } else {
                    raw_stream
                };
                Ok((stream, schema))
            })
            .await?;

        let ticket = QueryTicket::new();
        self.state.lock().await.insert(ticket.clone(), stream);

        let ticket_proto = TicketStatementQuery {
            statement_handle: ticket.as_bytes().to_vec().into(),
        };
        let ticket_bytes = ticket_proto.as_any().encode_to_vec();

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket_bytes.into(),
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
            result.map_err(|e| {
                error!("batch streaming error: {e}");
                arrow_flight::error::FlightError::ExternalError(Box::new(e))
            })
        });

        let output = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(output)
            .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
