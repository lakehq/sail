use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate,
    DoPutPreparedStatementResult, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use prost::Message;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::config::PlanConfig;
use sail_plan::resolve_and_execute_plan;
use sail_session::session_manager::SessionManager;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;
use sail_sql_parser::ast::statement::Statement;
use sail_telemetry::metrics::{MetricAttribute, MetricRegistry};
use sail_telemetry::telemetry::global_metrics;
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryKind {
    Select,
    Ddl,
    Dml,
    Other,
}

impl QueryKind {
    fn label(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Ddl => "DDL",
            Self::Dml => "DML",
            Self::Other => "OTHER",
        }
    }

    fn from_statement(stmt: &Statement) -> Self {
        match stmt {
            Statement::Query(_) | Statement::Explain { .. } => Self::Select,
            Statement::ShowDatabases { .. }
            | Statement::ShowCatalogs { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowCreateTable { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowViews { .. }
            | Statement::ShowFunctions { .. }
            | Statement::Describe { .. } => Self::Select,
            Statement::CreateDatabase { .. }
            | Statement::CreateTable { .. }
            | Statement::ReplaceTable { .. }
            | Statement::CreateView { .. }
            | Statement::AlterDatabase { .. }
            | Statement::AlterTable { .. }
            | Statement::AlterView { .. }
            | Statement::DropDatabase { .. }
            | Statement::DropTable { .. }
            | Statement::DropView { .. }
            | Statement::DropFunction { .. }
            | Statement::CommentOnCatalog { .. }
            | Statement::CommentOnDatabase { .. }
            | Statement::CommentOnTable { .. }
            | Statement::CommentOnColumn { .. }
            | Statement::RefreshTable { .. }
            | Statement::RefreshFunction { .. } => Self::Ddl,
            Statement::InsertInto { .. }
            | Statement::InsertOverwriteDirectory { .. }
            | Statement::InsertIntoAndReplace { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::MergeInto { .. }
            | Statement::LoadData { .. } => Self::Dml,
            _ => Self::Other,
        }
    }
}

pub struct SailFlightSqlOptions {
    /// Maximum rows to return per query (0 = unlimited)
    pub max_rows: usize,
}

pub struct SailFlightSqlService {
    session_manager: SessionManager,
    config: Arc<PlanConfig>,
    options: SailFlightSqlOptions,
    /// Optional metric registry for OTLP metrics (None if telemetry disabled)
    metrics: Option<Arc<MetricRegistry>>,
}

impl SailFlightSqlService {
    pub fn new(session_manager: SessionManager, options: SailFlightSqlOptions) -> Self {
        let config = Arc::new(PlanConfig::default());

        // Get global metric registry if telemetry is enabled
        let metrics = global_metrics().map(|m| m.registry);
        if metrics.is_some() {
            info!("OTLP metrics enabled for Flight SQL service");
        }

        SailFlightSqlService {
            session_manager,
            config,
            options,
            metrics,
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
            .map_err(|e| Status::internal(format!("Session error: {}", e)))
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

    /// Helper to convert schemas to IPC format for Flight SQL protocol
    ///
    /// Returns a tuple of (dataset_schema_bytes, parameter_schema_bytes)
    fn schemas_to_ipc_bytes(
        dataset_schema: &Schema,
        param_schema: &Schema,
    ) -> Result<(prost::bytes::Bytes, prost::bytes::Bytes), Status> {
        let options = IpcWriteOptions::default();

        let dataset_schema_data = arrow_flight::IpcMessage::try_from(
            arrow_flight::SchemaAsIpc::new(dataset_schema, &options),
        )
        .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

        let param_schema_data = arrow_flight::IpcMessage::try_from(arrow_flight::SchemaAsIpc::new(
            param_schema,
            &options,
        ))
        .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

        Ok((dataset_schema_data.0, param_schema_data.0))
    }

    /// Convert a SQL string to an `ExecutionPlan` using `resolve_and_execute_plan`.
    ///
    /// This method is the single entry point for SQL execution and schema resolution.
    /// It parses, resolves, and creates a physical execution plan without actually running
    /// the plan. The returned plan can be used either to retrieve the output schema or to
    /// execute it via the `JobService` runner.
    async fn sql_to_execution_plan(
        &self,
        sql: &str,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, Status> {
        let statement = parse_one_statement(sql)
            .map_err(|e| Status::invalid_argument(format!("Parse error: {}", e)))?;
        let plan = from_ast_statement(statement)
            .map_err(|e| Status::internal(format!("AST conversion error: {}", e)))?;
        let (exec_plan, _) = resolve_and_execute_plan(ctx, self.config.clone(), plan)
            .await
            .map_err(|e| Status::internal(format!("Plan error: {}", e)))?;
        Ok(exec_plan)
    }

    /// Execute SQL and return a stream of `RecordBatch`es.
    ///
    /// The schema is accessible via `stream.schema()`.
    async fn execute_sql_stream(&self, sql: &str) -> Result<SendableRecordBatchStream, Status> {
        let ctx = self.get_session_context().await?;
        let plan = self.sql_to_execution_plan(sql, &ctx).await?;
        let service = ctx
            .extension::<JobService>()
            .map_err(|e| Status::internal(format!("Job service not found: {}", e)))?;
        let stream = service
            .runner()
            .execute(&ctx, plan)
            .await
            .map_err(|e| Status::internal(format!("Execution error: {}", e)))?;
        Ok(stream)
    }

    /// Execute SQL and collect all `RecordBatch`es into memory.
    ///
    /// Applies the `max_rows` limit if configured.
    async fn execute_sql_batches(&self, sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let stream = self.execute_sql_stream(sql).await?;
        let batches: Vec<RecordBatch> = stream
            .err_into::<Box<dyn std::error::Error + Send + Sync>>()
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Collection error: {}", e)))?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        if self.options.max_rows > 0 && total_rows > self.options.max_rows {
            let mut limited = Vec::new();
            let mut remaining = self.options.max_rows;
            for batch in batches {
                if remaining == 0 {
                    break;
                }
                if batch.num_rows() <= remaining {
                    remaining -= batch.num_rows();
                    limited.push(batch);
                } else {
                    limited.push(batch.slice(0, remaining));
                    remaining = 0;
                }
            }
            let final_rows: usize = limited.iter().map(|b| b.num_rows()).sum();
            warn!(
                "Query results truncated: {} rows returned (limit: {}), original: {} rows",
                final_rows, self.options.max_rows, total_rows
            );
            Ok(limited)
        } else {
            Ok(batches)
        }
    }

    /// Resolve SQL to get its output schema without executing the query.
    async fn get_query_schema(&self, sql: &str) -> Result<Arc<Schema>, Status> {
        let ctx = self.get_session_context().await?;
        let plan = self.sql_to_execution_plan(sql, &ctx).await?;
        Ok(plan.schema())
    }

    /// Wrap an async operation with metrics reporting (active query count, duration, status).
    ///
    /// Row count is not tracked here because the wrapped function may return a stream whose
    /// rows are consumed lazily by the caller; callers that need row-level metrics should
    /// record them separately after consuming the result.
    ///
    /// TODO: For streaming results, the active-query gauge is decremented and
    /// success/error metrics are recorded when the stream is created, not when it is
    /// fully consumed. This means metrics may not accurately reflect the true query
    /// lifetime. Consider wrapping the returned stream to track completion/errors.
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
        let stream = stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ticket_data = request.into_inner().ticket;
        debug!("do_get_fallback: ticket_data len = {}", ticket_data.len());

        let ticket = TicketStatementQuery::decode(ticket_data.as_ref())
            .map_err(|_| Status::not_found("Unknown ticket"))?;
        let sql = String::from_utf8_lossy(&ticket.statement_handle).to_string();
        debug!("do_get_fallback: decoded SQL = {}", sql);

        let query_type = parse_one_statement(&sql)
            .map(|s| QueryKind::from_statement(&s).label())
            .unwrap_or("OTHER");

        let stream = self
            .execute_with_metrics_reporting(query_type, || self.execute_sql_stream(&sql))
            .await?;
        let schema = stream.schema();

        let mapped_stream = stream.map(|result| {
            result.map_err(|e| {
                error!("Batch streaming error: {}", e);
                arrow_flight::error::FlightError::ExternalError(Box::new(e))
            })
        });

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(mapped_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        debug!("get_flight_info_statement: SQL = {}", sql);

        // Create ticket containing the SQL query
        let ticket = TicketStatementQuery {
            statement_handle: sql.clone().into_bytes().into(),
        };
        let ticket_bytes = ticket.as_any().encode_to_vec();

        // Resolve schema without executing the query
        let schema = self.get_query_schema(&sql).await?;

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket_bytes.into(),
            }),
            location: vec![],
            expiration_time: None,
            app_metadata: Default::default(),
        };

        // Use the builder pattern which handles schema encoding
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner())
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_substrait_plan"))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = String::from_utf8_lossy(&cmd.prepared_statement_handle).to_string();
        debug!("get_flight_info_prepared_statement: SQL = {}", sql);

        // Create ticket for do_get
        let ticket = TicketStatementQuery {
            statement_handle: cmd.prepared_statement_handle.clone(),
        };
        let ticket_bytes = ticket.as_any().encode_to_vec();

        // Resolve schema for the prepared statement (don't execute yet)
        let schema = self.get_query_schema(&sql).await?;

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
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_catalogs");
        // Schema: catalog_name (utf8)
        let schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);

        let ticket_bytes = query.as_any().encode_to_vec();
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
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_schemas: catalog={:?}", query.catalog);
        // Schema: catalog_name (utf8), db_schema_name (utf8)
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]);

        let ticket_bytes = query.as_any().encode_to_vec();
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
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_tables: catalog={:?}, schema={:?}",
            query.catalog, query.db_schema_filter_pattern
        );
        // Schema for GetTables (without table_schema column for simplicity)
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);

        let ticket_bytes = query.as_any().encode_to_vec();
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
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_table_types");
        // Schema: table_type (utf8)
        let schema = Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);

        let ticket_bytes = query.as_any().encode_to_vec();
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
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_sql_info"))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_primary_keys"))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_exported_keys"))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_imported_keys"))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_cross_reference"))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_xdbc_type_info"))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8_lossy(&ticket.statement_handle).to_string();
        debug!("do_get_statement: SQL = {}", sql);

        let query_type = parse_one_statement(&sql)
            .map(|s| QueryKind::from_statement(&s).label())
            .unwrap_or("OTHER");

        let stream = self
            .execute_with_metrics_reporting(query_type, || self.execute_sql_stream(&sql))
            .await?;
        let schema = stream.schema();

        // Wrap the DataFusion stream to convert errors to FlightError
        let mapped_stream = stream.map(|result| {
            result.map_err(|e| {
                error!("Batch streaming error: {}", e);
                arrow_flight::error::FlightError::ExternalError(Box::new(e))
            })
        });

        // Create flight data stream from the record batch stream
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(mapped_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_catalogs");

        // Return the default catalog "sail"
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let catalog_array = StringArray::from(vec!["sail"]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(catalog_array)])
            .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;

        let batch_stream = futures::stream::iter(vec![Ok(batch)]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_schemas: catalog={:?}", query.catalog);

        // Return the default schema "default"
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));

        let catalog_array = StringArray::from(vec![Some("sail")]);
        let schema_array = StringArray::from(vec!["default"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(catalog_array), Arc::new(schema_array)],
        )
        .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;

        let batch_stream = futures::stream::iter(vec![Ok(batch)]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_tables: catalog={:?}, schema={:?}",
            query.catalog, query.db_schema_filter_pattern
        );

        // Execute SHOW TABLES to get the list of tables
        let batches = self.execute_sql_batches("SHOW TABLES").await?;

        // Convert to Flight SQL schema format
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        // If SHOW TABLES returned results, transform them
        let result_batch = if !batches.is_empty() {
            let source_batch = &batches[0];

            // SHOW TABLES returns: database, tableName, isTemporary
            // We need: catalog_name, db_schema_name, table_name, table_type
            let num_rows = source_batch.num_rows();

            if num_rows > 0 {
                // Extract table names from the result
                let table_names: Vec<&str> =
                    if let Some(col) = source_batch.column_by_name("tableName") {
                        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                            (0..num_rows).filter_map(|i| arr.value(i).into()).collect()
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    };

                let catalog_array = StringArray::from(vec![Some("sail"); table_names.len()]);
                let schema_array = StringArray::from(vec![Some("default"); table_names.len()]);
                let table_array = StringArray::from(table_names);
                let type_array = StringArray::from(vec!["TABLE"; table_array.len()]);

                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(catalog_array),
                        Arc::new(schema_array),
                        Arc::new(table_array),
                        Arc::new(type_array),
                    ],
                )
                .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?
            } else {
                RecordBatch::new_empty(schema.clone())
            }
        } else {
            RecordBatch::new_empty(schema.clone())
        };

        let batch_stream = futures::stream::iter(vec![Ok(result_batch)]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_table_types");

        // Return supported table types
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let type_array = StringArray::from(vec!["TABLE", "VIEW", "TEMPORARY VIEW"]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(type_array)])
            .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;

        let batch_stream = futures::stream::iter(vec![Ok(batch)]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_sql_info"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_xdbc_type_info"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("do_put_statement_update"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented("do_put_prepared_statement_query"))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("do_put_prepared_statement_update"))
    }

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("do_put_substrait_plan"))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        info!("do_action_create_prepared_statement: SQL = {}", query.query);

        let statement = parse_one_statement(&query.query)
            .map_err(|e| Status::invalid_argument(format!("Parse error: {}", e)))?;
        let kind = QueryKind::from_statement(&statement);

        let schema = match kind {
            QueryKind::Ddl | QueryKind::Dml => {
                // Execute DDL/DML eagerly so that side effects (e.g. creating a view)
                // are visible to subsequent queries in the same session.
                let stream = self.execute_sql_stream(&query.query).await?;
                stream
                    .err_into::<Box<dyn std::error::Error + Send + Sync>>()
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| Status::internal(format!("Execution error: {}", e)))?;
                Arc::new(Schema::empty())
            }
            _ => self.get_query_schema(&query.query).await?,
        };

        // Use the query as the prepared statement handle
        let handle = query.query.clone().into_bytes();

        // Convert schema to IPC bytes
        let empty_schema = Schema::empty();
        let (dataset_schema, parameter_schema) =
            Self::schemas_to_ipc_bytes(&schema, &empty_schema)?;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema,
            parameter_schema,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let sql = String::from_utf8_lossy(&query.prepared_statement_handle).to_string();
        debug!("do_action_close_prepared_statement: SQL = {}", sql);
        // Nothing to clean up in this simple implementation
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented("do_action_begin_transaction"))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented("do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
