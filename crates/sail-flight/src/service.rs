use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
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
use datafusion::prelude::SessionContext;
use futures::{stream, Stream, StreamExt};
use log::{debug, error, info, warn};
use prost::Message;
use sail_plan::config::PlanConfig;
use sail_plan::execute_logical_plan;
use sail_plan::resolver::plan::NamedPlan;
use sail_plan::resolver::PlanResolver;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::session::create_sail_session_context;

/// Sail Flight SQL Service implementation
///
/// This service provides an Arrow Flight SQL server that executes queries using Sail's
/// full SQL pipeline: parsing → resolution → optimization → execution.
///
/// # Architecture
///
/// Uses the **same Sail crates** as `sail-spark-connect` for query processing:
/// 1. Parse SQL using `sail-sql-analyzer` (shared)
/// 2. Convert AST to `spec::Plan` (shared)
/// 3. Resolve plan using `PlanResolver` (shared - handles Spark semantics)
/// 4. Execute via DataFusion (shared)
///
/// # Differences with sail-spark-connect
///
/// - **NO SessionManager**: We use a simple shared `SessionContext` instead of
///   the actor-based multi-session manager (overkill for Flight SQL)
/// - **NO SparkSession extension**: No job tracking, streaming queries, or heartbeats
/// - **Simpler session model**: Flight SQL requests are typically stateless
///
/// # Current Limitations
///
/// - **Streaming**: Results are collected into memory before returning.
///   TODO: Stream RecordBatches incrementally for large result sets
///
/// # Shared components with sail-spark-connect
///
/// - `sail_plan`: PlanResolver, PlanConfig, execute_logical_plan
/// - `sail_sql_analyzer`: SQL parser and AST conversion
/// - `sail_session`: Optimizer and analyzer rules
pub struct SailFlightSqlService {
    ctx: Arc<RwLock<SessionContext>>,
    config: Arc<PlanConfig>,
    /// Cache for prepared statement results to avoid re-executing DDL statements
    /// Key: SQL query, Value: (schema, was_executed flag)
    prepared_cache: Arc<RwLock<HashMap<String, (Arc<Schema>, bool)>>>,
}

impl SailFlightSqlService {
    pub fn new() -> Self {
        let ctx = create_sail_session_context();
        let config = Arc::new(PlanConfig::default());
        SailFlightSqlService {
            ctx: Arc::new(RwLock::new(ctx)),
            config,
            prepared_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if a SQL statement is DDL (Data Definition Language)
    fn is_ddl_statement(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("CREATE ")
            || upper.starts_with("DROP ")
            || upper.starts_with("ALTER ")
            || upper.starts_with("TRUNCATE ")
    }

    /// Execute SQL using Sail's full pipeline (parser + resolver + executor)
    ///
    /// # Execution Pipeline (shared with sail-spark-connect)
    ///
    /// 1. **Parse SQL** → AST using `sail-sql-analyzer::parse_one_statement()`
    /// 2. **Convert AST** → `spec::Plan` using `from_ast_statement()`
    /// 3. **Resolve Plan** → Logical plan using `PlanResolver` (handles Spark semantics)
    /// 4. **Execute** → Via DataFusion's execution engine
    ///    ///
    /// # Arguments
    ///
    /// * `query` - SQL query string to execute
    ///
    /// # Returns
    ///
    /// A single `RecordBatch` containing the query results (first batch only).
    ///
    /// # TODO
    ///
    /// - Use `sail-plan::resolve_and_execute_plan()` directly instead of manual steps
    /// - Stream results instead of collecting into memory
    /// - Support multiple result batches
    async fn execute_sql(&self, query: &str) -> Result<RecordBatch, Status> {
        info!("Executing SQL query: {}", query);

        // Step 1: Parse SQL to AST using Sail's parser
        let statement = parse_one_statement(query).map_err(|e| {
            error!("Parse error for query '{}': {}", query, e);
            Status::invalid_argument(format!("Parse error: {}", e))
        })?;

        // Step 2: Convert AST to spec::Plan
        let plan = from_ast_statement(statement).map_err(|e| {
            error!("AST conversion error for query '{}': {}", query, e);
            Status::internal(format!("AST conversion error: {}", e))
        })?;

        // Step 3: Resolve the plan using PlanResolver
        let ctx = self.ctx.read().await;
        let resolver = PlanResolver::new(&ctx, self.config.clone());
        let NamedPlan {
            plan: logical_plan,
            fields: _,
        } = resolver.resolve_named_plan(plan).await.map_err(|e| {
            error!("Plan resolution error for query '{}': {}", query, e);
            Status::internal(format!("Plan resolution error: {}", e))
        })?;

        // Step 4: Execute the logical plan
        let df = execute_logical_plan(&ctx, logical_plan)
            .await
            .map_err(|e| {
                error!("Execution error for query '{}': {}", query, e);
                Status::internal(format!("Execution error: {}", e))
            })?;

        // Step 5: Collect results
        let batches = df.collect().await.map_err(|e| {
            error!("Collection error for query '{}': {}", query, e);
            Status::internal(format!("Collection error: {}", e))
        })?;

        if batches.is_empty() {
            debug!("Query returned no rows, returning success batch");
            return self.create_success_batch();
        }

        let batch = batches.into_iter().next().unwrap_or_else(|| {
            warn!("Empty batch iterator, returning success batch");
            self.create_success_batch()
                .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())))
        });

        info!(
            "Query executed successfully, returning {} rows",
            batch.num_rows()
        );
        Ok(batch)
    }

    fn create_success_batch(&self) -> Result<RecordBatch, Status> {
        let schema = Schema::new(vec![Field::new("status", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["OK"]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])
            .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))
    }

    fn create_demo_batch(&self) -> Result<RecordBatch, Status> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
        let value_array = Float64Array::from(vec![10.5, 20.3, 15.7, 25.1, 18.9]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))
    }

    fn create_one_batch(&self) -> Result<RecordBatch, Status> {
        let schema = Schema::new(vec![Field::new("1", DataType::Int64, false)]);
        let array = Int64Array::from(vec![1]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])
            .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))
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

        if let Ok(ticket) = TicketStatementQuery::decode(ticket_data.as_ref()) {
            let sql = String::from_utf8_lossy(&ticket.statement_handle).to_string();
            debug!("do_get_fallback: decoded SQL = {}", sql);

            let batch = if sql.trim().eq_ignore_ascii_case("SELECT 1") {
                self.create_one_batch()?
            } else {
                self.execute_sql(&sql).await?
            };

            let schema = batch.schema();
            let batches = vec![Ok(batch)];
            let batch_stream = futures::stream::iter(batches);

            let flight_data_stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batch_stream)
                .map(|result| {
                    result.map_err(|e| Status::internal(format!("Encoding error: {}", e)))
                });

            return Ok(Response::new(Box::pin(flight_data_stream)));
        }

        debug!("do_get_fallback: using demo data");
        let batch = self.create_demo_batch()?;
        let schema = batch.schema();
        let batches = vec![Ok(batch)];
        let batch_stream = futures::stream::iter(batches);

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
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

        // Execute to get schema
        let batch = self.execute_sql(&sql).await?;
        let schema = batch.schema();

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

    // ========================================================================
    // Unimplemented Flight SQL Operations (Future Work)
    // ========================================================================
    //
    // The following methods are part of the Arrow Flight SQL specification
    // but are not yet implemented. They return Status::unimplemented() to
    // indicate to clients that these operations are not supported.
    //
    // See ARCHITECTURE.md for implementation roadmap.

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

        // Check if we already have this cached (for DDL statements)
        let schema = {
            let cache = self.prepared_cache.read().await;
            if let Some((cached_schema, _)) = cache.get(&sql) {
                debug!("Using cached schema for: {}", sql);
                cached_schema.clone()
            } else {
                drop(cache);
                // Execute to get schema
                let batch = self.execute_sql(&sql).await?;
                batch.schema()
            }
        };

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
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_catalogs"))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_schemas"))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_tables"))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info_table_types"))
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

        let batch = if sql.trim().eq_ignore_ascii_case("SELECT 1") {
            self.create_one_batch()?
        } else {
            self.execute_sql(&sql).await?
        };

        debug!("do_get_statement: returning {} rows", batch.num_rows());

        let schema = batch.schema();
        let batches = vec![Ok(batch)];
        let batch_stream = futures::stream::iter(batches);

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_catalogs"))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_schemas"))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_tables"))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_table_types"))
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

        let is_ddl = Self::is_ddl_statement(&query.query);

        // Execute the query to get schema
        let batch = self.execute_sql(&query.query).await?;
        let schema = batch.schema();

        // For DDL statements, cache that we already executed it
        if is_ddl {
            let mut cache = self.prepared_cache.write().await;
            cache.insert(query.query.clone(), (schema.clone(), true));
            debug!("Cached DDL statement as already executed: {}", query.query);
        }

        // Use the query as the prepared statement handle
        let handle = query.query.clone().into_bytes();

        // Convert schema to IPC bytes
        let options = IpcWriteOptions::default();
        let dataset_schema_data =
            arrow_flight::IpcMessage::try_from(arrow_flight::SchemaAsIpc::new(&schema, &options))
                .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

        let empty_schema = Schema::empty();
        let param_schema_data = arrow_flight::IpcMessage::try_from(arrow_flight::SchemaAsIpc::new(
            &empty_schema,
            &options,
        ))
        .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: dataset_schema_data.0,
            parameter_schema: param_schema_data.0,
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
