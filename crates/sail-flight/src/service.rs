use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
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

use crate::cache::{CacheEntry, PreparedStatementCache};
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
    prepared_cache: Arc<PreparedStatementCache>,
}

impl SailFlightSqlService {
    /// Create a new service with the given cache configuration
    pub fn new(max_cache_size_bytes: usize, enable_cache_stats: bool) -> Self {
        let ctx = create_sail_session_context();
        let config = Arc::new(PlanConfig::default());
        let prepared_cache = Arc::new(PreparedStatementCache::new(
            max_cache_size_bytes,
            enable_cache_stats,
        ));

        SailFlightSqlService {
            ctx: Arc::new(RwLock::new(ctx)),
            config,
            prepared_cache,
        }
    }

    /// Remove SQL comments (-- style) from the beginning of a query
    fn strip_sql_comments(sql: &str) -> String {
        sql.lines()
            .filter(|line| !line.trim().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string()
    }

    /// Check if a SQL statement is DDL (Data Definition Language)
    fn is_ddl_statement(sql: &str) -> bool {
        let cleaned = Self::strip_sql_comments(sql);
        let upper = cleaned.to_uppercase();
        upper.starts_with("CREATE ")
            || upper.starts_with("DROP ")
            || upper.starts_with("ALTER ")
            || upper.starts_with("TRUNCATE ")
    }

    /// Check if a SQL statement is DML (INSERT/UPDATE/DELETE)
    fn is_dml_statement(sql: &str) -> bool {
        let cleaned = Self::strip_sql_comments(sql);
        let upper = cleaned.to_uppercase();
        upper.starts_with("INSERT ")
            || upper.starts_with("UPDATE ")
            || upper.starts_with("DELETE ")
            || upper.starts_with("MERGE ")
    }

    /// Execute SQL using Sail's full pipeline (parser + resolver + executor)
    ///
    /// # Execution Pipeline (shared with sail-spark-connect)
    ///
    /// 1. **Parse SQL** → AST using `sail-sql-analyzer::parse_one_statement()`
    /// 2. **Convert AST** → `spec::Plan` using `from_ast_statement()`
    /// 3. **Resolve Plan** → Logical plan using `PlanResolver` (handles Spark semantics)
    /// 4. **Execute** → Via DataFusion's execution engine
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query string to execute
    ///
    /// # Returns
    ///
    /// All `RecordBatch`es containing the query results.
    async fn execute_sql_batches(&self, query: &str) -> Result<Vec<RecordBatch>, Status> {
        let total_start = Instant::now();
        info!("Executing SQL query: {}", query);

        // Step 1: Parse SQL to AST using Sail's parser
        let parse_start = Instant::now();
        let statement = parse_one_statement(query).map_err(|e| {
            error!("Parse error for query '{}': {}", query, e);
            Status::invalid_argument(format!("Parse error: {}", e))
        })?;
        debug!("  [parse_sql] completed in {:?}", parse_start.elapsed());

        // Step 2: Convert AST to spec::Plan
        let convert_start = Instant::now();
        let plan = from_ast_statement(statement).map_err(|e| {
            error!("AST conversion error for query '{}': {}", query, e);
            Status::internal(format!("AST conversion error: {}", e))
        })?;
        debug!(
            "  [convert_ast_to_plan] completed in {:?}",
            convert_start.elapsed()
        );

        // Step 3: Resolve the plan using PlanResolver
        let resolve_start = Instant::now();
        let ctx = self.ctx.read().await;
        let resolver = PlanResolver::new(&ctx, self.config.clone());
        let NamedPlan {
            plan: logical_plan,
            fields: _,
        } = resolver.resolve_named_plan(plan).await.map_err(|e| {
            error!("Plan resolution error for query '{}': {}", query, e);
            Status::internal(format!("Plan resolution error: {}", e))
        })?;
        debug!(
            "  [resolve_plan] completed in {:?}",
            resolve_start.elapsed()
        );

        // Step 4: Execute the logical plan
        let exec_start = Instant::now();
        let df = execute_logical_plan(&ctx, logical_plan)
            .await
            .map_err(|e| {
                error!("Execution error for query '{}': {}", query, e);
                Status::internal(format!("Execution error: {}", e))
            })?;
        debug!("  [execute_plan] completed in {:?}", exec_start.elapsed());

        // Step 5: Collect results
        let collect_start = Instant::now();
        let batches = df.collect().await.map_err(|e| {
            error!("Collection error for query '{}': {}", query, e);
            Status::internal(format!("Collection error: {}", e))
        })?;
        debug!(
            "  [collect_results] completed in {:?}",
            collect_start.elapsed()
        );

        // Calculate statistics across all batches
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        let num_batches = batches.len();

        info!(
            "Query completed: time={:?}, rows={}, batches={}, memory={:.2} KB",
            total_start.elapsed(),
            total_rows,
            num_batches,
            total_bytes as f64 / 1024.0
        );

        Ok(batches)
    }

    /// Execute SQL and return a single batch (for backwards compatibility)
    async fn execute_sql(&self, query: &str) -> Result<RecordBatch, Status> {
        let batches = self.execute_sql_batches(query).await?;

        if batches.is_empty() {
            debug!("Query returned no rows, returning success batch");
            return self.create_success_batch();
        }

        Ok(batches
            .into_iter()
            .next()
            .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::empty()))))
    }

    /// Resolve SQL plan to get schema without executing
    ///
    /// This is used for prepared statements to get the result schema
    /// without actually executing the query.
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query string to analyze
    ///
    /// # Returns
    ///
    /// The schema that would be returned by executing this query
    async fn get_query_schema(&self, query: &str) -> Result<Arc<Schema>, Status> {
        info!("Resolving schema WITHOUT executing: {}", query);

        // Step 1: Parse SQL to AST
        let statement = parse_one_statement(query).map_err(|e| {
            error!("Parse error for query '{}': {}", query, e);
            Status::invalid_argument(format!("Parse error: {}", e))
        })?;

        // Step 2: Convert AST to spec::Plan
        let plan = from_ast_statement(statement).map_err(|e| {
            error!("AST conversion error for query '{}': {}", query, e);
            Status::internal(format!("AST conversion error: {}", e))
        })?;

        // Step 3: Resolve the plan to get logical plan (and schema)
        let ctx = self.ctx.read().await;
        let resolver = PlanResolver::new(&ctx, self.config.clone());
        let NamedPlan {
            plan: logical_plan,
            fields: _,
        } = resolver.resolve_named_plan(plan).await.map_err(|e| {
            error!("Plan resolution error for query '{}': {}", query, e);
            Status::internal(format!("Plan resolution error: {}", e))
        })?;

        // ✅ Extract schema from logical plan WITHOUT executing
        let schema = logical_plan.schema();
        let arrow_schema = schema.inner().as_ref().clone();

        info!(
            "Schema resolved successfully (NOT executed): {:?}",
            arrow_schema
        );
        Ok(Arc::new(arrow_schema))
    }

    /// Create an empty batch for DDL statements (no result set expected)
    ///
    /// DDL statements (CREATE TABLE, DROP TABLE, etc.) should return 0 rows
    /// according to JDBC/Flight SQL standards. Clients like DBeaver expect
    /// either an empty result set or an update count, not a result table.
    fn create_success_batch(&self) -> Result<RecordBatch, Status> {
        // Return empty schema with 0 rows - this is the standard for DDL
        let schema = Arc::new(Schema::empty());
        Ok(RecordBatch::new_empty(schema))
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

            let batches = if sql.trim().eq_ignore_ascii_case("SELECT 1") {
                vec![self.create_one_batch()?]
            } else {
                self.execute_sql_batches(&sql).await?
            };

            let schema = batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(Schema::empty()));

            debug!("do_get_fallback: streaming {} batches", batches.len());

            let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));

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
        let batch_stream = futures::stream::iter(vec![Ok(batch)]);

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

        // Check if we already have this cached (should always be the case after do_action_create_prepared_statement)
        let schema = if let Some(entry) = self.prepared_cache.get(&sql).await {
            info!(
                "✓ CACHE HIT in get_flight_info: Using cached schema (was_executed={}) for: {}",
                entry.was_executed, sql
            );
            entry.schema
        } else {
            // This shouldn't happen normally, but handle it gracefully
            // ⚠️ Important: Only resolve schema, never execute here (could be DDL)
            warn!(
                "✗ CACHE MISS in get_flight_info: Resolving schema only for: {}",
                sql
            );
            self.get_query_schema(&sql).await?
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

        // Check if this is a DDL statement that was already executed
        let batches = if let Some(entry) = self.prepared_cache.get(&sql).await {
            if entry.was_executed {
                info!(
                    "✓ CACHE HIT in do_get: DDL already executed, returning success batch for: {}",
                    sql
                );
                vec![self.create_success_batch()?]
            } else {
                info!(
                    "✓ CACHE HIT in do_get: SELECT query, executing now: {}",
                    sql
                );
                if sql.trim().eq_ignore_ascii_case("SELECT 1") {
                    vec![self.create_one_batch()?]
                } else {
                    self.execute_sql_batches(&sql).await?
                }
            }
        } else {
            info!(
                "✗ CACHE MISS in do_get: No cached info, executing query: {}",
                sql
            );
            if sql.trim().eq_ignore_ascii_case("SELECT 1") {
                vec![self.create_one_batch()?]
            } else {
                self.execute_sql_batches(&sql).await?
            }
        };

        // Get schema from first batch or create empty schema
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!(
            "do_get_statement: streaming {} batches with {} total rows",
            batches.len(),
            total_rows
        );

        // Stream all batches to the client
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));

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
        query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = String::from_utf8_lossy(&query.prepared_statement_handle).to_string();
        info!("do_put_prepared_statement_update: SQL = {}", sql);

        // Check if this is a DDL statement that was already executed in do_action_create_prepared_statement
        if let Some(entry) = self.prepared_cache.get(&sql).await {
            if entry.was_executed {
                info!(
                    "✓ CACHE HIT: DDL already executed in prepare phase, returning success: {}",
                    sql
                );
                // Return 0 for DDL (standard JDBC behavior - no rows affected)
                return Ok(0);
            }
        }

        // If not cached or not executed, execute now
        info!("Executing update statement: {}", sql);
        let _batch = self.execute_sql(&sql).await?;

        // Cache the result
        let schema = Arc::new(Schema::empty());
        let entry = CacheEntry::new(schema, true);
        self.prepared_cache.insert(sql.clone(), entry).await;

        // Return 0 for DDL statements (standard JDBC behavior)
        Ok(0)
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

        // Check if already in cache
        if let Some(entry) = self.prepared_cache.get(&query.query).await {
            info!(
                "✓ CACHE HIT: Using cached schema for '{}' (was_executed={})",
                query.query, entry.was_executed
            );

            // Return early with cached schema
            let handle = query.query.clone().into_bytes();
            let options = IpcWriteOptions::default();
            let dataset_schema_data = arrow_flight::IpcMessage::try_from(
                arrow_flight::SchemaAsIpc::new(&entry.schema, &options),
            )
            .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

            let empty_schema = Schema::empty();
            let param_schema_data = arrow_flight::IpcMessage::try_from(
                arrow_flight::SchemaAsIpc::new(&empty_schema, &options),
            )
            .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

            return Ok(ActionCreatePreparedStatementResult {
                prepared_statement_handle: handle.into(),
                dataset_schema: dataset_schema_data.0,
                parameter_schema: param_schema_data.0,
            });
        }

        info!(
            "✗ CACHE MISS: Processing new prepared statement for '{}'",
            query.query
        );

        let is_ddl = Self::is_ddl_statement(&query.query);
        let is_dml = Self::is_dml_statement(&query.query);

        // Strategy depends on query type:
        // - DDL: Execute now (creates table), return empty schema (DDL has no result set)
        // - DML: Execute now (INSERT/UPDATE/DELETE), return empty schema
        // - SELECT: Only resolve to get schema (don't execute yet)
        let schema = if is_ddl {
            // DDL: Execute now and cache that it's done
            info!("DDL detected, executing immediately: {}", query.query);
            let _batch = self.execute_sql(&query.query).await?;
            Arc::new(Schema::empty())
        } else if is_dml {
            // DML: Execute now (INSERT/UPDATE/DELETE)
            info!("DML detected, executing immediately: {}", query.query);
            let _batch = self.execute_sql(&query.query).await?;
            Arc::new(Schema::empty())
        } else {
            // SELECT: Only resolve plan to get schema (don't execute)
            info!(
                "SELECT detected, resolving plan for schema only: {}",
                query.query
            );
            self.get_query_schema(&query.query).await?
        };

        // Cache the schema and execution status
        // - DDL/DML: was_executed=true (don't re-execute in do_get_statement)
        // - SELECT: was_executed=false (will execute in do_get_statement)
        let was_executed = is_ddl || is_dml;
        let entry = CacheEntry::new(schema.clone(), was_executed);
        self.prepared_cache.insert(query.query.clone(), entry).await;

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
