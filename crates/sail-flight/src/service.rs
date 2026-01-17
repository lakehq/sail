use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, Float64Array, StringArray};
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

pub struct SailFlightSqlService {
    ctx: Arc<RwLock<SessionContext>>,
    config: Arc<PlanConfig>,
}

impl SailFlightSqlService {
    pub fn new() -> Self {
        let ctx = create_sail_session_context();
        let config = Arc::new(PlanConfig::default());
        SailFlightSqlService {
            ctx: Arc::new(RwLock::new(ctx)),
            config,
        }
    }

    /// Execute SQL using Sail's full pipeline (parser + resolver + executor)
    async fn execute_sql(&self, query: &str) -> Result<RecordBatch, Status> {
        println!("Executing SQL (Sail): {}", query);

        // Step 1: Parse SQL to AST using Sail's parser
        let statement = parse_one_statement(query)
            .map_err(|e| Status::internal(format!("Parse error: {}", e)))?;

        // Step 2: Convert AST to spec::Plan
        let plan = from_ast_statement(statement)
            .map_err(|e| Status::internal(format!("AST conversion error: {}", e)))?;

        // Step 3: Resolve the plan using PlanResolver
        let ctx = self.ctx.read().await;
        let resolver = PlanResolver::new(&ctx, self.config.clone());
        let NamedPlan { plan: logical_plan, fields: _ } = resolver
            .resolve_named_plan(plan)
            .await
            .map_err(|e| Status::internal(format!("Plan resolution error: {}", e)))?;

        // Step 4: Execute the logical plan
        let df = execute_logical_plan(&ctx, logical_plan)
            .await
            .map_err(|e| Status::internal(format!("Execution error: {}", e)))?;

        // Step 5: Collect results
        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("Collection error: {}", e)))?;

        if batches.is_empty() {
            return self.create_success_batch();
        }

        Ok(batches.into_iter().next().unwrap_or_else(|| {
            self.create_success_batch()
                .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())))
        }))
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
        println!("Handshake received");
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let stream = stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        println!("get_flight_info_statement: SQL = {}", sql);

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

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8_lossy(&ticket.statement_handle).to_string();
        println!("do_get_statement: SQL = {}", sql);

        let batch = if sql.trim().eq_ignore_ascii_case("SELECT 1") {
            self.create_one_batch()?
        } else {
            self.execute_sql(&sql).await?
        };

        println!("do_get_statement: returning {} rows", batch.num_rows());

        let schema = batch.schema();
        let batches = vec![Ok(batch)];
        let batch_stream = futures::stream::iter(batches);

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ticket_data = request.into_inner().ticket;
        println!("do_get_fallback: ticket_data len = {}", ticket_data.len());

        if let Ok(ticket) = TicketStatementQuery::decode(ticket_data.as_ref()) {
            let sql = String::from_utf8_lossy(&ticket.statement_handle).to_string();
            println!("do_get_fallback: decoded SQL = {}", sql);

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

        println!("do_get_fallback: using demo data");
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

    // Unimplemented methods
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
        println!("get_flight_info_prepared_statement: SQL = {}", sql);

        // Create ticket for do_get
        let ticket = TicketStatementQuery {
            statement_handle: cmd.prepared_statement_handle.clone(),
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

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("do_put_substrait_plan"))
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

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        println!("do_action_create_prepared_statement: SQL = {}", query.query);

        // Execute the query to get schema
        let batch = self.execute_sql(&query.query).await?;
        let schema = batch.schema();

        // Use the query as the prepared statement handle
        let handle = query.query.clone().into_bytes();

        // Convert schema to IPC bytes
        let options = IpcWriteOptions::default();
        let dataset_schema_data = arrow_flight::IpcMessage::try_from(
            arrow_flight::SchemaAsIpc::new(&schema, &options)
        ).map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

        let empty_schema = Schema::empty();
        let param_schema_data = arrow_flight::IpcMessage::try_from(
            arrow_flight::SchemaAsIpc::new(&empty_schema, &options)
        ).map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

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
        println!("do_action_close_prepared_statement: SQL = {}", sql);
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
