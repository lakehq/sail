use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, stream};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::session::job::JobService;
use sail_plan::execute_named_plan;
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::plan::NamedPlan;
use sail_sql_analyzer::parser::{parse_one_statement, rewrite_positional_parameter_markers};
use sail_sql_analyzer::statement::from_ast_statement;
use tonic::Status;

use crate::metrics::{MetricsRecordingContext, MetricsRecordingStream, StatementType};
use crate::service::SailFlightSqlService;
use crate::state::{FlightSession, PreparedStatement};

pub fn invalid(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}

pub fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

pub fn dataset_schema(statement: &PreparedStatement) -> Result<SchemaRef, Status> {
    match &statement.fields {
        Some(fields) => rename_schema(statement.plan.schema().as_arrow(), fields).map_err(internal),
        None => Ok(Arc::new(Schema::empty())),
    }
}

impl SailFlightSqlService {
    pub(super) async fn prepare(
        &self,
        session: &FlightSession,
        sql: &str,
    ) -> Result<PreparedStatement, Status> {
        let (sql, _) = rewrite_positional_parameter_markers(sql).map_err(invalid)?;
        let plan =
            from_ast_statement(parse_one_statement(&sql).map_err(invalid)?).map_err(invalid)?;
        let is_query = matches!(&plan, spec::Plan::Query(_));
        // Resolving the logical plan must not execute catalog commands or SET.
        let returns_rows = matches!(&plan, spec::Plan::Command(command) if matches!(&command.node,
            spec::CommandNode::ShowString(_)
            | spec::CommandNode::CallProcedure { .. }
            | spec::CommandNode::CurrentDatabase
            | spec::CommandNode::CurrentCatalog
            | spec::CommandNode::ListDatabases { .. }
            | spec::CommandNode::ListCatalogs { .. }
            | spec::CommandNode::ShowTableProperties { .. }
            | spec::CommandNode::ShowTables { .. }
            | spec::CommandNode::ShowTableExtended { .. }
            | spec::CommandNode::ShowFunctions { .. }
            | spec::CommandNode::ListTables { .. }
            | spec::CommandNode::ListViews { .. }
            | spec::CommandNode::ListFunctions { .. }
            | spec::CommandNode::ListColumns { .. }
            | spec::CommandNode::GetDatabase { .. }
            | spec::CommandNode::GetTable { .. }
            | spec::CommandNode::GetFunction { .. }
            | spec::CommandNode::DatabaseExists { .. }
            | spec::CommandNode::TableExists { .. }
            | spec::CommandNode::FunctionExists { .. }
            | spec::CommandNode::Explain { .. }
            | spec::CommandNode::DescribeQuery { .. }
            | spec::CommandNode::DescribeFunction { .. }
            | spec::CommandNode::DescribeCatalog { .. }
            | spec::CommandNode::DescribeDatabase { .. }
            | spec::CommandNode::DescribeTable { .. }
        ));
        let mut named = PlanResolver::new(&session.context, self.config.clone())
            .resolve_named_plan(plan)
            .await
            .map_err(invalid)?;
        // SHOW/DESCRIBE are command plans in Sail, but have user-facing result
        // sets. DDL success flags are internal and must not become result sets.
        if returns_rows {
            named.fields = Some(
                named
                    .plan
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
            );
        }
        let types = named.plan.get_parameter_types().map_err(invalid)?;
        let mut parameters = Vec::new();
        for (id, data_type) in types {
            let index = id
                .strip_prefix('$')
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| {
                    Status::invalid_argument("Flight SQL requires positional parameters")
                })?;
            parameters.push((index, data_type.unwrap_or(DataType::Null)));
        }
        parameters.sort_by_key(|(index, _)| *index);
        if parameters.iter().enumerate().any(|(i, (n, _))| i + 1 != *n) {
            return Err(Status::invalid_argument(
                "parameter positions must be contiguous starting at 1",
            ));
        }
        let parameter_schema = Arc::new(Schema::new(
            parameters
                .into_iter()
                .map(|(index, data_type)| Field::new(index.to_string(), data_type, true))
                .collect::<Vec<_>>(),
        ));
        Ok(PreparedStatement {
            session_id: session.id.clone(),
            sql,
            plan: named.plan,
            is_query,
            fields: named.fields,
            parameter_schema,
            parameters: None,
        })
    }

    pub(super) async fn refresh_statement(
        &self,
        session: &FlightSession,
        statement: PreparedStatement,
    ) -> Result<PreparedStatement, Status> {
        // Lakehouse sources and write plans capture snapshots and base metadata
        // during resolution. Cloning a prepared logical plan would reuse those
        // across executions, even after this session has committed new data.
        let mut refreshed = self.prepare(session, &statement.sql).await?;
        refreshed.parameters = statement.parameters;
        Ok(refreshed)
    }

    pub(super) fn bound_plans(statement: &PreparedStatement) -> Result<Vec<LogicalPlan>, Status> {
        let count = statement.parameter_schema.fields().len();
        if count == 0 {
            return Ok(vec![statement.plan.clone()]);
        }
        let batches = statement.parameters.as_ref().ok_or_else(|| {
            Status::invalid_argument("prepared statement parameters have not been bound")
        })?;
        let mut plans = Vec::new();
        for batch in batches {
            if batch.num_columns() != count {
                return Err(Status::invalid_argument(
                    "incorrect number of prepared statement parameters",
                ));
            }
            for row in 0..batch.num_rows() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, row))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(invalid)?;
                plans.push(
                    statement
                        .plan
                        .clone()
                        .with_param_values(values)
                        .map_err(invalid)?,
                );
            }
        }
        Ok(plans)
    }

    pub(super) async fn execute_plan(
        &self,
        session: &FlightSession,
        plan: LogicalPlan,
        fields: Option<&Vec<String>>,
    ) -> Result<SendableRecordBatchStream, Status> {
        let (plan, _) = execute_named_plan(
            &session.context,
            NamedPlan {
                plan,
                fields: fields.cloned(),
            },
        )
        .await
        .map_err(invalid)?;
        let stream = session
            .context
            .extension::<JobService>()
            .map_err(internal)?
            .runner()
            .execute(&session.context, plan)
            .await
            .map_err(internal)?;
        let stream: SendableRecordBatchStream = if let Some(metrics) = &self.metrics {
            Box::pin(MetricsRecordingStream::new(
                stream,
                metrics.clone(),
                MetricsRecordingContext {
                    session_id: session.id.clone(),
                    statement_type: if fields.is_some() {
                        StatementType::Query
                    } else {
                        StatementType::Command
                    },
                },
            ))
        } else {
            stream
        };
        if fields.is_some() {
            Ok(stream)
        } else {
            // Commands must finish even when the client never calls DoGet.
            stream.try_collect::<Vec<_>>().await.map_err(internal)?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )))
        }
    }

    pub(super) async fn execute_statement(
        &self,
        session: &FlightSession,
        statement: &PreparedStatement,
    ) -> Result<SendableRecordBatchStream, Status> {
        let plans = Self::bound_plans(statement)?;
        if plans.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                dataset_schema(statement)?,
                stream::empty(),
            )));
        }
        if plans.len() > 1 && !statement.is_query {
            // Separate command executions would commit earlier parameter sets
            // before a later failure. No backend transaction spans these plans.
            return Err(Status::unimplemented(
                "atomic execution of multiple parameter sets for commands is not supported",
            ));
        }
        let mut plans = plans.into_iter();
        let first = plans
            .next()
            .ok_or_else(|| Status::internal("missing bound plan"))?;
        let mut plan = LogicalPlanBuilder::from(first);
        for input in plans {
            plan = plan.union(input).map_err(invalid)?;
        }
        self.execute_plan(
            session,
            plan.build().map_err(invalid)?,
            statement.fields.as_ref(),
        )
        .await
    }

    pub(super) async fn execute_update(
        &self,
        session: &FlightSession,
        statement: &PreparedStatement,
    ) -> Result<i64, Status> {
        if statement.fields.is_some() {
            return Err(Status::invalid_argument(
                "update request cannot execute a query that returns rows",
            ));
        }
        let mut stream = self.execute_statement(session, statement).await?;
        while let Some(batch) = stream.next().await {
            batch.map_err(internal)?;
        }
        // Sail's command plans do not consistently expose an affected-row count.
        // Flight SQL explicitly permits -1 for an unknown count.
        Ok(-1)
    }
}
