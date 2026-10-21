use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_plan::explain::ExplainOptions;
use uuid::Uuid;

#[derive(Clone)]
pub struct FlightSession {
    pub id: String,
    pub context: SessionContext,
}

impl FlightSession {
    pub fn with_catalog(&self, catalog: Arc<CatalogManager>) -> Self {
        let mut state = self.context.state();
        state.config_mut().set_extension(catalog);
        Self {
            id: self.id.clone(),
            context: SessionContext::new_with_state(state),
        }
    }
}

#[derive(Clone)]
pub struct PreparedStatement {
    pub session_id: String,
    /// Re-resolve this SQL before execution: logical plans can capture table snapshots.
    pub sql: String,
    /// Keep preparation-time name resolution while refreshing live table metadata.
    pub catalog: Arc<CatalogManager>,
    pub plan: LogicalPlan,
    /// Bind command parameters before resolution can hide their expressions.
    pub unresolved_plan: spec::Plan,
    pub is_query: bool,
    pub mutates_session: bool,
    /// Explain the input plan from the innermost EXPLAIN outwards, at execution.
    pub explain: Vec<PreparedExplain>,
    pub fields: Option<Vec<String>>,
    pub parameter_schema: SchemaRef,
    /// None means unbound; a binding may contain no parameter rows.
    pub parameters: Option<BoundParameters>,
}

#[derive(Clone)]
pub struct PreparedExplain {
    pub options: ExplainOptions,
    pub fields: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct BoundParameters {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

pub struct QueryResult {
    pub session_id: String,
    pub stream: SendableRecordBatchStream,
}

#[derive(Default)]
pub struct SailFlightSqlState {
    pub sessions: HashMap<String, String>,
    pub statements: HashMap<String, PreparedStatement>,
    pub results: HashMap<String, QueryResult>,
}

impl SailFlightSqlState {
    pub fn remove_session(&mut self, id: &str) {
        self.sessions.remove(id);
        self.statements
            .retain(|_, statement| statement.session_id != id);
        self.results.retain(|_, result| result.session_id != id);
    }
}

pub fn new_handle() -> String {
    Uuid::new_v4().to_string()
}
