use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use uuid::Uuid;

#[derive(Clone)]
pub struct FlightSession {
    pub id: String,
    pub context: SessionContext,
}

#[derive(Clone)]
pub struct PreparedStatement {
    pub session_id: String,
    /// Re-resolve this SQL before execution: logical plans can capture table snapshots.
    pub sql: String,
    pub plan: LogicalPlan,
    pub is_query: bool,
    pub fields: Option<Vec<String>>,
    pub parameter_schema: SchemaRef,
    /// None means unbound; an empty vector means a bound empty parameter stream.
    pub parameters: Option<Vec<RecordBatch>>,
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
