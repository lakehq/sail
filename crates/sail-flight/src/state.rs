use std::collections::HashMap;

use datafusion::execution::SendableRecordBatchStream;
use tonic::Status;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryTicket(String);

impl QueryTicket {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Default for QueryTicket {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&[u8]> for QueryTicket {
    type Error = Status;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let s = std::str::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("invalid ticket encoding"))?;
        Ok(Self(s.to_string()))
    }
}

pub struct SailFlightSqlState {
    streams: HashMap<QueryTicket, SendableRecordBatchStream>,
}

impl SailFlightSqlState {
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    pub fn insert(&mut self, ticket: QueryTicket, stream: SendableRecordBatchStream) {
        self.streams.insert(ticket, stream);
    }

    pub fn take(&mut self, ticket: &QueryTicket) -> Option<SendableRecordBatchStream> {
        self.streams.remove(ticket)
    }
}

impl Default for SailFlightSqlState {
    fn default() -> Self {
        Self::new()
    }
}
