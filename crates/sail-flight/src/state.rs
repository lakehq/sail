use std::collections::HashMap;
use std::fmt;

use datafusion::execution::SendableRecordBatchStream;
use tonic::Status;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryHandle(String);

impl QueryHandle {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for QueryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Default for QueryHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&[u8]> for QueryHandle {
    type Error = Status;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let s = std::str::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("invalid ticket encoding"))?;
        Ok(Self(s.to_string()))
    }
}

pub struct SailFlightSqlState {
    streams: HashMap<QueryHandle, SendableRecordBatchStream>,
}

impl SailFlightSqlState {
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    pub fn insert(&mut self, ticket: QueryHandle, stream: SendableRecordBatchStream) {
        self.streams.insert(ticket, stream);
    }

    pub fn take(&mut self, ticket: &QueryHandle) -> Option<SendableRecordBatchStream> {
        self.streams.remove(ticket)
    }
}

impl Default for SailFlightSqlState {
    fn default() -> Self {
        Self::new()
    }
}
