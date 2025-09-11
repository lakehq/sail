mod options;
mod reader;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::source::StreamSourceTableProvider;

use crate::formats::socket::options::resolve_socket_read_options;
pub use crate::formats::socket::options::TableSocketOptions;
pub use crate::formats::socket::reader::SocketSourceExec;
use crate::formats::socket::reader::SocketStreamSource;

/// Read test data from a TCP socket for testing purposes.
/// The record batches contain a single string column corresponding to lines read from the socket.
#[derive(Debug)]
pub struct SocketTableFormat;

#[async_trait]
impl TableFormat for SocketTableFormat {
    fn name(&self) -> &str {
        "socket"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths: _,
            schema,
            constraints,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;
        if !constraints.deref().is_empty() {
            return plan_err!("the socket table format does not support constraints");
        }
        if !partition_by.is_empty() {
            return plan_err!("the socket table format does not support partitioning");
        }
        if bucket_by.is_some() || !sort_order.is_empty() {
            return plan_err!("the socket table format does not support bucketing");
        }
        let schema = match schema {
            Some(schema) if !schema.fields.is_empty() => schema,
            _ => Schema::new(vec![Arc::new(Field::new("value", DataType::Utf8, false))]),
        };
        let options = resolve_socket_read_options(options)?;
        let source = SocketStreamSource::try_new(options, Arc::new(schema))?;
        Ok(Arc::new(StreamSourceTableProvider::new(Arc::new(source))))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("socket table format writer")
    }
}
