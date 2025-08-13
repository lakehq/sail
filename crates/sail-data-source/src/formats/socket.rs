use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, not_impl_err, plan_err, DataFusionError, Result};
use deltalake::arrow::datatypes::{Field, Schema};
use futures::TryStreamExt;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

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
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(SocketTableProvider::new(
            "localhost".to_string(),
            9999,
        )))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("socket table format writer")
    }
}

#[derive(Debug, Clone)]
struct SocketTableProvider {
    host: String,
    port: u16,
    schema: SchemaRef,
}

impl SocketTableProvider {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            schema: Arc::new(Schema::new(vec![Arc::new(Field::new(
                "value",
                DataType::Utf8,
                false,
            ))])),
        }
    }
}

#[async_trait]
impl TableProvider for SocketTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SocketSourceExec::new(
            self.host.clone(),
            self.port,
            Arc::clone(&self.schema),
        )))
    }
}

#[derive(Debug)]
struct SocketSourceExec {
    host: String,
    port: u16,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SocketSourceExec {
    pub fn new(host: String, port: u16, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            host,
            port,
            schema,
            properties,
        }
    }
}

impl DisplayAs for SocketSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}: host={}, port={})",
            self.name(),
            self.host,
            self.port
        )
    }
}

impl ExecutionPlan for SocketSourceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            plan_err!("{} cannot have children", self.name())
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        const BATCH_SIZE: usize = 1024;
        const TIMEOUT_DURATION: Duration = Duration::from_secs(1);

        if partition != 0 {
            return plan_err!("{} only supports a single partition", self.name());
        }

        let host = self.host.clone();
        let port = self.port;
        let schema = Arc::clone(&self.schema);
        let output = futures::stream::once(async move {
            let stream = tokio::net::TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|e| exec_datafusion_err!("failed to connect to socket: {e}"))?;
            let reader = BufReader::new(stream);
            let output = LinesStream::new(reader.lines())
                .chunks_timeout(BATCH_SIZE, TIMEOUT_DURATION)
                .map(move |lines| {
                    let values = lines
                        .into_iter()
                        .map(|x| Ok(Some(x?)))
                        .collect::<Result<Vec<Option<String>>>>()?;
                    let array = Arc::new(StringArray::from(values));
                    Ok(RecordBatch::try_new(schema.clone(), vec![array])?)
                });
            Ok::<_, DataFusionError>(Box::pin(output))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
