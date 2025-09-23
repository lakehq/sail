use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, plan_err, DataFusionError, Result};
use futures::TryStreamExt;
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

use crate::formats::socket::options::TableSocketOptions;

#[derive(Debug, Clone)]
pub struct SocketStreamSource {
    options: TableSocketOptions,
    schema: SchemaRef,
}

impl SocketStreamSource {
    pub fn try_new(options: TableSocketOptions, schema: SchemaRef) -> Result<Self> {
        Self::validate_schema(&schema)?;
        Ok(Self { options, schema })
    }

    fn validate_schema(schema: &Schema) -> Result<()> {
        match schema.fields.iter().as_slice() {
            [value] => {
                if !matches!(value.data_type(), DataType::Utf8) {
                    plan_err!("invalid value type for socket table")
                } else {
                    Ok(())
                }
            }
            _ => {
                plan_err!("invalid schema for socket table")
            }
        }
    }
}

#[async_trait]
impl StreamSource for SocketStreamSource {
    fn data_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields.len()).collect());
        Ok(Arc::new(SocketSourceExec::try_new(
            self.options.clone(),
            Arc::clone(&self.schema),
            projection,
        )?))
    }
}

#[derive(Debug)]
pub struct SocketSourceExec {
    options: TableSocketOptions,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    properties: PlanProperties,
}

impl SocketSourceExec {
    /// Creates a new execution plan for the socket source.
    /// The schema should be the original schema before projection.
    pub fn try_new(
        options: TableSocketOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(&projection)?);
        let output_schema = Arc::new(to_flow_event_schema(&projected_schema));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Ok(Self {
            options,
            original_schema: schema,
            projected_schema,
            projection,
            properties,
        })
    }

    pub fn options(&self) -> &TableSocketOptions {
        &self.options
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn projection(&self) -> &[usize] {
        &self.projection
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
            "{}: host={}, port={}",
            self.name(),
            self.options.host,
            self.options.port
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
        if partition != 0 {
            return plan_err!("{} only supports a single partition", self.name());
        }
        if self.options.host.is_empty() {
            return plan_err!("host is required for reading from socket");
        }
        if self.options.port == 0 {
            return plan_err!("port must be greater than 0 for reading from socket");
        }
        if self.options.max_batch_size == 0 {
            return plan_err!("maximum batch size must be greater than 0 for reading from socket");
        }
        if self.options.timeout_sec == 0 {
            return plan_err!("timeout must be greater than 0 for reading from socket");
        }
        let options = self.options.clone();
        let projected_schema = self.projected_schema.clone();
        let output = futures::stream::once(async move {
            // We do not perform retries or reconnections here
            // since the socket source is for testing purposes only.
            let stream = tokio::net::TcpStream::connect((options.host.as_str(), options.port))
                .await
                .map_err(|e| {
                    exec_datafusion_err!(
                        "failed to connect to socket {}:{}: {e}",
                        options.host,
                        options.port
                    )
                })?;
            let reader = BufReader::new(stream);
            let output = LinesStream::new(reader.lines())
                .chunks_timeout(
                    options.max_batch_size,
                    Duration::from_secs(options.timeout_sec),
                )
                .map(move |lines| {
                    let values = lines
                        .into_iter()
                        .map(|x| Ok(Some(x?)))
                        .collect::<Result<Vec<Option<String>>>>()?;
                    let array = Arc::new(StringArray::from(values));
                    Ok::<_, DataFusionError>(RecordBatch::try_new(
                        projected_schema.clone(),
                        vec![array; projected_schema.fields.len()],
                    )?)
                });
            Ok::<_, DataFusionError>(Box::pin(output))
        })
        .try_flatten();
        let output = output.map(|x| Ok(FlowEvent::append_only_data(x?)));
        let stream = Box::pin(FlowEventStreamAdapter::new(
            self.projected_schema.clone(),
            output,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}
