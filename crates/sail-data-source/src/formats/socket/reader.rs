// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, exec_datafusion_err, plan_err};
use futures::stream::BoxStream;
use sail_common::runtime::spawn_stream_on;
use sail_common_datafusion::session::runtime::IoRuntime;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::source::StreamSource;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::LinesStream;

use crate::options::r#gen::SocketReadOptions;

#[derive(Debug, Clone)]
pub struct SocketStreamSource {
    options: SocketReadOptions,
    schema: SchemaRef,
}

impl SocketStreamSource {
    pub fn try_new(options: SocketReadOptions, schema: SchemaRef) -> Result<Self> {
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
    options: SocketReadOptions,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl SocketSourceExec {
    /// Creates a new execution plan for the socket source.
    /// The schema should be the original schema before projection.
    pub fn try_new(
        options: SocketReadOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(&projection)?);
        let output_schema = Arc::new(to_flow_event_schema(&projected_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Ok(Self {
            options,
            original_schema: schema,
            projected_schema,
            projection,
            properties,
        })
    }

    pub fn options(&self) -> &SocketReadOptions {
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
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
        context: Arc<TaskContext>,
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
        let chunks = futures::stream::once(async move {
            let io = IoRuntime::from_options(context.session_config().options());
            spawn_stream_on(io.0, options, read_socket_lines)
        });
        let projected_schema = self.projected_schema.clone();
        let output = futures::StreamExt::flatten(chunks).map(move |lines| {
            let lines = lines?;
            let rows = lines.len();
            let arrays = if projected_schema.fields.is_empty() {
                vec![]
            } else {
                let values = Arc::new(StringArray::from_iter_values(lines));
                vec![values as ArrayRef; projected_schema.fields.len()]
            };
            let batch = RecordBatch::try_new_with_options(
                projected_schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )?;
            Ok(FlowEvent::append_only_data(batch))
        });
        let stream = Box::pin(FlowEventStreamAdapter::new(
            self.projected_schema.clone(),
            output,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}

fn read_socket_lines(options: &SocketReadOptions) -> BoxStream<'_, Result<Vec<String>>> {
    Box::pin(async_stream::try_stream! {
        // The socket source is for testing and opens one connection per scan.
        let socket = tokio::net::TcpStream::connect((options.host.as_str(), options.port))
            .await
            .map_err(|e| exec_datafusion_err!(
                "failed to connect to socket {}:{}: {e}", options.host, options.port
            ))?;
        let reader = BufReader::new(socket);
        let chunks = LinesStream::new(reader.lines()).chunks_timeout(
            options.max_batch_size,
            Duration::from_secs(options.timeout_sec),
        );
        tokio::pin!(chunks);
        while let Some(lines) = chunks.next().await {
            yield lines.into_iter().collect::<std::io::Result<_>>()?;
        }
    })
}
