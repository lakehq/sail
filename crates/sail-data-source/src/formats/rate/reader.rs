use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{arrow_datafusion_err, plan_err, Result};
use futures::{Stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;

use crate::formats::rate::options::TableRateOptions;

#[derive(Debug, Clone)]
pub struct RateStreamSource {
    options: TableRateOptions,
    schema: SchemaRef,
}

impl RateStreamSource {
    pub fn try_new(options: TableRateOptions, schema: SchemaRef) -> Result<Self> {
        Self::validate_schema(&schema)?;
        Ok(Self { options, schema })
    }

    fn validate_schema(schema: &Schema) -> Result<()> {
        match schema.fields.iter().as_slice() {
            [t, v] => {
                if !matches!(
                    t.data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(_tz))
                ) {
                    plan_err!("invalid timestamp type for rate table")
                } else if !matches!(v.data_type(), DataType::Int64) {
                    plan_err!("invalid value type for rate table")
                } else {
                    Ok(())
                }
            }
            _ => {
                plan_err!("invalid schema for rate table")
            }
        }
    }
}

#[async_trait]
impl StreamSource for RateStreamSource {
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
        Ok(Arc::new(RateSourceExec::try_new(
            self.options.clone(),
            Arc::clone(&self.schema),
            projection,
        )?))
    }
}

#[derive(Debug)]
pub struct RateSourceExec {
    options: TableRateOptions,
    time_zone: Arc<str>,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    properties: PlanProperties,
}

impl RateSourceExec {
    /// Creates a new execution plan for the rate source.
    /// The schema should be the original schema before projection.
    pub fn try_new(
        options: TableRateOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let time_zone = Self::infer_time_zone(&schema)?;
        let projected_schema = Arc::new(schema.project(&projection)?);
        let output_schema = Arc::new(to_flow_event_schema(&projected_schema));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(options.num_partitions),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Ok(Self {
            options,
            time_zone,
            original_schema: schema,
            projected_schema,
            projection,
            properties,
        })
    }

    pub fn options(&self) -> &TableRateOptions {
        &self.options
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    fn infer_time_zone(schema: &Schema) -> Result<Arc<str>> {
        match schema.fields.iter().as_slice() {
            [t, _] => {
                if let DataType::Timestamp(_, Some(tz)) = t.data_type() {
                    Ok(Arc::clone(tz))
                } else {
                    plan_err!("invalid timestamp type for rate table schema")
                }
            }
            _ => {
                plan_err!("invalid schema for rate table")
            }
        }
    }
}

impl DisplayAs for RateSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}: rows_per_second={}, num_partitions={}",
            self.name(),
            self.options.rows_per_second,
            self.options.num_partitions
        )
    }
}

impl ExecutionPlan for RateSourceExec {
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
        if partition >= self.options.num_partitions {
            return plan_err!(
                "invalid partition {} for {} with {} partition(s)",
                partition,
                self.name(),
                self.options.num_partitions
            );
        }
        // TODO: consider token bucket algorithm for data generation with a more stable rate
        // TODO: make the data generation algorithm configurable
        let output: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            if self.options.rows_per_second == 0 {
                let output = futures::stream::unfold((), |()| async move {
                    tokio::time::sleep(Duration::MAX).await;
                    None
                });
                Box::pin(output)
            } else {
                let rows_per_second =
                    (self.options.rows_per_second / self.options.num_partitions).max(1);
                // We generate at most 1000 batches per second
                // since the sleep function only has millisecond accuracy.
                let batches_per_second = rows_per_second.min(1_000);
                let batch_size = rows_per_second / batches_per_second;
                let interval = Duration::from_secs(1) / (batches_per_second as u32);
                let generator = BatchGenerator::try_new(
                    Arc::clone(&self.time_zone),
                    &self.projection,
                    self.projected_schema.clone(),
                )?;
                let output = futures::stream::unfold(generator, move |mut generator| async move {
                    // The interval does not take into account the time it takes to generate data,
                    // but the sleep itself is inaccurate anyway.
                    tokio::time::sleep(interval).await;
                    let result = generator.generate(batch_size);
                    Some((result, generator))
                });
                Box::pin(output)
            };
        let output = output.map(|x| Ok(FlowEvent::append_only_data(x?)));
        let stream = Box::pin(FlowEventStreamAdapter::new(
            self.projected_schema.clone(),
            output,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}

/// The action for generating each column in the record batch.
enum BatchGeneratorAction {
    /// Generates a timestamp array.
    Timestamp,
    /// Generates a value array.
    Value,
    /// Copies a previously generated array.
    Copy(usize),
}

struct BatchGenerator {
    offset: usize,
    projected_schema: SchemaRef,
    time_zone: Arc<str>,
    actions: Vec<BatchGeneratorAction>,
}

impl BatchGenerator {
    fn try_new(
        time_zone: Arc<str>,
        projection: &[usize],
        projected_schema: SchemaRef,
    ) -> Result<Self> {
        let mut actions = vec![];
        let mut timestamp_index = None;
        let mut value_index = None;
        for i in projection {
            match i {
                0 => {
                    if let Some(j) = timestamp_index {
                        actions.push(BatchGeneratorAction::Copy(j));
                    } else {
                        timestamp_index = Some(actions.len());
                        actions.push(BatchGeneratorAction::Timestamp);
                    }
                }
                1 => {
                    if let Some(j) = value_index {
                        actions.push(BatchGeneratorAction::Copy(j));
                    } else {
                        value_index = Some(actions.len());
                        actions.push(BatchGeneratorAction::Value);
                    }
                }
                _ => {
                    return plan_err!("invalid projection index {i} for rate source table");
                }
            }
        }
        Ok(Self {
            offset: 0,
            projected_schema,
            time_zone,
            actions,
        })
    }

    fn generate(&mut self, batch_size: usize) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.actions.len());
        for action in &self.actions {
            match action {
                BatchGeneratorAction::Timestamp => {
                    let ts = chrono::Utc::now().timestamp_micros();
                    let array = TimestampMicrosecondArray::from(vec![ts; batch_size])
                        .with_timezone(Arc::clone(&self.time_zone));
                    columns.push(Arc::new(array) as _);
                }
                BatchGeneratorAction::Value => {
                    let values = (0..batch_size)
                        .map(|i| (self.offset + i) as i64)
                        .collect::<Vec<_>>();
                    let array = Int64Array::from(values);
                    columns.push(Arc::new(array) as _);
                }
                BatchGeneratorAction::Copy(index) => {
                    columns.push(columns[*index].clone());
                }
            }
        }
        self.offset += batch_size;
        RecordBatch::try_new(self.projected_schema.clone(), columns)
            .map_err(|e| arrow_datafusion_err!(e))
    }
}
