use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, internal_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::future::try_join_all;
use futures::TryStreamExt;
use log::warn;

use crate::plan::write_list_of_lists;
use crate::stream::{MergedRecordBatchStream, TaskReadLocation, TaskStreamReader};

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    /// The stage to read from.
    stage: usize,
    /// For each output partition, a list of locations to read from.
    locations: Vec<Vec<TaskReadLocation>>,
    properties: PlanProperties,
    reader: Option<Arc<dyn TaskStreamReader>>,
}

impl ShuffleReadExec {
    pub fn new(stage: usize, schema: SchemaRef, partitioning: Partitioning) -> Self {
        let partitioning = match partitioning {
            Partitioning::Hash(expr, n) if expr.is_empty() => Partitioning::UnknownPartitioning(n),
            Partitioning::Hash(expr, n) => {
                // https://github.com/apache/arrow-datafusion/issues/5184
                Partitioning::Hash(
                    expr.into_iter()
                        .filter(|e| e.as_any().downcast_ref::<UnKnownColumn>().is_none())
                        .collect(),
                    n,
                )
            }
            _ => partitioning,
        };
        let partition_count = partitioning.partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            partitioning,
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: true,
            },
        );
        Self {
            stage,
            locations: vec![vec![]; partition_count],
            properties,
            reader: None,
        }
    }

    pub fn stage(&self) -> usize {
        self.stage
    }

    pub fn partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    pub fn locations(&self) -> &[Vec<TaskReadLocation>] {
        &self.locations
    }

    pub fn with_locations(self, locations: Vec<Vec<TaskReadLocation>>) -> Self {
        Self { locations, ..self }
    }

    pub fn with_reader(self, reader: Option<Arc<dyn TaskStreamReader>>) -> Self {
        Self { reader, ..self }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReadExec: stage={}, partitioning={}, locations=",
            self.stage,
            self.properties.output_partitioning()
        )?;
        write_list_of_lists(f, &self.locations)
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
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
            return internal_err!("ShuffleReadExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let locations = self
            .locations
            .get(partition)
            .ok_or_else(|| {
                exec_datafusion_err!("read locations for partition {partition} not found")
            })?
            .clone();
        if locations.is_empty() {
            let stage = self.stage;
            warn!("empty read locations for stage {stage} partition {partition}");
        }
        let reader = self
            .reader
            .as_ref()
            .ok_or_else(|| exec_datafusion_err!("reader not set for ShuffleReadExec"))?
            .clone();
        let output_schema = self.schema();
        let output =
            futures::stream::once(
                async move { shuffle_read(reader, &locations, output_schema).await },
            )
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

async fn shuffle_read(
    reader: Arc<dyn TaskStreamReader>,
    locations: &[TaskReadLocation],
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let futures = locations
        .iter()
        .map(|location| reader.open(location, schema.clone()));
    let streams = try_join_all(futures).await?;
    Ok(Box::pin(MergedRecordBatchStream::new(schema, streams)))
}
