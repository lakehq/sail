use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sail_catalog::command::{CatalogCommand, ShowPartitionRow};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;

/// A physical plan node that executes a [`CatalogCommand`].
///
/// This node has a single output partition and no children.
/// When executed, it delegates to [`CatalogCommand::execute()`] using the [`TaskContext`]
/// to obtain both the [`CatalogManager`] and any session-level services.
#[derive(Debug, Clone)]
pub struct CatalogCommandExec {
    command: CatalogCommand,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl CatalogCommandExec {
    pub fn new(command: CatalogCommand, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            command,
            schema,
            properties,
        }
    }

    pub fn command(&self) -> &CatalogCommand {
        &self.command
    }
}

impl DisplayAs for CatalogCommandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CatalogCommandExec: {}", self.command.name())
    }
}

impl ExecutionPlan for CatalogCommandExec {
    fn name(&self) -> &'static str {
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
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let command = self.command.clone();
        let schema = self.schema.clone();
        // Extract table name before moving command into the async block.
        let partition_table = if let CatalogCommand::ListPartitions { ref table } = command {
            Some(table.clone())
        } else {
            None
        };
        let stream = futures::stream::once(async move {
            let manager = context.extension::<CatalogManager>()?;
            let batch = command
                .execute(context.as_ref(), manager.as_ref())
                .await
                .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?;

            // For ListPartitions, enrich the (empty) result with actual partition values
            // from the filesystem. This must happen here because we need TaskContext
            // for object store access, which CatalogCommand::execute doesn't have.
            let batch = if let Some(table) = &partition_table {
                list_partition_values(&context, &manager, table).await?
            } else {
                batch
            };

            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Lists partition values from the filesystem for a Hive-partitioned table.
///
/// Walks the partition directory tree under the table's location and returns
/// partition strings like `year=2023/month=01`.
async fn list_partition_values(
    context: &TaskContext,
    manager: &CatalogManager,
    table: &[String],
) -> Result<RecordBatch> {
    let table_status = manager
        .get_table_or_view(table)
        .await
        .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?;

    let (location, partition_by) = match &table_status.kind {
        TableKind::Table {
            location: Some(loc),
            partition_by,
            ..
        } if !partition_by.is_empty() => (loc.clone(), partition_by.clone()),
        _ => {
            // Non-partitioned or no location: return empty batch.
            // The error for non-partitioned tables is already handled
            // in CatalogCommand::execute.
            let serializer = ArrowSerializer::default();
            return serializer
                .build_record_batch::<ShowPartitionRow>(&[])
                .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"));
        }
    };

    let table_url =
        datafusion::datasource::listing::ListingTableUrl::parse(&location).map_err(|e| {
            datafusion_common::exec_datafusion_err!(
                "failed to parse table location '{location}': {e}"
            )
        })?;
    let store = context.runtime_env().object_store(&table_url)?;
    let prefix = table_url.prefix().clone();

    let depth = partition_by.len();
    let mut partitions = Vec::new();
    collect_partitions(&store, &prefix, depth, String::new(), &mut partitions).await?;

    partitions.sort();
    let rows: Vec<ShowPartitionRow> = partitions
        .into_iter()
        .map(|partition| ShowPartitionRow { partition })
        .collect();

    let serializer = ArrowSerializer::default();
    serializer
        .build_record_batch(&rows)
        .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))
}

/// Recursively collects Hive partition paths by walking subdirectories.
fn collect_partitions<'a>(
    store: &'a Arc<dyn ObjectStore>,
    prefix: &'a ObjectPath,
    remaining_depth: usize,
    current_path: String,
    result: &'a mut Vec<String>,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if remaining_depth == 0 {
            if !current_path.is_empty() {
                result.push(current_path);
            }
            return Ok(());
        }

        let listing = store.list_with_delimiter(Some(prefix)).await.map_err(|e| {
            datafusion_common::exec_datafusion_err!("failed to list partition directories: {e}")
        })?;

        for dir in &listing.common_prefixes {
            let dir_name = dir
                .filename()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!(
                        "failed to extract directory name from path: {dir}"
                    )
                })?
                .to_string();

            // Only include directories that look like Hive partitions (key=value)
            if !dir_name.contains('=') {
                continue;
            }

            let next_path = if current_path.is_empty() {
                dir_name
            } else {
                format!("{current_path}/{dir_name}")
            };

            collect_partitions(store, dir, remaining_depth - 1, next_path, result).await?;
        }

        Ok(())
    })
}
