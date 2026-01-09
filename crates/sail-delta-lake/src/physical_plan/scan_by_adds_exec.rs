// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{self, TryStreamExt};
use url::Url;

use crate::datasource::scan::FileScanParams;
use crate::datasource::{build_file_scan_config, DeltaScanConfigBuilder};
use crate::physical_plan::{decode_adds_from_batch, COL_ACTION};
use crate::storage::StorageConfig;
use crate::table::open_table_with_object_store;

/// Physical execution node that scans Delta data files based on Add actions from upstream.
///
/// This node bridges the metadata layer (Add actions) with the data layer (Parquet scans).
/// It consumes a stream of encoded Add actions and produces a stream of data records by
/// scanning the referenced files.
#[derive(Debug, Clone)]
pub struct DeltaScanByAddsExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    table_schema: SchemaRef,
    cache: PlanProperties,
}

impl DeltaScanByAddsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url, table_schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(
            table_schema.clone(),
            input.output_partitioning().partition_count(),
        );
        Self {
            input,
            table_url,
            table_schema,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    fn compute_properties(schema: SchemaRef, partition_count: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }
}

#[async_trait]
impl ExecutionPlan for DeltaScanByAddsExec {
    fn name(&self) -> &'static str {
        "DeltaScanByAddsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaScanByAddsExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.table_url.clone(),
            self.table_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let table_url = self.table_url.clone();
        let table_schema = self.table_schema.clone();
        let output_schema = self.schema();
        let output_schema_clone = output_schema.clone();

        let stream_fut = async move {
            let mut input_stream = input.execute(partition, context.clone())?;
            let mut candidate_adds = Vec::new();
            let mut is_partition_scan = true;

            while let Some(batch) = input_stream.try_next().await? {
                if batch.num_rows() == 0 {
                    continue;
                }

                // Check if this is a partition-only scan (no data scan needed)
                if let Some(scan_col) = batch.column_by_name("partition_scan") {
                    let scan_array = scan_col
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "partition_scan column is not a BooleanArray".to_string(),
                            )
                        })?;
                    is_partition_scan = scan_array.value(0);
                } else {
                    is_partition_scan = false;
                }

                // Decode Add actions from the action column
                if batch.column_by_name(COL_ACTION).is_some() {
                    candidate_adds.extend(decode_adds_from_batch(&batch)?);
                } else {
                    return Err(DataFusionError::Plan(
                        "DeltaScanByAddsExec input must contain 'action' column".to_string(),
                    ));
                }
            }

            if is_partition_scan || candidate_adds.is_empty() {
                let empty_batch = RecordBatch::new_empty(output_schema_clone.clone());
                let stream = stream::once(async { Ok(empty_batch) });
                return Ok(
                    Box::pin(RecordBatchStreamAdapter::new(output_schema_clone, stream))
                        as SendableRecordBatchStream,
                );
            }

            let object_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let table = open_table_with_object_store(table_url, object_store, StorageConfig)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let snapshot = table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .clone();

            let scan_config = DeltaScanConfigBuilder::new()
                .with_schema(table_schema.clone())
                .build(&snapshot)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let session_state = SessionStateBuilder::new()
                .with_runtime_env(context.runtime_env().clone())
                .build();

            let table_partition_cols = snapshot.metadata().partition_columns();
            let file_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
                table_schema
                    .fields()
                    .iter()
                    .filter(|f| !table_partition_cols.contains(f.name()))
                    .cloned()
                    .collect::<Vec<_>>(),
            ));

            let log_store = table.log_store();
            let file_scan_config = build_file_scan_config(
                &snapshot,
                &log_store,
                &candidate_adds,
                &scan_config,
                FileScanParams {
                    pruning_mask: None,
                    projection: None,
                    limit: None,
                    pushdown_filter: None,
                },
                &session_state,
                file_schema,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // `build_file_scan_config` groups files by partition values which can yield multiple
            // `file_groups` (i.e. scan partitions). This exec is driven by upstream partitioning,
            // so coalesce the inner scan into a single partition to avoid dropping data when
            // executing `partition=0` below.
            let mut file_scan_config = file_scan_config;
            if file_scan_config.file_groups.len() > 1 {
                let merged = file_scan_config
                    .file_groups
                    .into_iter()
                    .flat_map(|group| group.into_inner())
                    .collect::<Vec<_>>();
                file_scan_config.file_groups =
                    vec![datafusion::datasource::physical_plan::FileGroup::new(
                        merged,
                    )];
            }

            let scan_exec =
                datafusion::datasource::source::DataSourceExec::from_data_source(file_scan_config);
            scan_exec.execute(0, context)
        };

        let stream = stream::once(stream_fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

impl DisplayAs for DeltaScanByAddsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaScanByAddsExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaScanByAddsExec: table_path={}", self.table_url)
            }
        }
    }
}
