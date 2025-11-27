// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::{stream, TryStreamExt};
use url::Url;

use crate::datasource::{
    collect_physical_columns, DataFusionMixins, DeltaScanConfigBuilder, DeltaTableProvider,
    PredicateProperties, PATH_COLUMN,
};
use crate::kernel::models::Add;
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::physical_plan::{join_batches_with_add_actions, DeltaPhysicalExprAdapterFactory};
use crate::storage::{get_object_store_from_context, LogStore, LogStoreRef, StorageConfig};
use crate::table::{open_table_with_object_store, DeltaTableState};

/// Physical execution node for finding files in a Delta table that match a predicate.
#[derive(Debug)]
pub struct DeltaFindFilesExec {
    table_url: Url,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    table_schema: Option<SchemaRef>, // Schema of the table for predicate evaluation
    version: i64,
    cache: PlanProperties,
}

impl DeltaFindFilesExec {
    /// Create a new DeltaFindFilesExec instance
    pub fn new(
        table_url: Url,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: Option<SchemaRef>,
        version: i64,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            // JSON-serialized Add action
            Field::new("add", DataType::Utf8, false),
            // Boolean flag indicating if it was a partition-only scan
            Field::new("partition_scan", DataType::Boolean, false),
        ]));
        let cache = Self::compute_properties(schema);
        Self {
            table_url,
            predicate,
            table_schema,
            version,
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Get the table URL
    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    /// Get the predicate
    pub fn predicate(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.predicate
    }

    /// Get the table schema
    pub fn table_schema(&self) -> &Option<SchemaRef> {
        &self.table_schema
    }

    /// Get the table version
    pub fn version(&self) -> i64 {
        self.version
    }

    async fn find_files(&self, context: Arc<TaskContext>) -> Result<(Vec<Add>, bool)> {
        let object_store = get_object_store_from_context(&context, &self.table_url)?;

        let mut table =
            open_table_with_object_store(self.table_url.clone(), object_store, StorageConfig)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table
            .load_version(self.version)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();

        let session_state = SessionStateBuilder::new()
            .with_runtime_env(context.runtime_env().clone())
            .with_config(context.session_config().clone())
            .build();
        let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});

        let find_result = find_files_physical(
            &snapshot,
            table.log_store(),
            &session_state,
            self.predicate.clone(),
            adapter_factory,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok((find_result.candidates, find_result.partition_scan))
    }
}

#[async_trait]
impl ExecutionPlan for DeltaFindFilesExec {
    fn name(&self) -> &'static str {
        "DeltaFindFilesExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaFindFilesExec can only be executed in a single partition");
        }

        let schema = self.schema();
        let find_files_exec = self.clone();
        let future = async move {
            let (adds, partition_scan) = find_files_exec.find_files(context).await?;

            if adds.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let adds_json: Vec<String> = adds
                .into_iter()
                .map(|add| serde_json::to_string(&add))
                .collect::<serde_json::Result<_>>()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let adds_array = Arc::new(StringArray::from(adds_json));
            let scan_array = Arc::new(datafusion::arrow::array::BooleanArray::from(vec![
                partition_scan;
                adds_array.len()
            ]));

            RecordBatch::try_new(schema, vec![adds_array, scan_array])
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for DeltaFindFilesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaFindFilesExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

impl Clone for DeltaFindFilesExec {
    fn clone(&self) -> Self {
        Self::new(
            self.table_url.clone(),
            self.predicate.clone(),
            self.table_schema.clone(),
            self.version,
        )
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct FindFiles {
    pub candidates: Vec<Add>,
    pub partition_scan: bool,
}

async fn collect_add_actions(
    snapshot: &DeltaTableState,
    log_store: &dyn LogStore,
) -> DeltaResult<Vec<Add>> {
    snapshot
        .snapshot()
        .files(log_store, None)
        .map_ok(|view| view.add_action())
        .try_collect()
        .await
}

/// Scan memory table (for partition-only predicates)
pub async fn scan_memory_table_physical(
    snapshot: &DeltaTableState,
    log_store: &dyn LogStore,
    state: &dyn Session,
    physical_predicate: Arc<dyn PhysicalExpr>,
) -> DeltaResult<Vec<Add>> {
    let actions = collect_add_actions(snapshot, log_store).await?;
    let batch = snapshot.add_actions_table(true)?;
    let mut arrays = Vec::new();
    let mut fields = Vec::new();

    let schema = batch.schema();

    arrays.push(
        batch
            .column_by_name("path")
            .ok_or(DeltaTableError::generic(
                "Column with name `path` does not exist".to_owned(),
            ))?
            .to_owned(),
    );
    fields.push(Field::new(PATH_COLUMN, DataType::Utf8, false));

    for partition_column in snapshot.metadata().partition_columns() {
        // In add_actions_table, partition columns are prefixed with "partition."
        let partition_column_name = format!("partition.{}", partition_column);
        if let Some(array) = batch.column_by_name(&partition_column_name) {
            arrays.push(array.to_owned());
            let field = schema
                .field_with_name(&partition_column_name)
                .map_err(|err| DeltaTableError::generic(err.to_string()))?;
            // Create a new field with the original partition column name (without "partition." prefix)
            let partition_field = Field::new(
                partition_column,
                field.data_type().clone(),
                field.is_nullable(),
            );
            fields.push(partition_field);
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|err| DeltaTableError::generic(err.to_string()))?;

    let memory_source = MemorySourceConfig::try_new(&[vec![batch]], schema, None)?;
    let memory_exec = DataSourceExec::from_data_source(memory_source);

    let filter_exec = Arc::new(FilterExec::try_new(physical_predicate, memory_exec)?);

    let task_ctx = Arc::new(TaskContext::from(state));
    let mut partitions = Vec::new();

    for i in 0..filter_exec
        .properties()
        .output_partitioning()
        .partition_count()
    {
        let stream = filter_exec.execute(i, task_ctx.clone())?;
        let data = collect(stream).await?;
        partitions.extend(data);
    }

    let map = actions
        .into_iter()
        .map(|action| (action.path.clone(), action))
        .collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(partitions, map, PATH_COLUMN, false)
}

/// Scan files for non-partition-only predicates
pub async fn find_files_scan_physical(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &dyn Session,
    physical_predicate: Arc<dyn PhysicalExpr>,
) -> DeltaResult<Vec<Add>> {
    let candidate_map: HashMap<String, Add> = collect_add_actions(snapshot, log_store.as_ref())
        .await?
        .into_iter()
        .map(|add| (add.path.clone(), add))
        .collect();

    let scan_config = DeltaScanConfigBuilder::new()
        .with_file_column(true)
        .build(snapshot)?;

    let logical_schema =
        crate::datasource::df_logical_schema(snapshot, &scan_config.file_column_name, None)?;

    let mut used_columns = Vec::new();

    let referenced_columns = collect_physical_columns(&physical_predicate);

    for (i, field) in logical_schema.fields().iter().enumerate() {
        if referenced_columns.contains(field.name()) {
            used_columns.push(i);
        }
    }

    if let Some(file_column_name) = &scan_config.file_column_name {
        if let Ok(idx) = logical_schema.index_of(file_column_name) {
            if !used_columns.contains(&idx) {
                used_columns.push(idx);
            }
        }
    }

    // If no columns were referenced, include all columns to be safe
    if used_columns.is_empty() {
        for (i, _field) in logical_schema.fields().iter().enumerate() {
            used_columns.push(i);
        }
    }

    let table_provider = DeltaTableProvider::try_new(snapshot.clone(), log_store, scan_config)?;

    // Scan without filtering first, then apply the physical predicate
    let scan = table_provider
        .scan(state, Some(&used_columns), &[], Some(1))
        .await?;

    // For non-partition columns, Scan without filtering to identify candidate files
    let limit: Arc<dyn ExecutionPlan> = scan;

    let task_ctx = Arc::new(TaskContext::from(state));
    let mut partitions = Vec::new();

    for i in 0..limit.properties().output_partitioning().partition_count() {
        let stream = limit.execute(i, task_ctx.clone())?;
        let data = collect(stream).await?;
        partitions.extend(data);
    }

    let map = candidate_map.into_iter().collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(partitions, map, PATH_COLUMN, true)
}

pub async fn find_files_physical(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &dyn Session,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
) -> DeltaResult<FindFiles> {
    let current_metadata = snapshot.metadata();

    match predicate {
        Some(physical_predicate) => {
            let logical_schema = snapshot.arrow_schema()?;

            // Check if the predicate only references partition columns
            let mut expr_properties =
                PredicateProperties::new(current_metadata.partition_columns().clone());
            expr_properties.analyze_predicate(&physical_predicate)?;

            if expr_properties.partition_only {
                // For partition-only predicates, create a schema with just path and partition columns
                let mut fields = vec![Field::new(PATH_COLUMN, DataType::Utf8, false)];
                for partition_column in current_metadata.partition_columns() {
                    if let Ok(field) = logical_schema.field_with_name(partition_column) {
                        fields.push(field.clone());
                    }
                }
                let partition_schema = Arc::new(Schema::new(fields));

                let adapter = adapter_factory.create(logical_schema.clone(), partition_schema);
                let adapted_predicate = adapter.rewrite(physical_predicate)?;

                // Use partition-only scanning (memory table approach)
                let candidates = scan_memory_table_physical(
                    snapshot,
                    log_store.as_ref(),
                    state,
                    adapted_predicate,
                )
                .await?;
                Ok(FindFiles {
                    candidates,
                    partition_scan: true,
                })
            } else {
                // For non-partition predicates, use the full schema
                let physical_schema = logical_schema.clone();
                let adapter = adapter_factory.create(logical_schema, physical_schema);
                let adapted_predicate = adapter.rewrite(physical_predicate)?;

                // Use full file scanning
                let candidates =
                    find_files_scan_physical(snapshot, log_store, state, adapted_predicate).await?;
                Ok(FindFiles {
                    candidates,
                    partition_scan: false,
                })
            }
        }
        None => Ok(FindFiles {
            candidates: collect_add_actions(snapshot, log_store.as_ref()).await?,
            partition_scan: true,
        }),
    }
}
