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
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
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
    collect_physical_columns, DeltaScanConfigBuilder, DeltaTableProvider, PATH_COLUMN,
};
use crate::kernel::models::Add;
use crate::kernel::DeltaResult;
use crate::physical_plan::{
    encode_add_actions, join_batches_with_add_actions, DeltaPhysicalExprAdapterFactory,
};
use crate::storage::{get_object_store_from_context, LogStoreRef, StorageConfig};
use crate::table::{open_table_with_object_store, DeltaTableState};

/// Physical execution node for finding files in a Delta table that match a predicate.
#[derive(Debug)]
pub struct DeltaFindFilesExec {
    table_url: Url,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    table_schema: Option<SchemaRef>, // Schema of the table for predicate evaluation
    version: i64,
    input: Arc<dyn ExecutionPlan>,
    input_partition_columns: Vec<String>,
    input_partition_scan: bool,
    cache: PlanProperties,
}

impl DeltaFindFilesExec {
    /// Create a new DeltaFindFilesExec instance.
    ///
    /// `DeltaFindFilesExec` always consumes an upstream metadata plan (`input`).
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: Option<SchemaRef>,
        version: i64,
        partition_columns: Vec<String>,
        partition_scan: bool,
    ) -> Result<Self> {
        let mut fields = crate::physical_plan::delta_action_schema()?
            .fields()
            .to_vec();
        // Boolean flag indicating if it was a partition-only scan
        fields.push(Arc::new(Field::new(
            "partition_scan",
            DataType::Boolean,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let cache = Self::compute_properties(schema);
        Ok(Self {
            table_url,
            predicate,
            table_schema,
            version,
            input,
            input_partition_columns: partition_columns,
            input_partition_scan: partition_scan,
            cache,
        })
    }

    /// Create a DeltaFindFilesExec that consumes an upstream metadata plan.
    ///
    /// The input is expected to yield a single partition stream of file rows with at least:
    /// - `PATH_COLUMN` (Utf8, non-null)
    /// - `size_bytes` (Int64) and `modification_time` (Int64) if present (used for metrics / Add fields)
    /// - partition columns as named in `partition_columns` (nullable)
    pub fn from_log_scan(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        partition_columns: Vec<String>,
        partition_scan: bool,
    ) -> Result<Self> {
        Self::new(
            input,
            table_url,
            None,
            None,
            version,
            partition_columns,
            partition_scan,
        )
    }

    /// Create a DeltaFindFilesExec that consumes an upstream metadata plan and can still apply
    /// a data predicate (non-partition-only) correctly.
    pub fn with_input(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: Option<SchemaRef>,
        version: i64,
        partition_columns: Vec<String>,
        partition_scan: bool,
    ) -> Result<Self> {
        Self::new(
            input,
            table_url,
            predicate,
            table_schema,
            version,
            partition_columns,
            partition_scan,
        )
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

    /// Get the upstream metadata input plan.
    pub fn input(&self) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.input)
    }

    /// Get partition columns carried by the upstream metadata plan.
    pub fn input_partition_columns(&self) -> &[String] {
        &self.input_partition_columns
    }

    /// Whether the upstream metadata plan is already a partition-only scan.
    pub fn input_partition_scan(&self) -> bool {
        self.input_partition_scan
    }

    async fn find_files_from_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<Add>, bool)> {
        let mut stream = input.execute(0, context.clone())?;
        let mut adds: Vec<Add> = Vec::new();

        let partition_scan = self.input_partition_scan;

        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }

            let path_arr = batch
                .column_by_name(PATH_COLUMN)
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<datafusion::arrow::array::StringArray>()
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "DeltaFindFilesExec input must have Utf8 column '{PATH_COLUMN}'"
                    ))
                })?;

            let size_arr = batch.column_by_name("size_bytes").and_then(|c| {
                c.as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
            });
            let mod_time_arr = batch.column_by_name("modification_time").and_then(|c| {
                c.as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
            });

            // Best-effort partition columns: store as strings.
            let part_arrays: Vec<(String, Arc<dyn Array>)> = self
                .input_partition_columns
                .iter()
                .filter_map(|name| {
                    batch
                        .column_by_name(name)
                        .map(|a| (name.clone(), a.clone()))
                })
                .collect();

            for row in 0..batch.num_rows() {
                if path_arr.is_null(row) {
                    return Err(DataFusionError::Plan(format!(
                        "DeltaFindFilesExec input '{PATH_COLUMN}' cannot be null"
                    )));
                }
                let path = path_arr.value(row);
                let size = size_arr.map(|a| a.value(row)).unwrap_or_default();
                let modification_time = mod_time_arr.map(|a| a.value(row)).unwrap_or_default();

                let mut partition_values: HashMap<String, Option<String>> = HashMap::new();
                for (name, arr) in &part_arrays {
                    let v = if arr.is_null(row) {
                        None
                    } else {
                        Some(
                            datafusion_common::scalar::ScalarValue::try_from_array(
                                arr.as_ref(),
                                row,
                            )?
                            .to_string(),
                        )
                    };
                    partition_values.insert(name.clone(), v);
                }

                adds.push(Add {
                    path: path.to_string(),
                    partition_values,
                    size,
                    modification_time,
                    data_change: true,
                    stats: None,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                });
            }
        }

        // If this exec has a non-partition predicate, we still need to scan data files to identify
        // matching candidates. Reuse the existing scan path but supply add actions from the input.
        if let Some(pred) = &self.predicate {
            if !partition_scan {
                let object_store = get_object_store_from_context(&context, &self.table_url)?;

                let mut table = open_table_with_object_store(
                    self.table_url.clone(),
                    object_store,
                    StorageConfig,
                )
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

                let log_store = table.log_store();
                let candidates = find_files_scan_physical_with_candidates(
                    &snapshot,
                    log_store,
                    context,
                    Arc::clone(pred),
                    adds,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                return Ok((candidates, false));
            }
        }

        Ok((adds, partition_scan))
    }
}

async fn find_files_scan_physical_with_candidates(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    ctx: Arc<TaskContext>,
    physical_predicate: Arc<dyn PhysicalExpr>,
    candidates: Vec<Add>,
) -> DeltaResult<Vec<Add>> {
    let candidate_map: HashMap<String, Add> = candidates
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
    if used_columns.is_empty() {
        for (i, _field) in logical_schema.fields().iter().enumerate() {
            used_columns.push(i);
        }
    }

    let table_provider = DeltaTableProvider::try_new(snapshot.clone(), log_store, scan_config)?;

    // FIXME: avoid creating a new session state
    let session_state = SessionStateBuilder::new()
        .with_runtime_env(ctx.runtime_env().clone())
        .with_config(ctx.session_config().clone())
        .build();

    // Scan candidate files and keep only rows that match the predicate. We intentionally do NOT
    // limit here: limiting before filtering can miss matching rows (e.g. if the first row in a
    // file doesn't match but later rows do), which would make DELETE/UPDATE a no-op.
    let scan: Arc<dyn ExecutionPlan> = table_provider
        .scan(&session_state, Some(&used_columns), &[], None)
        .await?;

    // Adapt the predicate to the projected scan schema (Column indices are schema-dependent).
    let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});
    let adapter = adapter_factory.create(Arc::clone(&logical_schema), scan.schema());
    let adapted_predicate = adapter.rewrite(physical_predicate)?;

    let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(adapted_predicate, scan)?);

    let mut partitions = Vec::new();
    for i in 0..filtered
        .properties()
        .output_partitioning()
        .partition_count()
    {
        let stream = filtered.execute(i, ctx.clone())?;
        let data = collect(stream).await?;
        partitions.extend(data);
    }

    let map = candidate_map.into_iter().collect::<HashMap<String, Add>>();
    join_batches_with_add_actions(partitions, map, PATH_COLUMN, true)
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
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if _children.len() != 1 {
            return internal_err!(
                "DeltaFindFilesExec requires exactly one child when used as a unary node"
            );
        }
        let mut cloned = (*self).clone();
        cloned.input = _children[0].clone();
        Ok(Arc::new(cloned))
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
            let (adds, partition_scan) = find_files_exec
                .find_files_from_input(Arc::clone(&find_files_exec.input), context)
                .await?;

            if adds.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let action_batch = encode_add_actions(adds)?;
            let scan_array = Arc::new(datafusion::arrow::array::BooleanArray::from(vec![
                partition_scan;
                action_batch.num_rows()
            ]));

            let mut cols = action_batch.columns().to_vec();
            cols.push(scan_array);
            RecordBatch::try_new(schema, cols)
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
        Self {
            table_url: self.table_url.clone(),
            predicate: self.predicate.clone(),
            table_schema: self.table_schema.clone(),
            version: self.version,
            input: Arc::clone(&self.input),
            input_partition_columns: self.input_partition_columns.clone(),
            input_partition_scan: self.input_partition_scan,
            cache: self.cache.clone(),
        }
    }
}
