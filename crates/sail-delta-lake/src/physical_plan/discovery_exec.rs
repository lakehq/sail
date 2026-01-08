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
use crate::physical_plan::{encode_actions, DeltaPhysicalExprAdapterFactory, ExecAction};
use crate::storage::{get_object_store_from_context, StorageConfig};
use crate::table::open_table_with_object_store;

#[derive(Debug)]
pub struct DeltaDiscoveryExec {
    table_url: Url,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    table_schema: Option<SchemaRef>,
    version: i64,
    input: Arc<dyn ExecutionPlan>,
    input_partition_columns: Vec<String>,
    input_partition_scan: bool,
    cache: PlanProperties,
}

impl DeltaDiscoveryExec {
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
}

#[async_trait]
impl ExecutionPlan for DeltaDiscoveryExec {
    fn name(&self) -> &'static str {
        "DeltaDiscoveryExec"
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
                "DeltaDiscoveryExec requires exactly one child when used as a unary node"
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
            return internal_err!("DeltaDiscoveryExec can only be executed in a single partition");
        }

        let schema = self.schema();
        let input = Arc::clone(&self.input);
        let table_url = self.table_url.clone();
        let version = self.version;
        let predicate = self.predicate.clone();
        let partition_columns = self.input_partition_columns.clone();
        let partition_scan = self.input_partition_scan;

        let future = async move {
            let mut stream = input.execute(0, context.clone())?;
            let mut adds: Vec<Add> = Vec::new();

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
                            "DeltaDiscoveryExec input must have Utf8 column '{PATH_COLUMN}'"
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

                let part_arrays: Vec<(String, Arc<dyn Array>)> = partition_columns
                    .iter()
                    .filter_map(|name| {
                        batch.column_by_name(name).map(|a| {
                            let a = datafusion::arrow::compute::cast(a, &DataType::Utf8)
                                .unwrap_or_else(|_| a.clone());
                            (name.clone(), a)
                        })
                    })
                    .collect();

                for row in 0..batch.num_rows() {
                    if path_arr.is_null(row) {
                        return Err(DataFusionError::Plan(format!(
                            "DeltaDiscoveryExec input '{PATH_COLUMN}' cannot be null"
                        )));
                    }
                    let path = path_arr.value(row);
                    let size = size_arr.map(|a| a.value(row)).unwrap_or_default();
                    let modification_time = mod_time_arr.map(|a| a.value(row)).unwrap_or_default();

                    let mut partition_values: HashMap<String, Option<String>> =
                        HashMap::with_capacity(part_arrays.len());
                    for (name, arr) in &part_arrays {
                        let v = if arr.is_null(row) {
                            None
                        } else if let Some(s) = arr
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                        {
                            Some(s.value(row).to_string())
                        } else {
                            datafusion::arrow::util::display::array_value_to_string(
                                arr.as_ref(),
                                row,
                            )
                            .ok()
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

            let (final_adds, is_partition_scan) = if let Some(pred) = predicate {
                if !partition_scan {
                    let object_store = get_object_store_from_context(&context, &table_url)?;
                    let mut table =
                        open_table_with_object_store(table_url, object_store, StorageConfig)
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    table
                        .load_version(version)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let snapshot = table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .clone();
                    let log_store = table.log_store();

                    let mut candidate_map: HashMap<String, Add> = adds
                        .into_iter()
                        .map(|add| (add.path.clone(), add))
                        .collect();

                    let scan_config = DeltaScanConfigBuilder::new()
                        .with_file_column(true)
                        .build(&snapshot)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let logical_schema = crate::datasource::df_logical_schema(
                        &snapshot,
                        &scan_config.file_column_name,
                        None,
                    )?;

                    let mut used_columns = Vec::new();
                    let referenced_columns = collect_physical_columns(&pred);
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
                        for (i, _) in logical_schema.fields().iter().enumerate() {
                            used_columns.push(i);
                        }
                    }

                    let table_provider =
                        DeltaTableProvider::try_new(snapshot, log_store, scan_config)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let session_state = SessionStateBuilder::new()
                        .with_runtime_env(context.runtime_env().clone())
                        .with_config(context.session_config().clone())
                        .build();

                    let scan: Arc<dyn ExecutionPlan> = table_provider
                        .scan(&session_state, Some(&used_columns), &[], None)
                        .await?;

                    let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});
                    let adapter =
                        adapter_factory.create(Arc::clone(&logical_schema), scan.schema());
                    let adapted_predicate = adapter.rewrite(pred)?;

                    let filtered: Arc<dyn ExecutionPlan> =
                        Arc::new(FilterExec::try_new(adapted_predicate, scan)?);

                    let mut discovered_files = Vec::new();
                    let mut seen_paths = std::collections::HashSet::new();

                    for i in 0..filtered
                        .properties()
                        .output_partitioning()
                        .partition_count()
                    {
                        let mut stream = filtered.execute(i, context.clone())?;
                        while let Some(batch) = stream.try_next().await? {
                            let path_column =
                                batch.column_by_name(PATH_COLUMN).ok_or_else(|| {
                                    DataFusionError::Plan(
                                        "Missing path column in filtered results".to_string(),
                                    )
                                })?;

                            let path_array = path_column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::DictionaryArray<
                                    datafusion::arrow::datatypes::UInt16Type,
                                >>()
                                .ok_or_else(|| {
                                    DataFusionError::Plan(
                                        "Path column is not a dictionary array".to_string(),
                                    )
                                })?;

                            let string_values = path_array
                                .values()
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                                .ok_or_else(|| {
                                    DataFusionError::Plan(
                                        "Dictionary values are not strings".to_string(),
                                    )
                                })?;

                            for idx in path_array.keys().iter().flatten() {
                                let path = string_values.value(idx as usize);
                                if !seen_paths.insert(path.to_string()) {
                                    continue;
                                }

                                match candidate_map.remove(path) {
                                    Some(action) => discovered_files.push(action),
                                    None => {
                                        return Err(DataFusionError::Plan(format!(
                                            "File path '{}' not found in candidate actions",
                                            path
                                        )));
                                    }
                                }
                            }
                        }
                    }

                    (discovered_files, false)
                } else {
                    (adds, partition_scan)
                }
            } else {
                (adds, partition_scan)
            };

            if final_adds.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let exec_actions: Vec<ExecAction> =
                final_adds.into_iter().map(|add| add.into()).collect();
            let action_batch = encode_actions(exec_actions)?;
            let scan_array = Arc::new(datafusion::arrow::array::BooleanArray::from(
                vec![is_partition_scan; action_batch.num_rows()],
            ));

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

impl DisplayAs for DeltaDiscoveryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDiscoveryExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

impl Clone for DeltaDiscoveryExec {
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
