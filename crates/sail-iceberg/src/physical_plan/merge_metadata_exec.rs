use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::TryStreamExt;

use crate::row_level_metadata::{
    MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN, RowLevelMetadataColumns,
};

#[derive(Debug, Clone)]
pub struct IcebergMergeMetadataExec {
    input: Arc<dyn ExecutionPlan>,
    data_file_path: Option<String>,
    data_file_partition_spec_id: Option<i32>,
    data_file_partition_json: Option<String>,
    file_column_name: Option<String>,
    row_index_column_name: Option<String>,
    output_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl IcebergMergeMetadataExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: String,
        data_file_partition_spec_id: i32,
        data_file_partition_json: String,
        file_column_name: Option<String>,
        row_index_column_name: Option<String>,
    ) -> Result<Self> {
        Self::try_new_with_path_source(
            input,
            Some(data_file_path),
            Some(data_file_partition_spec_id),
            Some(data_file_partition_json),
            file_column_name,
            row_index_column_name,
        )
    }

    pub fn try_new_partitioned_files(
        input: Arc<dyn ExecutionPlan>,
        file_column_name: String,
        row_index_column_name: Option<String>,
    ) -> Result<Self> {
        for metadata_column in [
            file_column_name.as_str(),
            MERGE_PARTITION_SPEC_ID_COLUMN,
            MERGE_PARTITION_COLUMN,
        ] {
            if input.schema().field_with_name(metadata_column).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "Iceberg merge scan is missing metadata column '{metadata_column}'"
                )));
            }
        }
        Self::try_new_with_path_source(
            input,
            None,
            None,
            None,
            Some(file_column_name),
            row_index_column_name,
        )
    }

    fn try_new_with_path_source(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: Option<String>,
        data_file_partition_spec_id: Option<i32>,
        data_file_partition_json: Option<String>,
        file_column_name: Option<String>,
        row_index_column_name: Option<String>,
    ) -> Result<Self> {
        if row_index_column_name.is_some() {
            ensure_absolute_row_position_scan(&input)?;
        }
        let appended_file_column = data_file_path
            .is_some()
            .then_some(file_column_name.as_deref())
            .flatten();
        let metadata_columns =
            RowLevelMetadataColumns::new(appended_file_column, row_index_column_name.as_deref());
        let metadata_columns = if data_file_path.is_some() {
            metadata_columns.with_delete_file_metadata()
        } else {
            metadata_columns
        };
        let output_schema = Arc::new(metadata_columns.append_to_schema(input.schema().as_ref())?);
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            data_file_path,
            data_file_partition_spec_id,
            data_file_partition_json,
            file_column_name,
            row_index_column_name,
            output_schema,
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn data_file_path(&self) -> Option<&str> {
        self.data_file_path.as_deref()
    }

    pub fn file_column_name(&self) -> Option<&str> {
        self.file_column_name.as_deref()
    }

    pub fn data_file_partition_spec_id(&self) -> Option<i32> {
        self.data_file_partition_spec_id
    }

    pub fn data_file_partition_json(&self) -> Option<&str> {
        self.data_file_partition_json.as_deref()
    }

    pub fn row_index_column_name(&self) -> Option<&str> {
        self.row_index_column_name.as_deref()
    }
}

#[async_trait]
impl ExecutionPlan for IcebergMergeMetadataExec {
    fn name(&self) -> &str {
        "IcebergMergeMetadataExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.data_file_path.is_some() {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::UnspecifiedDistribution]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergMergeMetadataExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new_with_path_source(
            Arc::clone(&children[0]),
            self.data_file_path.clone(),
            self.data_file_partition_spec_id,
            self.data_file_partition_json.clone(),
            self.file_column_name.clone(),
            self.row_index_column_name.clone(),
        )?))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if self.data_file_path.is_some() && partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergMergeMetadataExec only supports partition 0, got {partition}"
            )));
        }

        let child = self.input.execute(partition, context)?;
        let output_schema = self.output_schema.clone();
        let schema_for_adapter = output_schema.clone();
        let data_file_path = self.data_file_path.clone();
        let data_file_partition_spec_id = self.data_file_partition_spec_id;
        let data_file_partition_json = self.data_file_partition_json.clone();
        let file_column_name = self.file_column_name.clone();
        let include_file = data_file_path.is_some() && file_column_name.is_some();
        let include_row_index = self.row_index_column_name.is_some();

        let stream = try_stream! {
            // The provider and constructor guarantee that each input partition contains
            // complete, naturally ordered files, so this offset is file-absolute.
            let mut row_offset = 0i64;
            let mut current_file_path: Option<String> = None;
            let mut stream = child;
            while let Some(batch) = stream.try_next().await? {
                let rows = batch.num_rows();
                let mut columns = batch.columns().to_vec();
                if include_file {
                    let values = (0..rows)
                        .map(|_| data_file_path.as_deref())
                        .collect::<Vec<_>>();
                    columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    columns.push(Arc::new(Int32Array::from(vec![
                        data_file_partition_spec_id;
                        rows
                    ])) as ArrayRef);
                    columns.push(Arc::new(StringArray::from(vec![
                        data_file_partition_json.as_deref();
                        rows
                    ])) as ArrayRef);
                }
                if include_row_index {
                    let values = if let Some(file_column_name) = file_column_name.as_deref()
                        && data_file_path.is_none()
                    {
                        let file_paths = batch
                            .column_by_name(file_column_name)
                            .ok_or_else(|| DataFusionError::Internal(format!(
                                "missing Iceberg merge file column '{file_column_name}'"
                            )))?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| DataFusionError::Internal(format!(
                                "Iceberg merge file column '{file_column_name}' must be Utf8"
                            )))?;
                        let mut values = Vec::with_capacity(rows);
                        for row in 0..rows {
                            if file_paths.is_null(row) {
                                Err(DataFusionError::Execution(
                                    "Iceberg merge file path cannot be null".to_string(),
                                ))?;
                            }
                            let file_path = file_paths.value(row);
                            if current_file_path.as_deref() != Some(file_path) {
                                current_file_path = Some(file_path.to_string());
                                row_offset = 0;
                            }
                            values.push(row_offset);
                            row_offset += 1;
                        }
                        values
                    } else {
                        let values = (0..rows)
                            .map(|idx| row_offset + idx as i64)
                            .collect::<Vec<_>>();
                        row_offset += rows as i64;
                        values
                    };
                    columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                }

                yield RecordBatch::try_new(output_schema.clone(), columns)?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            Box::pin(stream),
        )))
    }
}

fn ensure_absolute_row_position_scan(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
        let config = scan
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "Iceberg row-position metadata requires a data-file scan".to_string(),
                )
            })?;
        if !config.preserve_order {
            return Err(DataFusionError::Plan(
                "Iceberg row-position scans must preserve data-file row order".to_string(),
            ));
        }
        if !config.partitioned_by_file_group {
            return Err(DataFusionError::Plan(
                "Iceberg row-position scans must disable data-file repartitioning".to_string(),
            ));
        }
        for group in &config.file_groups {
            if group.len() > 1 {
                return Err(DataFusionError::Plan(
                    "Iceberg row-position scans require at most one data file per input partition"
                        .to_string(),
                ));
            }
            if group.iter().any(|file| file.range.is_some()) {
                return Err(DataFusionError::Plan(
                    "Iceberg row-position scans do not support split data-file ranges".to_string(),
                ));
            }
        }
        return Ok(());
    }

    let children = plan.children();
    if children.is_empty() {
        return Err(DataFusionError::Plan(
            "Iceberg row-position metadata requires a data-file scan".to_string(),
        ));
    }
    for child in children {
        ensure_absolute_row_position_scan(child)?;
    }
    Ok(())
}

impl DisplayAs for IcebergMergeMetadataExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => match self.data_file_path.as_deref() {
                Some(data_file_path) => {
                    write!(f, "IcebergMergeMetadataExec: data_file={data_file_path}")
                }
                None => write!(
                    f,
                    "IcebergMergeMetadataExec: data_file_column={}",
                    self.file_column_name.as_deref().unwrap_or("<missing>")
                ),
            },
        }
    }
}
