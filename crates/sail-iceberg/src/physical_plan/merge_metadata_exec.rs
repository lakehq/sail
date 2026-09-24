use std::collections::HashMap;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::TryStreamExt;

use crate::row_level_metadata::{
    MERGE_FILE_METADATA_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN, RowLevelMetadataColumns,
};

#[derive(Debug, Clone)]
pub struct IcebergMergeMetadataExec {
    input: Arc<dyn ExecutionPlan>,
    data_file_path: Option<String>,
    data_file_partition_spec_id: Option<i32>,
    data_file_metadata_json: Option<String>,
    file_column_name: Option<String>,
    row_index_column_name: Option<String>,
    row_lineage: Option<crate::row_lineage::RowLineage>,
    file_lineage: Arc<HashMap<String, crate::row_lineage::RowLineage>>,
    output_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl IcebergMergeMetadataExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: String,
        data_file_partition_spec_id: i32,
        data_file_metadata_json: String,
        file_column_name: Option<String>,
        row_index_column_name: Option<String>,
        row_lineage: Option<crate::row_lineage::RowLineage>,
    ) -> Result<Self> {
        Self::try_new_with_path_source(
            input,
            Some(data_file_path),
            Some(data_file_partition_spec_id),
            Some(data_file_metadata_json),
            file_column_name,
            row_index_column_name,
            row_lineage,
            HashMap::new(),
        )
    }

    pub fn try_new_partitioned_files(
        input: Arc<dyn ExecutionPlan>,
        file_column_name: String,
        row_index_column_name: Option<String>,
        file_lineage: HashMap<String, crate::row_lineage::RowLineage>,
    ) -> Result<Self> {
        for metadata_column in [
            file_column_name.as_str(),
            MERGE_PARTITION_SPEC_ID_COLUMN,
            MERGE_FILE_METADATA_COLUMN,
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
            None,
            file_lineage,
        )
    }

    fn try_new_with_path_source(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: Option<String>,
        data_file_partition_spec_id: Option<i32>,
        data_file_metadata_json: Option<String>,
        file_column_name: Option<String>,
        row_index_column_name: Option<String>,
        row_lineage: Option<crate::row_lineage::RowLineage>,
        file_lineage: HashMap<String, crate::row_lineage::RowLineage>,
    ) -> Result<Self> {
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
        let equivalence = EquivalenceProperties::new(output_schema.clone());
        let equivalence = if row_lineage.is_some() || !file_lineage.is_empty() {
            equivalence
        } else {
            equivalence.extend(input.equivalence_properties().clone())?
        };
        let cache = Arc::new(PlanProperties::new(
            equivalence,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            data_file_path,
            data_file_partition_spec_id,
            data_file_metadata_json,
            file_column_name,
            row_index_column_name,
            row_lineage,
            file_lineage: Arc::new(file_lineage),
            output_schema,
            cache,
        })
    }

    pub fn row_lineage(&self) -> Option<crate::row_lineage::RowLineage> {
        self.row_lineage
    }

    pub fn file_lineage(&self) -> &HashMap<String, crate::row_lineage::RowLineage> {
        &self.file_lineage
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

    pub fn data_file_metadata_json(&self) -> Option<&str> {
        self.data_file_metadata_json.as_deref()
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
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergMergeMetadataExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new_with_path_source(
            Arc::clone(&children[0]),
            self.data_file_path.clone(),
            self.data_file_partition_spec_id,
            self.data_file_metadata_json.clone(),
            self.file_column_name.clone(),
            self.row_index_column_name.clone(),
            self.row_lineage,
            self.file_lineage.as_ref().clone(),
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
        let data_file_metadata_json = self.data_file_metadata_json.clone();
        let file_column_name = self.file_column_name.clone();
        let include_file = data_file_path.is_some() && file_column_name.is_some();
        let include_row_index = self.row_index_column_name.is_some();
        let row_lineage = self.row_lineage;
        let file_lineage = self.file_lineage.clone();

        let stream = try_stream! {
            // The provider keeps complete, naturally ordered files in each input
            // partition, so this offset is file-absolute.
            let mut row_offset = 0i64;
            let mut current_file_path: Option<String> = None;
            let mut stream = child;
            while let Some(batch) = stream.try_next().await? {
                let file_paths = if data_file_path.is_none() {
                    file_column_name.as_deref().map(|name| {
                        batch.column_by_name(name)
                            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                            .ok_or_else(|| DataFusionError::Execution(format!("Iceberg merge file column '{name}' must be Utf8")))
                    }).transpose()?
                } else {
                    None
                };
                let mut start = 0;
                while start < batch.num_rows() {
                    let mut end = batch.num_rows();
                    let lineage = if let Some(paths) = file_paths {
                        if paths.is_null(start) {
                            Err(DataFusionError::Execution("Iceberg merge file path cannot be null".to_string()))?;
                        }
                        let path = paths.value(start);
                        end = (start + 1..batch.num_rows()).find(|index| paths.is_null(*index) || paths.value(*index) != path)
                            .unwrap_or(batch.num_rows());
                        if current_file_path.as_deref() != Some(path) {
                            current_file_path = Some(path.to_string());
                            row_offset = 0;
                        }
                        if file_lineage.is_empty() {
                            None
                        } else {
                            Some(*file_lineage.get(path).ok_or_else(|| DataFusionError::Execution(
                                format!("Missing Iceberg row lineage for {path}")
                            ))?)
                        }
                    } else {
                        row_lineage
                    };
                    let rows = end - start;
                    let batch = batch.slice(start, rows);
                    let mut columns = match lineage {
                        Some(lineage) => crate::row_lineage::materialize_lineage(&batch, lineage, row_offset)?,
                        None => batch.columns().to_vec(),
                    };
                    if include_file {
                        columns.push(Arc::new(StringArray::from(vec![data_file_path.as_deref(); rows])) as ArrayRef);
                        columns.push(Arc::new(Int32Array::from(vec![data_file_partition_spec_id; rows])) as ArrayRef);
                        columns.push(Arc::new(StringArray::from(vec![data_file_metadata_json.as_deref(); rows])) as ArrayRef);
                    }
                    let next_offset = i64::try_from(rows).ok().and_then(|rows| row_offset.checked_add(rows))
                        .ok_or_else(|| DataFusionError::Execution("Iceberg row position overflow".to_string()))?;
                    if include_row_index {
                        columns.push(Arc::new(Int64Array::from_iter_values(row_offset..next_offset)) as ArrayRef);
                    }
                    row_offset = next_offset;
                    yield RecordBatch::try_new(output_schema.clone(), columns)?;
                    start = end;
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            Box::pin(stream),
        )))
    }
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use sail_common_datafusion::datasource::{MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN};

    use super::*;
    use crate::row_lineage::{LINEAGE_COLUMNS, RowLineage, lineage_fields};

    #[tokio::test]
    async fn partitioned_lineage_tracks_file_offsets_across_batches() -> Result<()> {
        let mut fields = lineage_fields().to_vec();
        fields.extend([
            Arc::new(Field::new(MERGE_FILE_COLUMN, DataType::Utf8, false)),
            Arc::new(Field::new(
                MERGE_PARTITION_SPEC_ID_COLUMN,
                DataType::Int32,
                false,
            )),
            Arc::new(Field::new(
                MERGE_FILE_METADATA_COLUMN,
                DataType::Utf8,
                false,
            )),
        ]);
        let schema = Arc::new(Schema::new(fields));
        let batch = |paths: Vec<&str>, ids: Vec<Option<i64>>| {
            let rows = paths.len();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(vec![None; rows])),
                    Arc::new(StringArray::from(paths)),
                    Arc::new(Int32Array::from(vec![0; rows])),
                    Arc::new(StringArray::from(vec!["[]"; rows])),
                ],
            )
        };
        let input = MemorySourceConfig::try_new_exec(
            &[vec![
                batch(vec!["a", "a", "b"], vec![None, None, Some(777)])?,
                batch(vec!["b", "c"], vec![None, None])?,
            ]],
            schema,
            None,
        )?;
        let plan = Arc::new(IcebergMergeMetadataExec::try_new_partitioned_files(
            input,
            MERGE_FILE_COLUMN.to_string(),
            Some(MERGE_ROW_INDEX_COLUMN.to_string()),
            HashMap::from([
                (
                    "a".to_string(),
                    RowLineage {
                        first_row_id: Some(100),
                        data_sequence_number: 2,
                    },
                ),
                (
                    "b".to_string(),
                    RowLineage {
                        first_row_id: Some(200),
                        data_sequence_number: 3,
                    },
                ),
                (
                    "c".to_string(),
                    RowLineage {
                        first_row_id: None,
                        data_sequence_number: 1,
                    },
                ),
            ]),
        )?);
        let output_schema = plan.schema();
        let batches = collect(plan, Arc::new(TaskContext::default())).await?;
        let output = concat_batches(&output_schema, &batches)?;
        for (name, values) in [
            (
                LINEAGE_COLUMNS[0],
                vec![Some(100), Some(101), Some(777), Some(201), None],
            ),
            (
                LINEAGE_COLUMNS[1],
                vec![Some(2), Some(2), Some(3), Some(3), None],
            ),
            (
                MERGE_ROW_INDEX_COLUMN,
                vec![Some(0), Some(1), Some(0), Some(1), Some(0)],
            ),
        ] {
            let index = output_schema.index_of(name)?;
            assert_eq!(
                output.column(index).as_ref(),
                &Int64Array::from(values) as &dyn Array
            );
        }
        Ok(())
    }
}
