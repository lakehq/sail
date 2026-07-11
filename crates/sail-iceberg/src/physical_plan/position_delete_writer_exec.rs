use std::collections::BTreeSet;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::StreamExt;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::io::StoreContext;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::physical_plan::action_schema::{encode_delete_data_files, iceberg_action_schema};
use crate::physical_plan::delete_writer_common::{self, IcebergDeleteWriterConfig};
use crate::row_level_metadata::{MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};
use crate::spec::types::values::Literal;
use crate::spec::{DataContentType, DataFile, TableMetadata};

#[derive(Debug, Clone, PartialEq)]
struct PositionDeleteTarget {
    file_path: String,
    partition_spec_id: i32,
    partition_json: String,
    partition: Vec<Option<Literal>>,
}

fn position_delete_target(
    table_meta: &TableMetadata,
    file_path: &str,
    partition_spec_id: i32,
    partition_json: &str,
) -> Result<PositionDeleteTarget> {
    if !table_meta
        .partition_specs
        .iter()
        .any(|spec| spec.spec_id() == partition_spec_id)
    {
        return Err(DataFusionError::Plan(format!(
            "MERGE target file uses unknown Iceberg partition spec {partition_spec_id}: {file_path}"
        )));
    }
    let partition = serde_json::from_str(partition_json).map_err(|error| {
        DataFusionError::Plan(format!(
            "failed to decode Iceberg partition metadata for {file_path}: {error}"
        ))
    })?;
    Ok(PositionDeleteTarget {
        file_path: file_path.to_string(),
        partition_spec_id,
        partition_json: partition_json.to_string(),
        partition,
    })
}

const POSITION_DELETE_FILE_PATH_COL: &str = "file_path";
const POSITION_DELETE_POS_COL: &str = "pos";
const POSITION_DELETE_FILE_PATH_ID: &str = "2147483546";
const POSITION_DELETE_POS_ID: &str = "2147483545";

#[derive(Debug, Clone)]
pub struct IcebergPositionDeleteWriterExec {
    input: Arc<dyn ExecutionPlan>,
    writer_config: IcebergDeleteWriterConfig,
    file_column_name: String,
    row_index_column_name: String,
    cache: Arc<PlanProperties>,
}

impl IcebergPositionDeleteWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        table_properties: Vec<(String, String)>,
        write_data_path: Option<String>,
        write_folder_storage_path: Option<String>,
        file_column_name: impl Into<String>,
        row_index_column_name: impl Into<String>,
    ) -> Self {
        let schema = iceberg_action_schema().unwrap_or_else(|e| {
            log::error!("failed to initialize iceberg action schema: {e}");
            Arc::new(Schema::empty())
        });
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            writer_config: IcebergDeleteWriterConfig::new(
                table_url,
                table_properties,
                write_data_path,
                write_folder_storage_path,
            ),
            file_column_name: file_column_name.into(),
            row_index_column_name: row_index_column_name.into(),
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        self.writer_config.table_url()
    }

    pub fn table_properties(&self) -> &[(String, String)] {
        self.writer_config.table_properties()
    }

    pub fn write_data_path(&self) -> Option<&str> {
        self.writer_config.write_data_path()
    }

    pub fn write_folder_storage_path(&self) -> Option<&str> {
        self.writer_config.write_folder_storage_path()
    }

    pub fn file_column_name(&self) -> &str {
        &self.file_column_name
    }

    pub fn row_index_column_name(&self) -> &str {
        &self.row_index_column_name
    }
}

#[async_trait]
impl ExecutionPlan for IcebergPositionDeleteWriterExec {
    fn name(&self) -> &str {
        "IcebergPositionDeleteWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let Ok(index) = self.input.schema().index_of(&self.file_column_name) else {
            return vec![Distribution::SinglePartition];
        };
        let expression: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(&self.file_column_name, index));
        vec![Distribution::HashPartitioned(vec![expression])]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let schema = self.input.schema();
        let (Ok(file_index), Ok(row_index)) = (
            schema.index_of(&self.file_column_name),
            schema.index_of(&self.row_index_column_name),
        ) else {
            return vec![None];
        };
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new(&self.file_column_name, file_index)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new(&self.row_index_column_name, row_index)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ]);
        vec![ordering.map(OrderingRequirements::from)]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergPositionDeleteWriterExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.writer_config.table_url().clone(),
            self.writer_config.table_properties().to_vec(),
            self.writer_config
                .write_data_path()
                .map(ToString::to_string),
            self.writer_config
                .write_folder_storage_path()
                .map(ToString::to_string),
            self.file_column_name.clone(),
            self.row_index_column_name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partitions = self.input.output_partitioning().partition_count().max(1);
        if partition >= input_partitions {
            return internal_err!(
                "IcebergPositionDeleteWriterExec partition {partition} exceeds partition count {input_partitions}"
            );
        }

        let input = self.input.execute(partition, Arc::clone(&context))?;
        let writer_config = self.writer_config.clone();
        let file_column_name = self.file_column_name.clone();
        let row_index_column_name = self.row_index_column_name.clone();
        let output_schema = self.schema();
        let schema_for_adapter = output_schema.clone();

        let stream = try_stream! {
            let store_ctx =
                delete_writer_common::store_context(&context, writer_config.table_url())?;
            let table_meta = writer_config
                .load_current_table_metadata(&store_ctx)
                .await?;
            delete_writer_common::ensure_position_delete_file_writes(&table_meta)?;
            let data_dir = writer_config.resolve_data_dir(&table_meta);

            let mut current_target: Option<PositionDeleteTarget> = None;
            // FIXME: Stream and roll position-delete files instead of buffering every
            // position for a data file.
            let mut current_positions = BTreeSet::new();
            let mut input = input;
            while let Some(batch_result) = input.next().await {
                let batch = batch_result?;
                let file_paths = batch
                    .column_by_name(&file_column_name)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("missing column {file_column_name}"))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "{file_column_name} must be a Utf8 column"
                        ))
                    })?;
                let row_indices = batch
                    .column_by_name(&row_index_column_name)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "missing column {row_index_column_name}"
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "{row_index_column_name} must be an Int64 column"
                        ))
                    })?;
                let partition_spec_ids = batch
                    .column_by_name(MERGE_PARTITION_SPEC_ID_COLUMN)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "missing column {MERGE_PARTITION_SPEC_ID_COLUMN}"
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "{MERGE_PARTITION_SPEC_ID_COLUMN} must be an Int32 column"
                        ))
                    })?;
                let partitions = batch
                    .column_by_name(MERGE_PARTITION_COLUMN)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "missing column {MERGE_PARTITION_COLUMN}"
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "{MERGE_PARTITION_COLUMN} must be a Utf8 column"
                        ))
                    })?;

                for row in 0..batch.num_rows() {
                    if file_paths.is_null(row) || row_indices.is_null(row) {
                        continue;
                    }
                    if partition_spec_ids.is_null(row) || partitions.is_null(row) {
                        Err(DataFusionError::Plan(
                            "MERGE position delete rows require Iceberg partition metadata"
                                .to_string(),
                        ))?;
                    }
                    let file_path = file_paths.value(row);
                    let partition_spec_id = partition_spec_ids.value(row);
                    let partition_json = partitions.value(row);
                    if current_target
                        .as_ref()
                        .is_some_and(|target| target.file_path != file_path)
                    {
                        let next_target = position_delete_target(
                            &table_meta,
                            file_path,
                            partition_spec_id,
                            partition_json,
                        )?;
                        let completed_target = current_target.replace(next_target).ok_or_else(|| {
                            DataFusionError::Internal(
                                "missing current Iceberg position-delete target".to_string(),
                            )
                        })?;
                        let completed_positions = std::mem::take(&mut current_positions);
                        let delete_file = write_position_delete_file(
                            &store_ctx,
                            writer_config.table_url(),
                            &data_dir,
                            &completed_target,
                            &completed_positions,
                        )
                        .await?;
                        yield encode_delete_data_files(vec![delete_file])?;
                    } else if let Some(target) = &current_target {
                        if target.partition_spec_id != partition_spec_id
                            || target.partition_json != partition_json
                        {
                            Err(DataFusionError::Plan(format!(
                                "inconsistent Iceberg partition metadata for MERGE target file {file_path}"
                            )))?;
                        }
                    } else {
                        current_target = Some(position_delete_target(
                            &table_meta,
                            file_path,
                            partition_spec_id,
                            partition_json,
                        )?);
                    }
                    let row_index = row_indices.value(row);
                    if row_index < 0 {
                        Err(DataFusionError::Plan(format!(
                            "MERGE position delete row index must be non-negative, got {row_index}"
                        )))?;
                    }
                    current_positions.insert(row_index);
                }
            }

            if let Some(completed_target) = current_target {
                let delete_file = write_position_delete_file(
                    &store_ctx,
                    writer_config.table_url(),
                    &data_dir,
                    &completed_target,
                    &current_positions,
                )
                .await?;
                yield encode_delete_data_files(vec![delete_file])?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            Box::pin(stream),
        )))
    }
}

impl DisplayAs for IcebergPositionDeleteWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergPositionDeleteWriterExec(table_path={})",
                    self.writer_config.table_url()
                )
            }
        }
    }
}

async fn write_position_delete_file(
    store_ctx: &StoreContext,
    table_url: &Url,
    data_dir: &str,
    target: &PositionDeleteTarget,
    positions: &BTreeSet<i64>,
) -> Result<DataFile> {
    let delete_schema = position_delete_arrow_schema();
    let file_paths = (0..positions.len())
        .map(|_| Some(target.file_path.as_str()))
        .collect::<Vec<_>>();
    let pos_values = positions.iter().copied().collect::<Vec<_>>();
    let batch = ArrowRecordBatch::try_new(
        Arc::new(delete_schema.clone()),
        vec![
            Arc::new(StringArray::from(file_paths)),
            Arc::new(Int64Array::from(pos_values)),
        ],
    )?;

    let mut writer = ArrowParquetWriter::try_new(&delete_schema, WriterProperties::default())
        .map_err(DataFusionError::Execution)?;
    writer
        .write_batch(&batch)
        .await
        .map_err(DataFusionError::Execution)?;
    let mut delete_file = delete_writer_common::write_delete_parquet_file(
        store_ctx,
        table_url,
        data_dir,
        "delete",
        writer,
        target.partition_spec_id,
        target.partition.clone(),
    )
    .await?;
    delete_file.content = DataContentType::PositionDeletes;
    delete_file.referenced_data_file = Some(target.file_path.clone());
    delete_file.sort_order_id = None;
    delete_file.equality_ids.clear();
    Ok(delete_file)
}

fn position_delete_arrow_schema() -> Schema {
    Schema::new(vec![
        Arc::new(
            Field::new(POSITION_DELETE_FILE_PATH_COL, DataType::Utf8, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_DELETE_FILE_PATH_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ),
        Arc::new(
            Field::new(POSITION_DELETE_POS_COL, DataType::Int64, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_DELETE_POS_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ),
    ])
}
