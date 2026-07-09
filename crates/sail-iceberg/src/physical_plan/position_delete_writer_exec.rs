use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::once;
use futures::StreamExt;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::io::{
    load_manifest as io_load_manifest, load_manifest_list as io_load_manifest_list, StoreContext,
};
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::physical_plan::action_schema::{encode_delete_data_files, iceberg_action_schema};
use crate::physical_plan::delete_writer_common::{self, IcebergDeleteWriterConfig};
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestList, ManifestStatus, TableMetadata,
};

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
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
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
        vec![Distribution::SinglePartition]
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
        if partition != 0 {
            return internal_err!(
                "IcebergPositionDeleteWriterExec can only be executed in a single partition"
            );
        }
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "IcebergPositionDeleteWriterExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let input = self.input.execute(0, Arc::clone(&context))?;
        let writer_config = self.writer_config.clone();
        let file_column_name = self.file_column_name.clone();
        let row_index_column_name = self.row_index_column_name.clone();
        let output_schema = self.schema();
        let schema_for_adapter = output_schema.clone();

        let future = async move {
            let store_ctx =
                delete_writer_common::store_context(&context, writer_config.table_url())?;

            let mut positions_by_file: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
            let mut stream = input;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                collect_positions(
                    &batch,
                    &file_column_name,
                    &row_index_column_name,
                    &mut positions_by_file,
                )?;
            }

            if positions_by_file.is_empty() {
                return encode_delete_data_files(vec![]);
            }

            let table_meta = writer_config
                .load_current_table_metadata(&store_ctx)
                .await?;
            delete_writer_common::ensure_position_delete_file_writes(&table_meta)?;
            let data_dir = writer_config.resolve_data_dir(&table_meta);
            let data_files = load_current_data_files(&store_ctx, &table_meta).await?;
            let data_file_by_path = data_files
                .into_iter()
                .map(|file| (file.file_path.clone(), file))
                .collect::<HashMap<_, _>>();

            let mut delete_files = Vec::with_capacity(positions_by_file.len());
            for (data_file_path, positions) in positions_by_file {
                let target_file = data_file_by_path.get(&data_file_path).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "MERGE attempted to delete rows from unknown Iceberg data file: {data_file_path}"
                    ))
                })?;
                let delete_file = write_position_delete_file(
                    &store_ctx,
                    writer_config.table_url(),
                    &data_dir,
                    target_file,
                    &positions,
                )
                .await?;
                delete_files.push(delete_file);
            }

            encode_delete_data_files(delete_files)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            once(future),
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

fn collect_positions(
    batch: &RecordBatch,
    file_column_name: &str,
    row_index_column_name: &str,
    positions_by_file: &mut BTreeMap<String, BTreeSet<i64>>,
) -> Result<()> {
    let file_col = batch
        .column_by_name(file_column_name)
        .ok_or_else(|| DataFusionError::Internal(format!("missing column {file_column_name}")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{file_column_name} must be a Utf8 column"))
        })?;
    let pos_col = batch
        .column_by_name(row_index_column_name)
        .ok_or_else(|| {
            DataFusionError::Internal(format!("missing column {row_index_column_name}"))
        })?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{row_index_column_name} must be an Int64 column"))
        })?;

    for row in 0..batch.num_rows() {
        if file_col.is_null(row) || pos_col.is_null(row) {
            continue;
        }
        positions_by_file
            .entry(file_col.value(row).to_string())
            .or_default()
            .insert(pos_col.value(row));
    }
    Ok(())
}

async fn load_current_data_files(
    store_ctx: &StoreContext,
    table_meta: &TableMetadata,
) -> Result<Vec<DataFile>> {
    let snapshot = table_meta.current_snapshot().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table has no current snapshot".to_string())
    })?;
    let manifest_list = io_load_manifest_list(store_ctx, snapshot.manifest_list()).await?;
    data_files_from_manifest_list(store_ctx, &manifest_list).await
}

async fn data_files_from_manifest_list(
    store_ctx: &StoreContext,
    manifest_list: &ManifestList,
) -> Result<Vec<DataFile>> {
    let mut out = Vec::new();
    for manifest_file in manifest_list
        .entries()
        .iter()
        .filter(|mf| mf.content == ManifestContentType::Data)
    {
        let manifest = io_load_manifest(store_ctx, manifest_file.manifest_path.as_str()).await?;
        for entry_ref in manifest.entries().iter() {
            let entry = entry_ref.as_ref();
            if !matches!(
                entry.status,
                ManifestStatus::Added | ManifestStatus::Existing
            ) {
                continue;
            }
            let mut data_file = entry.data_file.clone();
            data_file.partition_spec_id = manifest_file.partition_spec_id;
            out.push(data_file);
        }
    }
    Ok(out)
}

async fn write_position_delete_file(
    store_ctx: &StoreContext,
    table_url: &Url,
    data_dir: &str,
    target_file: &DataFile,
    positions: &BTreeSet<i64>,
) -> Result<DataFile> {
    let delete_schema = position_delete_arrow_schema();
    let file_paths = (0..positions.len())
        .map(|_| Some(target_file.file_path.as_str()))
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
        target_file.partition_spec_id,
        target_file.partition.clone(),
    )
    .await?;
    delete_file.content = DataContentType::PositionDeletes;
    delete_file.referenced_data_file = Some(target_file.file_path.clone());
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
