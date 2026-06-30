use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
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
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::datasource::type_converter::iceberg_field_to_arrow;
use crate::io::StoreContext;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::write::base_writer::DataFileWriter;
use crate::physical_plan::action_schema::{
    encode_commit_meta, encode_delete_data_files, iceberg_action_schema, CommitMeta,
};
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{
    DataContentType, DataFile, FormatVersion, Operation, TableMetadata, TableRequirement,
    MAIN_BRANCH,
};
use crate::table::metadata_loader::{
    load_metadata_file_bytes, metadata_location_to_object_path_string,
};
use crate::table_format::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
};

#[derive(Debug, Clone)]
pub struct IcebergEqualityDeleteWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    table_properties: Vec<(String, String)>,
    lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
    cache: Arc<PlanProperties>,
}

impl IcebergEqualityDeleteWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        table_properties: Vec<(String, String)>,
        lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
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
            table_url,
            table_properties,
            lakehouse_table,
            cache,
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergEqualityDeleteWriterExec {
    fn name(&self) -> &str {
        "IcebergEqualityDeleteWriterExec"
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
            return internal_err!("IcebergEqualityDeleteWriterExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.table_properties.clone(),
            self.lakehouse_table.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "IcebergEqualityDeleteWriterExec can only be executed in a single partition"
            );
        }
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "IcebergEqualityDeleteWriterExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let input = self.input.execute(0, Arc::clone(&context))?;
        let input_schema = self.input.schema();
        let table_url = self.table_url.clone();
        let table_properties = self.table_properties.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let output_schema = self.schema();
        let schema_for_adapter = output_schema.clone();

        let future = async move {
            let object_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;
            let table_meta =
                load_current_table_metadata(&store_ctx, &table_url, &table_properties).await?;
            let current_schema = table_meta.current_schema().ok_or_else(|| {
                DataFusionError::Plan(
                    "Iceberg table metadata is missing current schema".to_string(),
                )
            })?;
            let default_spec = table_meta
                .default_partition_spec()
                .cloned()
                .unwrap_or_else(crate::spec::PartitionSpec::unpartitioned_spec);

            // TODO: Prefer identifier/configured equality fields over full-row keys.
            let delete_fields = equality_delete_fields(current_schema, &input_schema)?;
            let delete_schema = Arc::new(Schema::new(
                delete_fields
                    .iter()
                    .map(|field| field.arrow_field.clone())
                    .collect::<Vec<_>>(),
            ));
            let equality_ids = delete_fields
                .iter()
                .map(|field| field.field_id)
                .collect::<Vec<_>>();

            let mut stream = input;
            let mut writer: Option<ArrowParquetWriter> = None;
            let mut total_rows = 0u64;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }
                if table_meta.format_version < FormatVersion::V2 {
                    return Err(DataFusionError::Plan(
                        "Iceberg equality delete writes require table format-version 2 or higher"
                            .to_string(),
                    ));
                }
                if !default_spec.is_unpartitioned() {
                    // TODO: Group rows by partition and write partition-scoped delete files.
                    return Err(DataFusionError::NotImplemented(
                        "Iceberg equality delete writes for partitioned tables are not supported yet"
                            .to_string(),
                    ));
                }

                let delete_batch =
                    project_delete_batch(&batch, &delete_fields, delete_schema.clone())?;
                let row_count = delete_batch.num_rows();
                total_rows = total_rows
                    .checked_add(u64::try_from(row_count).map_err(|e| {
                        DataFusionError::Execution(format!("Row count overflow: {e}"))
                    })?)
                    .ok_or_else(|| DataFusionError::Execution("Row count overflow".to_string()))?;

                let writer = match writer.as_mut() {
                    Some(writer) => writer,
                    None => {
                        writer = Some(
                            ArrowParquetWriter::try_new(
                                delete_schema.as_ref(),
                                WriterProperties::default(),
                            )
                            .map_err(DataFusionError::Execution)?,
                        );
                        writer.as_mut().expect("writer was initialized")
                    }
                };
                writer
                    .write_batch(&delete_batch)
                    .await
                    .map_err(DataFusionError::Execution)?;
            }

            let Some(writer) = writer else {
                return encode_delete_data_files(vec![]);
            };

            let delete_file = write_equality_delete_file(
                &store_ctx,
                &table_url,
                writer,
                default_spec.spec_id(),
                total_rows,
                equality_ids,
            )
            .await?;
            let requirements = commit_requirements(&table_meta);
            let commit_meta = CommitMeta {
                table_uri: table_url.to_string(),
                row_count: total_rows,
                operation: Operation::Delete,
                requirements,
                table_properties,
                lakehouse_table,
                schema: None,
                partition_spec: None,
            };

            let schema = iceberg_action_schema()?;
            let batches = vec![
                encode_delete_data_files(vec![delete_file])?,
                encode_commit_meta(commit_meta)?,
            ];
            concat_batches(&schema, &batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            once(future),
        )))
    }
}

impl DisplayAs for IcebergEqualityDeleteWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergEqualityDeleteWriterExec(table_path={})",
                    self.table_url
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
struct EqualityDeleteField {
    field_id: i32,
    name: String,
    arrow_field: FieldRef,
}

fn equality_delete_fields(
    iceberg_schema: &crate::spec::Schema,
    input_schema: &SchemaRef,
) -> Result<Vec<EqualityDeleteField>> {
    let mut fields = Vec::with_capacity(iceberg_schema.fields().len());
    for field in iceberg_schema.fields() {
        validate_equality_delete_type(&field.name, &field.field_type)?;
        let arrow_field = Arc::new(iceberg_field_to_arrow(field)?);
        let input_field = input_schema.field_with_name(&field.name).map_err(|_| {
            DataFusionError::Plan(format!(
                "Iceberg equality delete input is missing column '{}'",
                field.name
            ))
        })?;
        if input_field.data_type() != arrow_field.data_type() {
            return Err(DataFusionError::Plan(format!(
                "Iceberg equality delete column '{}' has input type {:?}, expected {:?}",
                field.name,
                input_field.data_type(),
                arrow_field.data_type()
            )));
        }
        fields.push(EqualityDeleteField {
            field_id: field.id,
            name: field.name.clone(),
            arrow_field,
        });
    }
    Ok(fields)
}

fn validate_equality_delete_type(name: &str, ty: &Type) -> Result<()> {
    match ty {
        // TODO: Add equality writer support for variant/geometry/geography encodings.
        Type::Primitive(
            PrimitiveType::Unknown
            | PrimitiveType::Variant
            | PrimitiveType::Geometry { .. }
            | PrimitiveType::Geography { .. },
        ) => Err(DataFusionError::NotImplemented(format!(
            "Iceberg equality delete writes for column '{name}' with type {ty} are not supported yet"
        ))),
        Type::Primitive(_) => Ok(()),
        // TODO: Support nested equality fields once projection preserves nested field IDs.
        Type::Struct(_) | Type::List(_) | Type::Map(_) => Err(DataFusionError::NotImplemented(
            format!(
                "Iceberg equality delete writes for nested column '{name}' with type {ty} are not supported yet"
            ),
        )),
    }
}

fn project_delete_batch(
    batch: &RecordBatch,
    delete_fields: &[EqualityDeleteField],
    delete_schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(delete_fields.len());
    for field in delete_fields {
        let col = batch.column_by_name(&field.name).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Iceberg equality delete batch is missing column '{}'",
                field.name
            ))
        })?;
        if col.data_type() != field.arrow_field.data_type() {
            return Err(DataFusionError::Internal(format!(
                "Iceberg equality delete column '{}' has batch type {:?}, expected {:?}",
                field.name,
                col.data_type(),
                field.arrow_field.data_type()
            )));
        }
        columns.push(col.clone());
    }
    RecordBatch::try_new(delete_schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

async fn load_current_table_metadata(
    store_ctx: &StoreContext,
    table_url: &Url,
    table_properties: &[(String, String)],
) -> Result<TableMetadata> {
    let metadata_file = if catalog_managed_iceberg_from_properties(table_properties) {
        match metadata_location_from_properties(table_properties) {
            Some(location) => metadata_location_to_object_path_string(&location)?,
            None => crate::table::find_latest_metadata_file(&store_ctx.base, table_url).await?,
        }
    } else {
        crate::table::find_latest_metadata_file(&store_ctx.base, table_url).await?
    };
    let bytes = load_metadata_file_bytes(&store_ctx.base, &metadata_file).await?;
    TableMetadata::from_json(&bytes).map_err(|e| DataFusionError::External(Box::new(e)))
}

async fn write_equality_delete_file(
    store_ctx: &StoreContext,
    table_url: &Url,
    writer: ArrowParquetWriter,
    partition_spec_id: i32,
    total_rows: u64,
    equality_ids: Vec<i32>,
) -> Result<DataFile> {
    let (bytes, meta) = writer.close().await.map_err(DataFusionError::Execution)?;

    // TODO: Respect write_data_path/write_folder_storage_path for delete files.
    let rel = format!("data/equality-delete-{}.parquet", uuid::Uuid::new_v4());
    let path = ObjectPath::from(rel.as_str());
    store_ctx
        .prefixed
        .put(&path, object_store::PutPayload::from(Bytes::from(bytes)))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let delete_file_path = crate::utils::join_table_uri(
        table_url.as_str(),
        &rel,
        &crate::utils::WritePathMode::Absolute,
    );

    let mut delete_file = DataFileWriter::new(partition_spec_id, delete_file_path, Vec::new())
        .finish(meta)
        .map_err(DataFusionError::Execution)?
        .data_file;
    delete_file.content = DataContentType::EqualityDeletes;
    delete_file.record_count = total_rows;
    delete_file.referenced_data_file = None;
    delete_file.sort_order_id = None;
    delete_file.equality_ids = equality_ids;
    Ok(delete_file)
}

fn commit_requirements(table_meta: &TableMetadata) -> Vec<TableRequirement> {
    vec![
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: table_meta.last_column_id,
        },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: table_meta.current_schema_id,
        },
        TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: table_meta.last_partition_id,
        },
        TableRequirement::DefaultSpecIdMatch {
            default_spec_id: table_meta.default_spec_id,
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: table_meta.current_snapshot_id.filter(|id| *id >= 0),
        },
    ]
}
