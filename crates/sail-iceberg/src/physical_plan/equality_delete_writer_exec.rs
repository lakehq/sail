use std::sync::Arc;

use async_trait::async_trait;
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
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::StreamExt;
use futures::stream::once;
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::datasource::type_converter::iceberg_field_to_arrow;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::physical_plan::action_schema::{
    CommitMeta, encode_commit_meta, encode_delete_data_files, iceberg_action_schema,
};
use crate::physical_plan::delete_writer_common::{self, IcebergDeleteWriterConfig};
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataContentType, DataFile, MAIN_BRANCH, TableMetadata, TableRequirement};

#[derive(Debug, Clone)]
pub struct IcebergEqualityDeleteWriterExec {
    input: Arc<dyn ExecutionPlan>,
    writer_config: IcebergDeleteWriterConfig,
    lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
    cache: Arc<PlanProperties>,
}

impl IcebergEqualityDeleteWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        table_properties: Vec<(String, String)>,
        write_data_path: Option<String>,
        write_folder_storage_path: Option<String>,
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
            writer_config: IcebergDeleteWriterConfig::new(
                table_url,
                table_properties,
                write_data_path,
                write_folder_storage_path,
            ),
            lakehouse_table,
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

    pub fn lakehouse_table(
        &self,
    ) -> Option<&sail_common_datafusion::catalog::LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
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
        // FIXME: Write equality deletes in parallel and roll files by target size once
        // writer commit metadata can be merged safely across output partitions.
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
            self.writer_config.table_url().clone(),
            self.writer_config.table_properties().to_vec(),
            self.writer_config
                .write_data_path()
                .map(ToString::to_string),
            self.writer_config
                .write_folder_storage_path()
                .map(ToString::to_string),
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
        let writer_config = self.writer_config.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let output_schema = self.schema();
        let schema_for_adapter = output_schema.clone();

        let future = async move {
            let store_ctx =
                delete_writer_common::store_context(&context, writer_config.table_url())?;
            let table_meta = writer_config
                .load_current_table_metadata(&store_ctx)
                .await?;
            let current_schema = table_meta.current_schema().ok_or_else(|| {
                DataFusionError::Plan(
                    "Iceberg table metadata is missing current schema".to_string(),
                )
            })?;
            let default_spec = table_meta
                .default_partition_spec()
                .cloned()
                .unwrap_or_else(crate::spec::PartitionSpec::unpartitioned_spec);
            ensure_unpartitioned_equality_delete_spec(&default_spec)?;
            let data_dir = writer_config.resolve_data_dir(&table_meta)?;

            // TODO: Prefer identifier/configured equality fields over full-row keys.
            let delete_spec = EqualityDeleteSpec::full_row(current_schema, &input_schema)?;

            let mut stream = input;
            let mut writer: Option<ArrowParquetWriter> = None;
            let mut total_rows = 0u64;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }
                delete_writer_common::ensure_equality_delete_writes(&table_meta)?;
                let delete_batch = delete_spec.project_batch(&batch)?;
                let row_count = delete_batch.num_rows();
                total_rows = total_rows
                    .checked_add(u64::try_from(row_count).map_err(|e| {
                        DataFusionError::Execution(format!("Row count overflow: {e}"))
                    })?)
                    .ok_or_else(|| DataFusionError::Execution("Row count overflow".to_string()))?;

                let writer = match writer.as_mut() {
                    Some(writer) => writer,
                    None => writer.insert(
                        ArrowParquetWriter::try_new(
                            delete_spec.arrow_schema.as_ref(),
                            WriterProperties::default(),
                        )
                        .map_err(DataFusionError::Execution)?,
                    ),
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
                writer_config.table_url(),
                &data_dir,
                writer,
                default_spec.spec_id(),
                total_rows,
                delete_spec.equality_ids.clone(),
            )
            .await?;
            let requirements = commit_requirements(&table_meta);
            let commit_meta = CommitMeta {
                table_uri: writer_config.table_url().to_string(),
                row_count: total_rows,
                requirements,
                table_properties: writer_config.table_properties().to_vec(),
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
                    self.writer_config.table_url()
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

#[derive(Debug, Clone)]
struct EqualityDeleteSpec {
    fields: Vec<EqualityDeleteField>,
    arrow_schema: SchemaRef,
    equality_ids: Vec<i32>,
}

impl EqualityDeleteSpec {
    fn full_row(iceberg_schema: &crate::spec::Schema, input_schema: &SchemaRef) -> Result<Self> {
        let fields = equality_delete_fields(iceberg_schema, input_schema)?;
        let arrow_schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|field| field.arrow_field.clone())
                .collect::<Vec<_>>(),
        ));
        let equality_ids = fields.iter().map(|field| field.field_id).collect();
        Ok(Self {
            fields,
            arrow_schema,
            equality_ids,
        })
    }

    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        project_delete_batch(batch, &self.fields, self.arrow_schema.clone())
    }
}

fn equality_delete_fields(
    iceberg_schema: &crate::spec::Schema,
    input_schema: &SchemaRef,
) -> Result<Vec<EqualityDeleteField>> {
    ensure_full_row_equality_delete_schema(iceberg_schema)?;
    let mut fields = Vec::with_capacity(iceberg_schema.fields().len());
    for field in iceberg_schema.fields() {
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

pub(crate) fn ensure_full_row_equality_delete_schema(
    iceberg_schema: &crate::spec::Schema,
) -> Result<()> {
    for field in iceberg_schema.fields() {
        validate_equality_delete_type(&field.name, &field.field_type)?;
    }
    Ok(())
}

pub(crate) fn ensure_full_row_equality_delete_preflight(table_meta: &TableMetadata) -> Result<()> {
    delete_writer_common::ensure_equality_delete_writes(table_meta)?;
    let default_spec = table_meta
        .default_partition_spec()
        .cloned()
        .unwrap_or_else(crate::spec::PartitionSpec::unpartitioned_spec);
    ensure_unpartitioned_equality_delete_spec(&default_spec)?;
    let current_schema = table_meta.current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    ensure_full_row_equality_delete_schema(current_schema)
}

fn ensure_unpartitioned_equality_delete_spec(
    partition_spec: &crate::spec::PartitionSpec,
) -> Result<()> {
    if partition_spec.is_unpartitioned() {
        Ok(())
    } else {
        Err(DataFusionError::NotImplemented(
            "Iceberg equality delete writes for partitioned tables are not supported yet"
                .to_string(),
        ))
    }
}

fn validate_equality_delete_type(name: &str, ty: &Type) -> Result<()> {
    match ty {
        Type::Primitive(PrimitiveType::Float | PrimitiveType::Double) => {
            Err(DataFusionError::Plan(format!(
                "Iceberg equality delete column '{name}' has identifier-field-invalid type {ty}; float and double cannot be equality delete keys"
            )))
        }
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
        Type::Struct(_) | Type::List(_) | Type::Map(_) => {
            Err(DataFusionError::NotImplemented(format!(
                "Iceberg equality delete writes for nested column '{name}' with type {ty} are not supported yet"
            )))
        }
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

async fn write_equality_delete_file(
    store_ctx: &crate::io::StoreContext,
    table_url: &Url,
    data_dir: &str,
    writer: ArrowParquetWriter,
    partition_spec_id: i32,
    total_rows: u64,
    equality_ids: Vec<i32>,
) -> Result<DataFile> {
    let mut delete_file = delete_writer_common::write_delete_parquet_file(
        store_ctx,
        table_url,
        data_dir,
        "equality-delete",
        writer,
        partition_spec_id,
        Vec::new(),
    )
    .await?;
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
