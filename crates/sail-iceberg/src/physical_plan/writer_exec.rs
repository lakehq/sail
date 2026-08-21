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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::{Column, Literal as PhysicalLiteral};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use futures::StreamExt;
use futures::stream::once;
use parquet::file::properties::WriterProperties;
use sail_common_datafusion::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, PhysicalSinkMode,
};
use url::Url;

use crate::io::StoreContext;
use crate::operations::write::config::WriterConfig;
use crate::operations::write::table_writer::IcebergTableWriter;
use crate::physical_plan::action_schema::{
    CommitMeta, encode_add_data_files, encode_commit_meta, encode_delete_data_files,
    iceberg_action_schema,
};
use crate::physical_plan::merge_row_projection::IcebergMergeRowProjection;
use crate::physical_plan::partition_transform_expr::IcebergPartitionTransformExpr;
use crate::physical_plan::position_delete_writer::PositionDeleteAccumulator;
use crate::physical_plan::write_context::IcebergWriteContext;
use crate::physical_plan::writer_options::IcebergWriterExecOptions;
use crate::row_level_metadata::{MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};
use crate::spec::FormatVersion;
use crate::spec::transform::Transform;
use crate::utils::get_object_store_from_context;
use crate::utils::partition_transform::iceberg_transform_from_partition_field;

#[derive(Debug)]
pub struct IcebergWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<CatalogPartitionField>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    options: IcebergWriterExecOptions,
    write_context: IcebergWriteContext,
    merge_row_intents: bool,
    merge_distribution_keys: Option<Vec<Arc<dyn PhysicalExpr>>>,
    cache: Arc<PlanProperties>,
}

impl IcebergWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<CatalogPartitionField>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        options: IcebergWriterExecOptions,
        write_context: IcebergWriteContext,
    ) -> Result<Self> {
        write_context.validate_table_state(table_exists)?;
        let schema = match iceberg_action_schema() {
            Ok(s) => s,
            Err(e) => {
                log::error!("failed to initialize iceberg action schema: {e}");
                Arc::new(datafusion::arrow::datatypes::Schema::empty())
            }
        };
        let output_partitions = input.output_partitioning().partition_count().max(1);
        let cache = Self::compute_properties(schema.clone(), output_partitions);
        Ok(Self {
            input,
            table_url,
            partition_columns,
            sink_mode,
            table_exists,
            options,
            write_context,
            merge_row_intents: false,
            merge_distribution_keys: None,
            cache,
        })
    }

    pub fn new_merge(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<CatalogPartitionField>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        options: IcebergWriterExecOptions,
        write_context: IcebergWriteContext,
    ) -> Result<Self> {
        let merge_distribution_keys =
            Self::merge_distribution_keys(input.schema().as_ref(), &partition_columns)?;
        let mut writer = Self::new(
            input,
            table_url,
            partition_columns,
            sink_mode,
            table_exists,
            options,
            write_context,
        )?;
        writer.merge_row_intents = true;
        writer.merge_distribution_keys = Some(merge_distribution_keys);
        Ok(writer)
    }

    fn merge_distribution_keys(
        input_schema: &Schema,
        partition_columns: &[CatalogPartitionField],
    ) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        fn required_column(input_schema: &Schema, name: &str) -> Result<Arc<dyn PhysicalExpr>> {
            let index = input_schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "Iceberg MERGE writer requires input column '{name}' for hash distribution"
                ))
            })?;
            Ok(Arc::new(Column::new(name, index)))
        }

        let spec_id_index = input_schema.index_of(MERGE_PARTITION_SPEC_ID_COLUMN).ok();
        let partition_index = input_schema.index_of(MERGE_PARTITION_COLUMN).ok();
        let (mut distribution_keys, has_delete_metadata) = match (spec_id_index, partition_index) {
            (Some(spec_id_index), Some(partition_index)) => (
                vec![
                    Arc::new(Column::new(MERGE_PARTITION_SPEC_ID_COLUMN, spec_id_index))
                        as Arc<dyn PhysicalExpr>,
                    Arc::new(Column::new(MERGE_PARTITION_COLUMN, partition_index))
                        as Arc<dyn PhysicalExpr>,
                ],
                true,
            ),
            (None, None) => (
                vec![
                    Arc::new(PhysicalLiteral::new(ScalarValue::Int32(None)))
                        as Arc<dyn PhysicalExpr>,
                    Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(None)))
                        as Arc<dyn PhysicalExpr>,
                ],
                false,
            ),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Iceberg MERGE writer requires both '{MERGE_PARTITION_SPEC_ID_COLUMN}' and \
                     '{MERGE_PARTITION_COLUMN}' when either delete metadata column is present"
                )));
            }
        };
        if partition_columns.is_empty() {
            if has_delete_metadata {
                distribution_keys.push(required_column(input_schema, MERGE_FILE_COLUMN)?);
            } else {
                distribution_keys.push(Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(None))));
            }
            return Ok(distribution_keys);
        }

        for partition_field in partition_columns {
            let source = required_column(input_schema, &partition_field.column)?;
            let transform = iceberg_transform_from_partition_field(partition_field);
            if transform == Transform::Identity {
                distribution_keys.push(source);
            } else {
                let expression: Arc<dyn PhysicalExpr> =
                    Arc::new(IcebergPartitionTransformExpr::new(source, transform));
                expression.data_type(input_schema)?;
                distribution_keys.push(expression);
            }
        }
        Ok(distribution_keys)
    }

    fn compute_properties(
        schema: datafusion::arrow::datatypes::SchemaRef,
        output_partitions: usize,
    ) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn partition_columns(&self) -> &[CatalogPartitionField] {
        &self.partition_columns
    }

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn options(&self) -> &IcebergWriterExecOptions {
        &self.options
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.options.lakehouse_table.as_ref()
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn write_context(&self) -> &IcebergWriteContext {
        &self.write_context
    }

    pub fn reads_merge_row_intents(&self) -> bool {
        self.merge_row_intents
    }
}

#[async_trait]
impl ExecutionPlan for IcebergWriterExec {
    fn name(&self) -> &'static str {
        "IcebergWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match &self.merge_distribution_keys {
            Some(expressions) => vec![Distribution::HashPartitioned(expressions.clone())],
            None => vec![Distribution::UnspecifiedDistribution],
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergWriterExec requires exactly one child");
        }
        let input = Arc::clone(&children[0]);
        let writer = if self.merge_row_intents {
            Self::new_merge(
                input,
                self.table_url.clone(),
                self.partition_columns.clone(),
                self.sink_mode.clone(),
                self.table_exists,
                self.options.clone(),
                self.write_context.clone(),
            )?
        } else {
            Self::new(
                input,
                self.table_url.clone(),
                self.partition_columns.clone(),
                self.sink_mode.clone(),
                self.table_exists,
                self.options.clone(),
                self.write_context.clone(),
            )?
        };
        Ok(Arc::new(writer))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions == 0 {
            return internal_err!("IcebergWriterExec requires at least one input partition");
        }
        if partition >= input_partitions {
            return internal_err!(
                "IcebergWriterExec invalid partition {partition} (input partitions: {input_partitions})"
            );
        }

        let stream = self.input.execute(partition, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let partition_columns = self.partition_columns.clone();
        let sink_mode = self.sink_mode.clone();
        let table_exists = self.table_exists;
        let merge_projection = self
            .merge_row_intents
            .then(|| IcebergMergeRowProjection::try_new(self.input.schema()))
            .transpose()?;
        let writes_position_deletes = merge_projection.is_some()
            && self
                .input
                .schema()
                .field_with_name(MERGE_ROW_INDEX_COLUMN)
                .is_ok();
        let options = self.options.clone();
        let write_context = self.write_context.clone();

        let schema = self.schema();
        let future = async move {
            match sink_mode {
                PhysicalSinkMode::ErrorIfExists => {
                    if table_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg table already exists at path: {}",
                            table_url
                        )));
                    }
                }
                PhysicalSinkMode::IgnoreIfExists => {
                    if table_exists {
                        return Ok(RecordBatch::new_empty(schema.clone()));
                    }
                }
                PhysicalSinkMode::Append => {}
                PhysicalSinkMode::Overwrite
                | PhysicalSinkMode::OverwriteIf { .. }
                | PhysicalSinkMode::OverwritePartitions => {}
            }

            let data_location = write_context.data_location()?;
            let table_schema = write_context.writer_arrow_schema()?;
            let iceberg_schema = write_context.writer_schema.clone();
            let spec_id_val = write_context.writer_partition_spec_id();
            let variant_shredding = write_context.variant_shredding.clone();
            let base_table_context = write_context.base_table.as_ref();

            let writer_config = WriterConfig {
                table_schema: table_schema.clone(),
                partition_columns: partition_columns.clone(),
                writer_properties: WriterProperties::default(),
                target_file_size: 134_217_728,
                write_batch_size: 32 * 1024,
                num_indexed_cols: 32,
                stats_columns: None,
                iceberg_schema: Arc::new(iceberg_schema.clone()),
                partition_spec: write_context.unbound_writer_partition_spec(),
                variant_shredding,
            };

            let data_object_store = get_object_store_from_context(&context, &data_location)?;
            let writer_root = crate::utils::url_to_object_path(&data_location)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            let mut writer = IcebergTableWriter::new(
                data_object_store.clone(),
                writer_root,
                writer_config,
                spec_id_val,
                data_location.clone(),
            );

            let mut position_deletes = if writes_position_deletes {
                let base_table_context = base_table_context.ok_or_else(|| {
                    DataFusionError::Internal(
                        "Iceberg MERGE position deletes require base table state".to_string(),
                    )
                })?;
                match base_table_context.format_version {
                    FormatVersion::V1 => {
                        return Err(DataFusionError::Plan(
                            "Iceberg position delete writes require table format-version 2"
                                .to_string(),
                        ));
                    }
                    FormatVersion::V2 => {}
                    FormatVersion::V3 => {
                        return Err(DataFusionError::NotImplemented(
                            "Iceberg v3 MERGE MOR position delete writes are not supported; v3 requires deletion vectors".to_string(),
                        ));
                    }
                }
                Some(PositionDeleteAccumulator::try_new(base_table_context)?)
            } else {
                None
            };

            let mut total_rows = 0u64;
            let mut data = stream;
            while let Some(batch_result) = data.next().await {
                let input_batch = batch_result?;
                let batch = if let Some(merge_projection) = &merge_projection {
                    if let Some(position_deletes) = &mut position_deletes {
                        let base_table_context = base_table_context.ok_or_else(|| {
                            DataFusionError::Internal(
                                "Iceberg MERGE position deletes require base table state"
                                    .to_string(),
                            )
                        })?;
                        let delete_rows =
                            merge_projection.project_position_delete_rows(&input_batch)?;
                        position_deletes.add_batch(
                            base_table_context,
                            &delete_rows,
                            MERGE_FILE_COLUMN,
                            MERGE_ROW_INDEX_COLUMN,
                        )?;
                    }
                    merge_projection.project_data_rows(&input_batch)?
                } else {
                    input_batch
                };
                let batch_row_count = batch.num_rows();
                if batch_row_count == 0 {
                    continue;
                }
                total_rows += u64::try_from(batch_row_count).map_err(|e| {
                    DataFusionError::Execution(format!("Row count overflow: {}", e))
                })?;
                writer
                    .write(&batch)
                    .await
                    .map_err(DataFusionError::Execution)?;
            }

            let data_files = writer.close().await.map_err(DataFusionError::Execution)?;
            let delete_files = if let Some(position_deletes) = position_deletes {
                let data_store_ctx = StoreContext::new(data_object_store, &data_location)?;
                position_deletes
                    .finish(&data_store_ctx, &data_location)
                    .await?
            } else {
                Vec::new()
            };

            let commit_meta = CommitMeta {
                table_uri: table_url.to_string(),
                row_count: total_rows,
                requirements: write_context.requirements.clone(),
                table_properties: options.table_properties,
                lakehouse_table: options.lakehouse_table,
                schema: write_context
                    .commit_writer_schema
                    .then(|| write_context.writer_schema.clone()),
                partition_spec: write_context
                    .commit_writer_partition_spec
                    .then(|| write_context.writer_partition_spec.clone())
                    .flatten(),
            };

            let schema = iceberg_action_schema()?;
            let batches = vec![
                encode_add_data_files(data_files)?,
                encode_delete_data_files(delete_files)?,
                encode_commit_meta(commit_meta)?,
            ];
            let batch = concat_batches(&schema, &batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergWriterExec(table_path={}", self.table_url)?;
                if self.merge_row_intents {
                    write!(f, ", merge_row_intents=true")?;
                }
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)?;
                if self.merge_row_intents {
                    write!(f, ", merge_row_intents=true")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use object_store::ObjectStore;
    use sail_common_datafusion::catalog::PartitionTransform;
    use sail_common_datafusion::datasource::{MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN};

    use super::*;
    use crate::operations::bootstrap::{NewTableMetadataStyle, bootstrap_empty_table_metadata};
    use crate::physical_plan::action_schema::decode_actions_and_meta_from_batch;
    use crate::spec::PartitionSpec;
    use crate::spec::types::{NestedField, PrimitiveType, Type};

    fn merge_input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(MERGE_FILE_COLUMN, DataType::Utf8, true),
            Field::new(MERGE_ROW_INDEX_COLUMN, DataType::Int64, true),
            Field::new(MERGE_PARTITION_SPEC_ID_COLUMN, DataType::Int32, true),
            Field::new(MERGE_PARTITION_COLUMN, DataType::Utf8, true),
            Field::new(OPERATION_COLUMN, DataType::Int32, false),
            Field::new(MERGE_SOURCE_METRIC_COLUMN, DataType::Int64, true),
        ]))
    }

    fn iceberg_writer(
        merge_row_intents: bool,
        partition_columns: Vec<CatalogPartitionField>,
    ) -> IcebergWriterExec {
        iceberg_writer_for_schema(merge_row_intents, partition_columns, merge_input_schema())
    }

    fn iceberg_writer_for_schema(
        merge_row_intents: bool,
        partition_columns: Vec<CatalogPartitionField>,
        input_schema: SchemaRef,
    ) -> IcebergWriterExec {
        let writer_input_schema = if merge_row_intents {
            IcebergMergeRowProjection::try_new(Arc::clone(&input_schema))
                .expect("merge projection")
                .data_schema()
        } else {
            Arc::clone(&input_schema)
        };
        let input = Arc::new(EmptyExec::new(input_schema));
        let table_url = Url::parse("file:///tmp/table/").expect("table URL");
        let options = IcebergWriterExecOptions::default();
        let write_context = crate::physical_plan::prepare_iceberg_write_context(
            &table_url,
            None,
            &options,
            &partition_columns,
            &PhysicalSinkMode::Append,
            writer_input_schema.as_ref(),
        )
        .expect("write context");
        if merge_row_intents {
            IcebergWriterExec::new_merge(
                input,
                table_url,
                partition_columns,
                PhysicalSinkMode::Append,
                false,
                options,
                write_context,
            )
            .expect("merge writer")
        } else {
            IcebergWriterExec::new(
                input,
                table_url,
                partition_columns,
                PhysicalSinkMode::Append,
                false,
                options,
                write_context,
            )
            .expect("writer")
        }
    }

    fn assert_column(expression: &Arc<dyn PhysicalExpr>, name: &str, index: usize) {
        let column = expression
            .downcast_ref::<Column>()
            .expect("physical column");
        assert_eq!(column.name(), name);
        assert_eq!(column.index(), index);
    }

    fn assert_literal(expression: &Arc<dyn PhysicalExpr>, expected: ScalarValue) {
        let literal = expression
            .downcast_ref::<PhysicalLiteral>()
            .expect("physical literal");
        assert_eq!(literal.value(), &expected);
    }

    fn hash_expressions(distributions: &[Distribution]) -> &[Arc<dyn PhysicalExpr>] {
        assert_eq!(distributions.len(), 1);
        distributions
            .first()
            .and_then(|distribution| match distribution {
                Distribution::HashPartitioned(expressions) => Some(expressions.as_slice()),
                _ => None,
            })
            .expect("MERGE should require hash partitioning")
    }

    #[test]
    fn unpartitioned_merge_hashes_file_delete_keys() {
        let distributions = iceberg_writer(true, vec![]).required_input_distribution();
        let expressions = hash_expressions(&distributions);

        assert_eq!(expressions.len(), 3);
        assert_column(&expressions[0], MERGE_PARTITION_SPEC_ID_COLUMN, 4);
        assert_column(&expressions[1], MERGE_PARTITION_COLUMN, 5);
        assert_column(&expressions[2], MERGE_FILE_COLUMN, 2);
    }

    #[test]
    fn partitioned_merge_hashes_table_partition_transforms() {
        let partition_columns = vec![
            CatalogPartitionField {
                column: "id".to_string(),
                transform: None,
            },
            CatalogPartitionField {
                column: "event_time".to_string(),
                transform: Some(PartitionTransform::Day),
            },
        ];
        let distributions = iceberg_writer(true, partition_columns).required_input_distribution();
        let expressions = hash_expressions(&distributions);

        assert_eq!(expressions.len(), 4);
        assert_column(&expressions[0], MERGE_PARTITION_SPEC_ID_COLUMN, 4);
        assert_column(&expressions[1], MERGE_PARTITION_COLUMN, 5);
        assert_column(&expressions[2], "id", 0);
        let transform = expressions[3]
            .downcast_ref::<IcebergPartitionTransformExpr>()
            .expect("Iceberg day transform");
        assert_eq!(transform.transform(), Transform::Day);
        assert_column(transform.input(), "event_time", 1);
    }

    #[test]
    fn unpartitioned_insert_only_merge_hashes_null_file_delete_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(OPERATION_COLUMN, DataType::Int32, false),
        ]));
        let distributions =
            iceberg_writer_for_schema(true, vec![], schema).required_input_distribution();
        let expressions = hash_expressions(&distributions);

        assert_eq!(expressions.len(), 3);
        assert_literal(&expressions[0], ScalarValue::Int32(None));
        assert_literal(&expressions[1], ScalarValue::Utf8(None));
        assert_literal(&expressions[2], ScalarValue::Utf8(None));
    }

    #[test]
    fn partitioned_insert_only_merge_hashes_null_metadata_and_transform() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(OPERATION_COLUMN, DataType::Int32, false),
        ]));
        let partition_columns = vec![CatalogPartitionField {
            column: "event_time".to_string(),
            transform: Some(PartitionTransform::Day),
        }];
        let distributions = iceberg_writer_for_schema(true, partition_columns, schema)
            .required_input_distribution();
        let expressions = hash_expressions(&distributions);

        assert_eq!(expressions.len(), 3);
        assert_literal(&expressions[0], ScalarValue::Int32(None));
        assert_literal(&expressions[1], ScalarValue::Utf8(None));
        let transform = expressions[2]
            .downcast_ref::<IcebergPartitionTransformExpr>()
            .expect("Iceberg day transform");
        assert_eq!(transform.transform(), Transform::Day);
        assert_column(transform.input(), "event_time", 1);
    }

    #[test]
    fn ordinary_writes_preserve_upstream_distribution() {
        assert!(matches!(
            iceberg_writer(false, vec![])
                .required_input_distribution()
                .as_slice(),
            [Distribution::UnspecifiedDistribution]
        ));
    }

    #[test]
    fn writers_use_pinned_metadata_without_worker_metadata_reads() {
        futures::executor::block_on(async {
            let table_url =
                Url::parse("file:///tmp/iceberg-pinned-writer-context/").expect("table URL");
            let planning_store: Arc<dyn ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let planning_store_context =
                StoreContext::new(planning_store, &table_url).expect("planning store context");
            let iceberg_schema = crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("Iceberg schema");
            let table_properties = vec![("format-version".to_string(), "2".to_string())];
            let bootstrap = bootstrap_empty_table_metadata(
                &table_url,
                &planning_store_context,
                iceberg_schema.clone(),
                PartitionSpec::unpartitioned_spec(),
                &table_properties,
                NewTableMetadataStyle::Hadoop,
            )
            .await
            .expect("base metadata");
            let input_schema = Arc::new(
                crate::datasource::type_converter::iceberg_schema_to_arrow(&iceberg_schema)
                    .expect("Arrow schema"),
            );
            let options = IcebergWriterExecOptions::default();
            let write_context = crate::physical_plan::prepare_iceberg_write_context(
                &table_url,
                Some(&bootstrap.table_metadata),
                &options,
                &[],
                &PhysicalSinkMode::Append,
                input_schema.as_ref(),
            )
            .expect("write context");
            let expected_requirements = write_context.requirements.clone();
            let equality_write_context = write_context.clone();
            let writer = IcebergWriterExec::new(
                Arc::new(EmptyExec::new(Arc::clone(&input_schema))),
                table_url.clone(),
                vec![],
                PhysicalSinkMode::Append,
                true,
                options,
                write_context,
            )
            .expect("writer");

            let worker_store: Arc<dyn ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let session = SessionContext::new();
            session.runtime_env().register_object_store(
                &Url::parse("file:///").expect("object store URL"),
                worker_store,
            );

            let mut output = writer
                .execute(0, session.task_ctx())
                .expect("writer output stream");
            let batch = output
                .next()
                .await
                .expect("writer output")
                .expect("writer batch");
            let (_, _, commit_meta) =
                decode_actions_and_meta_from_batch(&batch).expect("commit metadata");
            assert_eq!(
                commit_meta.expect("commit metadata action").requirements,
                expected_requirements
            );

            let equality_writer = crate::physical_plan::IcebergEqualityDeleteWriterExec::new(
                Arc::new(EmptyExec::new(input_schema)),
                table_url,
                table_properties,
                None,
                None,
                equality_write_context,
                None,
            )
            .expect("equality delete writer");
            let mut equality_output = equality_writer
                .execute(0, session.task_ctx())
                .expect("equality writer output stream");
            equality_output
                .next()
                .await
                .expect("equality writer output")
                .expect("equality writer batch");
        });
    }
}
