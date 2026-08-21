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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, new_null_array};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::schema_evolution::{
    StructFieldMatching, cast_array_for_schema_evolution_write_relaxed_tz,
};
use url::Url;

use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::write::base_writer::DataFileWriter;
use crate::operations::write::config::WriterConfig;
use crate::operations::write::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use crate::operations::write::partition::split_record_batch_by_partition;
use crate::operations::write::variant_shredding::{
    VariantShreddingPlan, apply_variant_shredding_plan, build_variant_shredding_plan,
    unshred_shredded_variants_for_write,
};
use crate::spec::DataFile;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::NestedField;
use crate::spec::types::values::Literal;
use crate::utils::conversions::to_scalar;

enum PartitionWriterState {
    Pending {
        batches: Vec<RecordBatch>,
        num_rows: usize,
    },
    Open {
        writer: Box<ArrowParquetWriter>,
        variant_shredding_plan: Option<VariantShreddingPlan>,
    },
}

struct PartitionWriter {
    partition_dir: String,
    state: PartitionWriterState,
}

pub struct IcebergTableWriter {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    pub data_url: Url,
    // Typed partition tuple -> writer.
    writers: HashMap<Vec<Option<Literal>>, PartitionWriter>,
    written: Vec<DataFile>,
    pub partition_spec_id: i32,
}

impl IcebergTableWriter {
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        root: ObjectPath,
        config: WriterConfig,
        partition_spec_id: i32,
        data_url: Url,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new(root),
            store,
            config,
            data_url,
            writers: HashMap::new(),
            written: Vec::new(),
            partition_spec_id,
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let spec = &self.config.partition_spec;
        let iceberg_schema = &self.config.iceberg_schema;
        let padded = Self::align_batch_with_table_schema(
            batch,
            &self.config.table_schema,
            self.config.iceberg_schema.as_ref(),
        )
        .map_err(|e| e.to_string())?;
        let normalized = unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
        let aligned = Self::cast_batch_for_table_write(&normalized, &self.config.table_schema)
            .map_err(|e| e.to_string())?;
        Self::validate_required_columns(aligned.columns(), &self.config.table_schema)?;

        if spec.fields.is_empty() {
            // Unpartitioned: write as-is once
            let partition_dir = String::new();
            let partition_values = Vec::new();
            self.write_aligned_batch(partition_values, partition_dir, aligned)
                .await?;
            return Ok(());
        }

        let parts = split_record_batch_by_partition(&aligned, spec, iceberg_schema)?;
        for p in parts.into_iter() {
            let partition_dir = p.partition_dir;
            let partition_values = p.partition_values;
            self.write_aligned_batch(partition_values, partition_dir, p.record_batch)
                .await?;
        }

        Ok(())
    }

    async fn write_aligned_batch(
        &mut self,
        partition_values: Vec<Option<Literal>>,
        partition_dir: String,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let (partition_dir, state) = match self.writers.remove(&partition_values) {
            Some(writer) => (writer.partition_dir, writer.state),
            None => (partition_dir, self.new_partition_writer_state()?),
        };
        let state = self.write_partition_state(state, batch).await?;
        self.writers.insert(
            partition_values,
            PartitionWriter {
                partition_dir,
                state,
            },
        );
        Ok(())
    }

    fn new_partition_writer_state(&self) -> Result<PartitionWriterState, String> {
        if self.config.variant_shredding.enabled {
            Ok(PartitionWriterState::Pending {
                batches: Vec::new(),
                num_rows: 0,
            })
        } else {
            Ok(PartitionWriterState::Open {
                writer: Box::new(self.new_arrow_writer(self.config.table_schema.clone())?),
                variant_shredding_plan: None,
            })
        }
    }

    async fn write_partition_state(
        &mut self,
        state: PartitionWriterState,
        batch: RecordBatch,
    ) -> Result<PartitionWriterState, String> {
        match state {
            PartitionWriterState::Pending {
                mut batches,
                mut num_rows,
            } => {
                num_rows += batch.num_rows();
                batches.push(batch);
                if num_rows >= self.config.variant_shredding.inference_buffer_size.max(1) {
                    self.open_and_write_pending_batches(batches).await
                } else {
                    Ok(PartitionWriterState::Pending { batches, num_rows })
                }
            }
            PartitionWriterState::Open {
                mut writer,
                variant_shredding_plan,
            } => {
                let batch = if let Some(plan) = variant_shredding_plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)?
                } else {
                    batch
                };
                writer.write_batch(&batch).await?;
                Ok(PartitionWriterState::Open {
                    writer,
                    variant_shredding_plan,
                })
            }
        }
    }

    async fn open_and_write_pending_batches(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<PartitionWriterState, String> {
        let plan = build_variant_shredding_plan(
            &self.config.table_schema,
            &batches,
            self.config.variant_shredding.inference_buffer_size,
            self.config.variant_shredding.inference_node_budget,
        )?;
        let plan = (!plan.is_noop()).then_some(plan);
        let physical_batches = batches
            .into_iter()
            .map(|batch| {
                if let Some(plan) = plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)
                } else {
                    Ok(batch)
                }
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;

        let schema = physical_batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| self.config.table_schema.clone());
        let mut writer = self.new_arrow_writer(schema)?;
        for batch in physical_batches {
            writer.write_batch(&batch).await?;
        }
        Ok(PartitionWriterState::Open {
            writer: Box::new(writer),
            variant_shredding_plan: plan,
        })
    }

    fn new_arrow_writer(&self, schema: SchemaRef) -> Result<ArrowParquetWriter, String> {
        for (i, f) in schema.fields().iter().enumerate() {
            log::trace!(
                "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                i,
                f.name(),
                f.data_type(),
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)
            );
        }
        ArrowParquetWriter::try_new(schema.as_ref(), self.config.writer_properties.clone())
    }

    async fn finish_partition_state(
        &mut self,
        state: PartitionWriterState,
    ) -> Result<ArrowParquetWriter, String> {
        match state {
            PartitionWriterState::Pending { batches, .. } => {
                let PartitionWriterState::Open { writer, .. } =
                    self.open_and_write_pending_batches(batches).await?
                else {
                    return Err("failed to open pending Iceberg partition writer".to_string());
                };
                Ok(*writer)
            }
            PartitionWriterState::Open { writer, .. } => Ok(*writer),
        }
    }

    async fn flush_partition(
        &mut self,
        state: PartitionWriterState,
        partition_dir: &str,
        partition_values: Vec<Option<Literal>>,
    ) -> Result<(), String> {
        let writer = self.finish_partition_state(state).await?;
        let (bytes, meta) = writer.close().await?;
        let (rel, full) = self.generator.with_partition_dir(Some(partition_dir));
        log::trace!("iceberg.table_writer.flush_partition.writing: {}", full);
        self.store
            .put(&full, object_store::PutPayload::from(bytes))
            .await
            .map_err(|e| e.to_string())?;
        log::trace!(
            "iceberg.table_writer.flush_partition.written: rel={} full={}",
            rel,
            full
        );
        // Prevent a leading partition segment containing ':' from being parsed as a URI scheme.
        let file_path = match self.data_url.join(&format!("./{rel}")) {
            Ok(u) => u.to_string(),
            Err(_) => {
                format!("{}{}", self.data_url.as_str(), rel)
            }
        };
        let df = DataFileWriter::new(self.partition_spec_id, file_path, partition_values)
            .finish(meta)?
            .data_file;
        self.written.push(df);
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        for (partition_values, writer) in std::mem::take(&mut self.writers) {
            self.flush_partition(writer.state, &writer.partition_dir, partition_values)
                .await?;
        }
        Ok(self.written)
    }

    fn align_batch_with_table_schema(
        batch: &RecordBatch,
        table_schema: &SchemaRef,
        iceberg_schema: &IcebergSchema,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
        let mut schema_fields: Vec<FieldRef> = Vec::with_capacity(table_schema.fields().len());

        for field in table_schema.fields() {
            match batch.schema().index_of(field.name()) {
                Ok(idx) => {
                    columns.push(batch.column(idx).clone());
                    schema_fields.push(Arc::new(batch.schema().field(idx).clone()));
                }
                Err(_) => {
                    let array =
                        Self::build_missing_column_array(field, iceberg_schema, batch.num_rows())?;
                    columns.push(array);
                    schema_fields.push(field.clone());
                }
            }
        }

        let aligned_schema = Arc::new(Schema::new(schema_fields));
        Ok(RecordBatch::try_new(aligned_schema, columns)?)
    }

    fn build_missing_column_array(
        field: &FieldRef,
        iceberg_schema: &IcebergSchema,
        num_rows: usize,
    ) -> Result<ArrayRef, DataFusionError> {
        let iceberg_field = iceberg_schema.field_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Column '{}' missing from Iceberg schema during alignment",
                field.name()
            ))
        })?;

        if let Some(array) = Self::default_array_for_field(iceberg_field.as_ref(), num_rows)? {
            return Ok(array);
        }

        if field.is_nullable() {
            return Ok(new_null_array(field.data_type(), num_rows));
        }

        Err(DataFusionError::Plan(format!(
            "Column '{}' is required but missing in input batch and has no default value",
            field.name()
        )))
    }

    fn default_array_for_field(
        field: &NestedField,
        num_rows: usize,
    ) -> Result<Option<ArrayRef>, DataFusionError> {
        let literal = field
            .write_default
            .as_ref()
            .or(field.initial_default.as_ref());
        if let Some(lit) = literal {
            let scalar = to_scalar(lit, field.field_type.as_ref())?;
            let array = scalar
                .to_array_of_size(num_rows)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            return Ok(Some(array));
        }
        Ok(None)
    }

    fn cast_batch_for_table_write(
        batch: &RecordBatch,
        table_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(table_schema.fields().len());
        for field in table_schema.fields() {
            let source = match batch.schema().index_of(field.name()) {
                Ok(index) => batch.column(index),
                Err(_) if field.is_nullable() => {
                    columns.push(new_null_array(field.data_type(), batch.num_rows()));
                    continue;
                }
                Err(_) => {
                    return Err(DataFusionError::Plan(format!(
                        "Missing required column '{}' in input batch",
                        field.name()
                    )));
                }
            };
            columns.push(cast_array_for_schema_evolution_write_relaxed_tz(
                source,
                field,
                &DEFAULT_CAST_OPTIONS,
                StructFieldMatching::Name,
            )?);
        }

        Self::validate_required_columns(&columns, table_schema)
            .map_err(DataFusionError::Execution)?;

        if columns.is_empty() {
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(table_schema),
                columns,
                &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
            )?)
        } else {
            Ok(RecordBatch::try_new(Arc::clone(table_schema), columns)?)
        }
    }

    fn validate_required_columns(
        columns: &[ArrayRef],
        table_schema: &SchemaRef,
    ) -> Result<(), String> {
        for (field, column) in table_schema.fields().iter().zip(columns) {
            if !field.is_nullable() && column.null_count() > 0 {
                return Err(format!(
                    "Column '{}' is required but contains {} null value(s)",
                    field.name(),
                    column.null_count()
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used)]

    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, BinaryViewArray, Int32Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use object_store::memory::InMemory;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::partition::UnboundPartitionSpec;
    use crate::spec::types::{PrimitiveType, Type};

    fn fixed_size_writer(required: bool) -> IcebergTableWriter {
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::new(
                1,
                "value",
                Type::Primitive(PrimitiveType::Fixed(4)),
                required,
            ))])
            .build()
            .expect("Iceberg schema");
        let table_schema = Arc::new(
            iceberg_schema_to_arrow(&iceberg_schema).expect("Iceberg writer Arrow schema"),
        );
        let config = WriterConfig {
            table_schema,
            partition_columns: vec![],
            writer_properties: WriterProperties::default(),
            target_file_size: 1024,
            write_batch_size: 1024,
            num_indexed_cols: 1,
            stats_columns: None,
            iceberg_schema: Arc::new(iceberg_schema),
            partition_spec: UnboundPartitionSpec { fields: vec![] },
            variant_shredding: Default::default(),
        };
        IcebergTableWriter::new(
            Arc::new(InMemory::new()),
            ObjectPath::from("table/data"),
            config,
            0,
            Url::parse("file:///table/data/").expect("data URL"),
        )
    }

    fn binary_view_batch(values: Vec<Option<&'static [u8]>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::BinaryView,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(BinaryViewArray::from(values))])
            .expect("binary view batch")
    }

    #[test]
    fn writer_casts_binary_view_to_fixed_size_binary() {
        futures::executor::block_on(async {
            let mut writer = fixed_size_writer(true);
            writer
                .write(&binary_view_batch(vec![Some(b"1234")]))
                .await
                .expect("matching fixed-width value should write");

            let files = writer.close().await.expect("written data file");
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].record_count, 1);
        });
    }

    #[test]
    fn writer_rejects_wrong_length_binary_view_for_optional_fixed_size_field() {
        futures::executor::block_on(async {
            let mut writer = fixed_size_writer(false);
            let result = writer.write(&binary_view_batch(vec![Some(b"bad")])).await;

            let error = result.expect_err("wrong fixed width must fail");
            assert!(error.contains("length 3"), "unexpected error: {error}");
            assert!(writer.writers.is_empty());
        });
    }

    #[test]
    fn writer_rejects_null_for_required_fixed_size_field() {
        futures::executor::block_on(async {
            let mut writer = fixed_size_writer(true);
            let result = writer.write(&binary_view_batch(vec![None])).await;

            let error = result.expect_err("required null must fail");
            assert!(error.contains("required"), "unexpected error: {error}");
            assert!(writer.writers.is_empty());
        });
    }

    #[test]
    fn writer_allows_null_for_optional_fixed_size_field() {
        futures::executor::block_on(async {
            let mut writer = fixed_size_writer(false);
            writer
                .write(&binary_view_batch(vec![None]))
                .await
                .expect("optional null should write");

            let files = writer.close().await.expect("written data file");
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].record_count, 1);
        });
    }

    #[test]
    fn writer_cast_preserves_nested_name_matching_metadata_and_nullability() {
        let field_with_id = |name: &str, id: i32, nullable: bool| {
            Arc::new(
                Field::new(name, DataType::Int32, nullable).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    id.to_string(),
                )])),
            )
        };
        let source_fields = vec![
            field_with_id("legacy", 1, true),
            field_with_id("kept", 2, true),
        ];
        let source_payload = StructArray::new(
            source_fields.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(Int32Array::from(vec![Some(20)])),
            ],
            None,
        );
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(source_fields.into()),
            true,
        )]));
        let source = RecordBatch::try_new(source_schema, vec![Arc::new(source_payload)])
            .expect("source batch");

        let target_fields = vec![
            field_with_id("kept", 2, false),
            // The field ID matches `legacy`, but writer evolution has always matched nested
            // fields by name, so a rename remains a missing nullable field.
            field_with_id("renamed", 1, true),
        ];
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Struct(target_fields.into()), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "100".to_string())]),
            ),
        ]));

        let casted = IcebergTableWriter::cast_batch_for_table_write(&source, &target_schema)
            .expect("writer cast");
        assert_eq!(casted.schema(), target_schema);
        let payload = casted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct payload");
        assert_eq!(
            payload
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("kept values")
                .value(0),
            20
        );
        assert!(payload.column(1).is_null(0));
    }
}
