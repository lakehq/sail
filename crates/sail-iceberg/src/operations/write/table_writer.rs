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
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion_common::{DataFusionError, Result};
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_parquet::ParquetFileWriter;
use url::Url;

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
        writer: Box<ParquetFileWriter>,
        relative_path: String,
        physical_schema: SchemaRef,
        variant_shredding_plan: Option<VariantShreddingPlan>,
    },
}

pub struct IcebergTableWriter {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    pub data_url: Url,
    memory_pool: Arc<dyn MemoryPool>,
    object_store_buffer_size: usize,
    // partition_dir -> writer
    writers: HashMap<String, PartitionWriterState>,
    // partition_dir -> partition values aligned with spec
    partition_values_map: HashMap<String, Vec<Option<Literal>>>,
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
        memory_pool: Arc<dyn MemoryPool>,
        object_store_buffer_size: usize,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new(root),
            store,
            config,
            data_url,
            memory_pool,
            object_store_buffer_size,
            writers: HashMap::new(),
            partition_values_map: HashMap::new(),
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

        if spec.fields.is_empty() {
            // Unpartitioned: write as-is once
            let partition_dir = String::new();
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_default();
            let padded = Self::align_batch_with_table_schema(
                batch,
                &self.config.table_schema,
                self.config.iceberg_schema.as_ref(),
            )
            .map_err(|e| e.to_string())?;
            let normalized =
                unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
            let aligned = cast_record_batch_relaxed_tz(&normalized, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            self.write_aligned_batch(partition_dir, aligned).await?;
            return Ok(());
        }

        let parts = split_record_batch_by_partition(batch, spec, iceberg_schema)?;
        for p in parts.into_iter() {
            let partition_dir = p.partition_dir;
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_insert(p.partition_values);
            let padded = Self::align_batch_with_table_schema(
                &p.record_batch,
                &self.config.table_schema,
                self.config.iceberg_schema.as_ref(),
            )
            .map_err(|e| e.to_string())?;
            let normalized =
                unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
            let aligned = cast_record_batch_relaxed_tz(&normalized, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            self.write_aligned_batch(partition_dir, aligned).await?;
        }

        Ok(())
    }

    async fn write_aligned_batch(
        &mut self,
        partition_dir: String,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let state = match self.writers.remove(&partition_dir) {
            Some(state) => state,
            None => self.new_partition_writer_state(&partition_dir)?,
        };
        let state = self
            .write_partition_state(&partition_dir, state, batch)
            .await?;
        self.writers.insert(partition_dir, state);
        Ok(())
    }

    fn new_partition_writer_state(
        &self,
        partition_dir: &str,
    ) -> Result<PartitionWriterState, String> {
        if self.config.variant_shredding.enabled {
            Ok(PartitionWriterState::Pending {
                batches: Vec::new(),
                num_rows: 0,
            })
        } else {
            self.open_partition_writer_state(partition_dir, self.config.table_schema.clone(), None)
        }
    }

    async fn write_partition_state(
        &mut self,
        partition_dir: &str,
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
                    self.open_and_write_pending_batches(partition_dir, batches)
                        .await
                } else {
                    Ok(PartitionWriterState::Pending { batches, num_rows })
                }
            }
            PartitionWriterState::Open {
                writer,
                relative_path,
                physical_schema,
                variant_shredding_plan,
            } => {
                let batch = if let Some(plan) = variant_shredding_plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)?
                } else {
                    batch
                };
                self.write_open_batch(
                    partition_dir,
                    writer,
                    relative_path,
                    physical_schema,
                    variant_shredding_plan,
                    batch,
                )
                .await
            }
        }
    }

    async fn open_and_write_pending_batches(
        &mut self,
        partition_dir: &str,
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
        let mut state = self.open_partition_writer_state(partition_dir, schema, plan.clone())?;
        for batch in physical_batches {
            let PartitionWriterState::Open {
                writer,
                relative_path,
                physical_schema,
                variant_shredding_plan,
            } = state
            else {
                return Err("failed to open pending Iceberg partition writer".to_string());
            };
            state = self
                .write_open_batch(
                    partition_dir,
                    writer,
                    relative_path,
                    physical_schema,
                    variant_shredding_plan,
                    batch,
                )
                .await?;
        }
        Ok(state)
    }

    async fn write_open_batch(
        &mut self,
        partition_dir: &str,
        mut writer: Box<ParquetFileWriter>,
        mut relative_path: String,
        physical_schema: SchemaRef,
        variant_shredding_plan: Option<VariantShreddingPlan>,
        batch: RecordBatch,
    ) -> Result<PartitionWriterState, String> {
        let write_batch_size = self.config.write_batch_size.max(1);
        for offset in (0..batch.num_rows()).step_by(write_batch_size) {
            let length = write_batch_size.min(batch.num_rows() - offset);
            writer
                .write(&batch.slice(offset, length))
                .await
                .map_err(|error| error.to_string())?;
            if writer.estimated_file_size() >= self.config.target_file_size
                && writer.row_count() > 0
            {
                self.finish_open_writer(partition_dir, *writer, relative_path)
                    .await?;
                let next = self.open_partition_writer_state(
                    partition_dir,
                    Arc::clone(&physical_schema),
                    variant_shredding_plan.clone(),
                )?;
                let PartitionWriterState::Open {
                    writer: next_writer,
                    relative_path: next_relative_path,
                    ..
                } = next
                else {
                    return Err("failed to roll Iceberg partition writer".to_string());
                };
                writer = next_writer;
                relative_path = next_relative_path;
            }
        }
        Ok(PartitionWriterState::Open {
            writer,
            relative_path,
            physical_schema,
            variant_shredding_plan,
        })
    }

    fn open_partition_writer_state(
        &self,
        partition_dir: &str,
        schema: SchemaRef,
        variant_shredding_plan: Option<VariantShreddingPlan>,
    ) -> Result<PartitionWriterState, String> {
        for (i, f) in schema.fields().iter().enumerate() {
            log::trace!(
                "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                i,
                f.name(),
                f.data_type(),
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)
            );
        }
        let (relative_path, full_path) = self.generator.with_partition_dir(Some(partition_dir));
        let writer = ParquetFileWriter::try_new(
            Arc::clone(&self.store),
            full_path,
            Arc::clone(&schema),
            self.config.writer_properties.clone(),
            self.object_store_buffer_size,
            &self.memory_pool,
        )
        .map_err(|error| error.to_string())?;
        Ok(PartitionWriterState::Open {
            writer: Box::new(writer),
            relative_path,
            physical_schema: schema,
            variant_shredding_plan,
        })
    }

    async fn finish_partition_state(
        &mut self,
        partition_dir: &str,
        state: PartitionWriterState,
    ) -> Result<(), String> {
        let state = match state {
            PartitionWriterState::Pending { batches, .. } => {
                self.open_and_write_pending_batches(partition_dir, batches)
                    .await?
            }
            open @ PartitionWriterState::Open { .. } => open,
        };
        let PartitionWriterState::Open {
            writer,
            relative_path,
            ..
        } = state
        else {
            return Err("failed to finish Iceberg partition writer".to_string());
        };
        self.finish_open_writer(partition_dir, *writer, relative_path)
            .await
    }

    async fn finish_open_writer(
        &mut self,
        partition_dir: &str,
        writer: ParquetFileWriter,
        relative_path: String,
    ) -> Result<(), String> {
        if writer.row_count() == 0 {
            writer.abort().await;
            return Ok(());
        }
        let written = writer.finish().await.map_err(|error| error.to_string())?;
        log::trace!(
            "iceberg.table_writer.flush_partition.written: rel={} full={}",
            relative_path,
            written.path
        );
        // Prevent a leading partition segment containing ':' from being parsed as a URI scheme.
        let file_path = match self.data_url.join(&format!("./{relative_path}")) {
            Ok(url) => url.to_string(),
            Err(_) => format!("{}{}", self.data_url.as_str(), relative_path),
        };
        let partition_values = self
            .partition_values_map
            .get(partition_dir)
            .cloned()
            .unwrap_or_default();
        let data_file = DataFileWriter::new(self.partition_spec_id, file_path, partition_values)
            .finish(written)?
            .data_file;
        self.written.push(data_file);
        Ok(())
    }

    pub async fn flush_partition(&mut self, partition_dir: &str) -> Result<(), String> {
        if let Some(state) = self.writers.remove(partition_dir) {
            self.finish_partition_state(partition_dir, state).await?;
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        let keys: Vec<String> = self.writers.keys().cloned().collect();
        for k in keys {
            self.flush_partition(&k).await?;
            self.partition_values_map.remove(&k);
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
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion_common::{DataFusionError, Result};
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::operations::write::config::VariantShreddingConfig;
    use crate::spec::partition::UnboundPartitionSpec;
    use crate::spec::types::{NestedField, PrimitiveType, Type};

    #[tokio::test]
    async fn streams_files_and_rolls_over_at_target_size() -> Result<()> {
        let iceberg_schema = IcebergSchema::builder()
            .with_fields([Arc::new(NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .map_err(DataFusionError::Plan)?;
        let table_schema = Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?);
        let batch = RecordBatch::try_new(
            Arc::clone(&table_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;
        let store = Arc::new(InMemory::new());
        let context = TaskContext::default();
        let config = WriterConfig {
            table_schema,
            partition_columns: vec![],
            writer_properties: WriterProperties::default(),
            target_file_size: 1,
            write_batch_size: 1,
            num_indexed_cols: 32,
            stats_columns: None,
            iceberg_schema: Arc::new(iceberg_schema),
            partition_spec: UnboundPartitionSpec { fields: vec![] },
            variant_shredding: VariantShreddingConfig::default(),
        };
        let mut writer = IcebergTableWriter::new(
            store.clone(),
            ObjectPath::from("table/data"),
            config,
            0,
            Url::parse("memory:///table/data/")
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            Arc::clone(context.memory_pool()),
            64,
        );

        writer.write(&batch).await.map_err(DataFusionError::Plan)?;
        let files = writer.close().await.map_err(DataFusionError::Plan)?;

        assert_eq!(files.len(), 3);
        assert_eq!(files.iter().map(|file| file.record_count).sum::<u64>(), 3);
        assert!(
            files
                .iter()
                .all(|file| file.file_path.starts_with("memory:///table/data/part-"))
        );
        let objects = store
            .list(Some(&ObjectPath::from("table/data")))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        assert_eq!(objects.len(), 3);
        Ok(())
    }
}
