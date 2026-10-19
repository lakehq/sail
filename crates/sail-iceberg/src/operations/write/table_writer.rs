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

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::schema_evolution::{
    StructFieldMatching, cast_array_with_schema_evolution_relaxed_tz,
};
use url::Url;

use crate::operations::write::arrow_parquet::{ArrowParquetWriter, ParquetObjectSink};
use crate::operations::write::base_writer::DataFileWriter;
use crate::operations::write::config::WriterConfig;
use crate::operations::write::file_writer::location_generator::DefaultLocationGenerator;
use crate::operations::write::partition::split_record_batch_by_partition;
use crate::operations::write::variant_shredding::{
    VariantShreddingPlan, apply_variant_shredding_plan, build_variant_shredding_plan,
    unshred_shredded_variants_for_write,
};
use crate::spec::DataFile;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::values::Literal;

enum PartitionWriterState {
    Pending {
        batches: Vec<RecordBatch>,
        num_rows: usize,
    },
    Open {
        writer: Box<ArrowParquetWriter<ParquetObjectSink>>,
        file_path: String,
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
        let spec = &self.config.partition_spec;
        let iceberg_schema = &self.config.iceberg_schema;
        let padded =
            Self::align_batch_with_table_schema(batch, &self.config.table_schema, iceberg_schema)
                .map_err(|e| e.to_string())?;
        let normalized = unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
        let columns = normalized
            .columns()
            .iter()
            .zip(self.config.table_schema.fields())
            .map(|(column, field)| {
                cast_array_with_schema_evolution_relaxed_tz(
                    column,
                    field,
                    &Default::default(),
                    StructFieldMatching::Name,
                )
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        let aligned = RecordBatch::try_new(self.config.table_schema.clone(), columns)
            .map_err(|e| e.to_string())?;
        let parts = split_record_batch_by_partition(&aligned, spec, iceberg_schema)?;
        for p in parts {
            self.write_aligned_batch(p.partition_values, p.partition_dir, p.record_batch)
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
        // Check encoded size at a bounded row interval even for large input batches.
        for offset in (0..batch.num_rows()).step_by(1000) {
            let chunk = batch.slice(offset, (batch.num_rows() - offset).min(1000));
            let state = match self.writers.remove(&partition_values) {
                Some(writer) => writer.state,
                None => self.new_partition_writer_state(&partition_dir)?,
            };
            let state = self
                .write_partition_state(state, chunk, &partition_dir)
                .await?;
            if matches!(&state, PartitionWriterState::Open { writer, .. }
                if writer.estimated_size() >= self.config.target_file_size_bytes)
            {
                self.flush_partition(state, &partition_dir, partition_values.clone())
                    .await?;
            } else {
                self.writers.insert(
                    partition_values.clone(),
                    PartitionWriter {
                        partition_dir: partition_dir.clone(),
                        state,
                    },
                );
            }
        }
        Ok(())
    }

    fn new_partition_writer_state(
        &mut self,
        partition_dir: &str,
    ) -> Result<PartitionWriterState, String> {
        if self.config.variant_shredding.enabled {
            Ok(PartitionWriterState::Pending {
                batches: Vec::new(),
                num_rows: 0,
            })
        } else {
            let (writer, file_path) =
                self.new_arrow_writer(self.config.table_schema.clone(), partition_dir)?;
            Ok(PartitionWriterState::Open {
                writer: Box::new(writer),
                file_path,
                variant_shredding_plan: None,
            })
        }
    }

    async fn write_partition_state(
        &mut self,
        state: PartitionWriterState,
        batch: RecordBatch,
        partition_dir: &str,
    ) -> Result<PartitionWriterState, String> {
        match state {
            PartitionWriterState::Pending {
                mut batches,
                mut num_rows,
            } => {
                num_rows += batch.num_rows();
                batches.push(batch);
                if num_rows >= self.config.variant_shredding.inference_buffer_size.max(1) {
                    self.open_and_write_pending_batches(batches, partition_dir)
                        .await
                } else {
                    Ok(PartitionWriterState::Pending { batches, num_rows })
                }
            }
            PartitionWriterState::Open {
                mut writer,
                file_path,
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
                    file_path,
                    variant_shredding_plan,
                })
            }
        }
    }

    async fn open_and_write_pending_batches(
        &mut self,
        batches: Vec<RecordBatch>,
        partition_dir: &str,
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
        let (mut writer, file_path) = self.new_arrow_writer(schema, partition_dir)?;
        for batch in physical_batches {
            writer.write_batch(&batch).await?;
        }
        Ok(PartitionWriterState::Open {
            writer: Box::new(writer),
            file_path,
            variant_shredding_plan: plan,
        })
    }

    fn new_arrow_writer(
        &mut self,
        schema: SchemaRef,
        partition_dir: &str,
    ) -> Result<(ArrowParquetWriter<ParquetObjectSink>, String), String> {
        for (i, f) in schema.fields().iter().enumerate() {
            log::trace!(
                "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                i,
                f.name(),
                f.data_type(),
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)
            );
        }
        let (relative, path) = self.generator.next_data_path(Some(partition_dir))?;
        let file_path = self
            .data_url
            .join(&format!("./{relative}"))
            .map_err(|error| error.to_string())?
            .to_string();
        let output = ParquetObjectSink::new(BufWriter::new(Arc::clone(&self.store), path));
        let writer = ArrowParquetWriter::try_new(
            schema.as_ref(),
            self.config.writer_properties.clone(),
            output,
        )?;
        Ok((writer, file_path))
    }

    async fn finish_partition_state(
        &mut self,
        state: PartitionWriterState,
        partition_dir: &str,
    ) -> Result<(ArrowParquetWriter<ParquetObjectSink>, String), String> {
        match state {
            PartitionWriterState::Pending { batches, .. } => {
                let PartitionWriterState::Open {
                    writer, file_path, ..
                } = self
                    .open_and_write_pending_batches(batches, partition_dir)
                    .await?
                else {
                    return Err("failed to open pending Iceberg partition writer".to_string());
                };
                Ok((*writer, file_path))
            }
            PartitionWriterState::Open {
                writer, file_path, ..
            } => Ok((*writer, file_path)),
        }
    }

    async fn flush_partition(
        &mut self,
        state: PartitionWriterState,
        partition_dir: &str,
        partition_values: Vec<Option<Literal>>,
    ) -> Result<(), String> {
        let (writer, file_path) = self.finish_partition_state(state, partition_dir).await?;
        let (_, meta) = writer.close().await?;
        let mut df = DataFileWriter::new(self.partition_spec_id, file_path, partition_values)
            .finish_with_schema(
                meta,
                self.config.iceberg_schema.as_ref(),
                &self.config.metrics,
            )?
            .data_file;
        df.sort_order_id = self.config.sort_order_id;
        self.written.push(df);
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        // FIXME: Retain ownership of uploaded files across partial close failures and task cancellation.
        // Cleanup must wait for the job outcome so retries can safely reuse successful task output.
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
                    schema_fields.push(Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(array.data_type().clone()),
                    ));
                    columns.push(array);
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

        crate::schema_defaults::missing_write_value(iceberg_field.as_ref(), num_rows)
    }
}
