use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use url::Url;

use crate::spec::types::values::Literal;
use crate::spec::DataFile;
use crate::writer::arrow_parquet::ArrowParquetWriter;
use crate::writer::base_writer::DataFileWriter;
use crate::writer::config::WriterConfig;
use crate::writer::file_writer::location_generator::{DefaultLocationGenerator, LocationGenerator};
use crate::writer::partition::split_record_batch_by_partition;

pub struct IcebergTableWriter {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    pub table_url: Url,
    // partition_dir -> writer
    writers: HashMap<String, ArrowParquetWriter>,
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
        data_dir: String,
        table_url: Url,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new_with_data_dir(root, data_dir),
            store,
            config,
            table_url,
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
            #[allow(clippy::expect_used)]
            let writer = self
                .writers
                .entry(partition_dir.clone())
                .or_insert_with(|| {
                    for (i, f) in self.config.table_schema.fields().iter().enumerate() {
                        log::trace!(
                            "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                            i,
                            f.name(),
                            f.data_type(),
                            f.metadata().get(PARQUET_FIELD_ID_META_KEY)
                        );
                    }
                    ArrowParquetWriter::try_new(
                        self.config.table_schema.as_ref(),
                        self.config.writer_properties.clone(),
                    )
                    .expect("parquet writer")
                });
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_default();
            let aligned = cast_record_batch_relaxed_tz(batch, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            writer.write_batch(&aligned).await?;
            return Ok(());
        }

        let parts = split_record_batch_by_partition(batch, spec, iceberg_schema)?;
        for p in parts.into_iter() {
            let partition_dir = p.partition_dir;
            #[allow(clippy::expect_used)]
            let writer = self
                .writers
                .entry(partition_dir.clone())
                .or_insert_with(|| {
                    for (i, f) in self.config.table_schema.fields().iter().enumerate() {
                        log::trace!(
                            "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                            i,
                            f.name(),
                            f.data_type(),
                            f.metadata().get(PARQUET_FIELD_ID_META_KEY)
                        );
                    }
                    ArrowParquetWriter::try_new(
                        self.config.table_schema.as_ref(),
                        self.config.writer_properties.clone(),
                    )
                    .expect("parquet writer")
                });
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_insert(p.partition_values);
            let aligned = cast_record_batch_relaxed_tz(&p.record_batch, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            writer.write_batch(&aligned).await?;
        }

        Ok(())
    }

    pub async fn flush_partition(
        &mut self,
        partition_dir: &str,
        partition_values: Vec<Option<Literal>>,
    ) -> Result<(), String> {
        if let Some(writer) = self.writers.remove(partition_dir) {
            let (bytes, meta) = writer.close().await?;
            let (rel, full) = self.generator.with_partition_dir(Some(partition_dir));
            log::trace!("iceberg.table_writer.flush_partition.writing: {}", &full);
            self.store
                .put(&full, object_store::PutPayload::from(bytes))
                .await
                .map_err(|e| e.to_string())?;
            log::trace!(
                "iceberg.table_writer.flush_partition.written: rel={} full={}",
                &rel,
                &full
            );
            let file_path = match self.table_url.join(&rel) {
                Ok(u) => u.to_string(),
                Err(_) => {
                    format!("{}{}", self.table_url.as_str(), rel)
                }
            };
            let df = DataFileWriter::new(self.partition_spec_id, file_path, partition_values)
                .finish(meta)?
                .data_file;
            self.written.push(df);
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        let keys: Vec<String> = self.writers.keys().cloned().collect();
        for k in keys {
            let vals = self
                .partition_values_map
                .remove(&k)
                .unwrap_or_default()
                .into_iter()
                .collect();
            self.flush_partition(&k, vals).await?;
        }
        Ok(self.written)
    }
}
