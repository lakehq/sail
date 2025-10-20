use std::collections::HashMap;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::io::IcebergObjectStore;
use crate::spec::types::values::Literal;
use crate::spec::DataFile;
use crate::writer::arrow_parquet::ArrowParquetWriter;
use crate::writer::base_writer::DataFileWriter;
use crate::writer::config::WriterConfig;
use crate::writer::file_writer::location_generator::{DefaultLocationGenerator, LocationGenerator};
use crate::writer::partition::group_by_partition;

pub struct IcebergTableWriter {
    pub store: IcebergObjectStore,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    // partition_dir -> writer
    writers: HashMap<String, ArrowParquetWriter>,
    // partition_dir -> partition values aligned with spec
    partition_values_map: HashMap<String, Vec<Option<Literal>>>,
    written: Vec<DataFile>,
    pub partition_spec_id: i32,
}

impl IcebergTableWriter {
    pub fn new(
        store: IcebergObjectStore,
        root: ObjectPath,
        config: WriterConfig,
        partition_spec_id: i32,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new(root),
            store,
            config,
            writers: HashMap::new(),
            partition_values_map: HashMap::new(),
            written: Vec::new(),
            partition_spec_id,
        }
    }

    fn cast_batch_to_schema(
        batch: &RecordBatch,
        target: &ArrowSchema,
    ) -> Result<RecordBatch, String> {
        log::trace!(
            "iceberg.table_writer.cast: src_fields={} target_fields={}",
            batch.schema().fields().len(),
            target.fields().len()
        );
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
        for f in target.fields() {
            let idx = batch
                .schema()
                .index_of(f.name())
                .map_err(|e| e.to_string())?;
            let col = batch.column(idx);
            log::trace!(
                "iceberg.table_writer.cast: field='{}' src_type={:?} -> target_type={:?}",
                f.name(),
                col.data_type(),
                f.data_type()
            );
            let out = if col.data_type() == f.data_type() {
                col.clone()
            } else {
                compute::cast(col, f.data_type()).map_err(|e| e.to_string())?
            };
            cols.push(out);
        }
        let out = RecordBatch::try_new(std::sync::Arc::new(target.clone()), cols)
            .map_err(|e| e.to_string())?;
        for (i, f) in out.schema().fields().iter().enumerate() {
            log::trace!(
                "iceberg.table_writer.cast: out_field[{}]='{}' type={:?} field_id_meta={:?}",
                i,
                f.name(),
                f.data_type(),
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)
            );
        }
        Ok(out)
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let groups = group_by_partition(
            batch,
            &self.config.partition_spec,
            &self.config.iceberg_schema,
        )?;
        for g in groups {
            #[allow(clippy::expect_used)]
            let writer = self
                .writers
                .entry(g.partition_dir.clone())
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
            // cache partition values for this directory (first wins)
            self.partition_values_map
                .entry(g.partition_dir.clone())
                .or_insert(g.partition_values);
            log::trace!(
                "iceberg.table_writer.write: partition='{}' in_rows={} in_cols={}",
                g.partition_dir,
                g.record_batch.num_rows(),
                g.record_batch.num_columns()
            );
            let aligned =
                Self::cast_batch_to_schema(&g.record_batch, self.config.table_schema.as_ref())?;
            log::trace!(
                "iceberg.table_writer.write: aligned_rows={} aligned_cols={}",
                aligned.num_rows(),
                aligned.num_columns()
            );
            for (i, f) in aligned.schema().fields().iter().enumerate() {
                log::trace!(
                    "iceberg.table_writer.aligned_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                    i,
                    f.name(),
                    f.data_type(),
                    f.metadata().get(PARQUET_FIELD_ID_META_KEY)
                );
            }
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
            self.store.put(&full, bytes).await?;
            log::trace!(
                "iceberg.table_writer.flush_partition.written: rel={} full={}",
                &rel,
                &full
            );
            // Use absolute filesystem path for cross-compat (PyIceberg expects an absolute file path)
            let abs_path = format!("/{}", full.to_string());
            let df = DataFileWriter::new(self.partition_spec_id, abs_path, partition_values)
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
