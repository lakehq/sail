use std::collections::HashMap;

use datafusion::arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;

use crate::io::IcebergObjectStore;
use crate::spec::types::values::Literal;
use crate::spec::DataFile;
use crate::writer::arrow_parquet::ArrowParquetWriter;
use crate::writer::base_writer::DataFileWriter;
use crate::writer::config::WriterConfig;
use crate::writer::file_writer::location_generator::{DefaultLocationGenerator, LocationGenerator};
use crate::writer::partition::compute_partition_values;

pub struct IcebergTableWriter {
    pub store: IcebergObjectStore,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    // partition_dir -> writer
    writers: HashMap<String, ArrowParquetWriter>,
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
            written: Vec::new(),
            partition_spec_id,
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        // TODO: real partitioning based on spec
        let (_partition_values, partition_dir) = compute_partition_values(
            batch,
            &crate::spec::partition::UnboundPartitionSpec { fields: vec![] },
        )?;
        #[allow(clippy::expect_used)]
        let writer = self
            .writers
            .entry(partition_dir.clone())
            .or_insert_with(|| {
                ArrowParquetWriter::try_new(
                    self.config.table_schema.as_ref(),
                    self.config.writer_properties.clone(),
                )
                .expect("parquet writer")
            });
        writer.write_batch(batch).await?;

        // size-triggered flush could be added here
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
            self.store.put(&full, bytes).await?;
            let df = DataFileWriter::new(self.partition_spec_id, rel, partition_values)
                .finish(meta)?
                .data_file;
            self.written.push(df);
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        let keys: Vec<String> = self.writers.keys().cloned().collect();
        for k in keys {
            // partition values are unknown in this placeholder; use empty
            self.flush_partition(&k, Vec::new()).await?;
        }
        Ok(self.written)
    }
}
