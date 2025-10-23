use std::collections::HashMap;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::io::IcebergObjectStore;
use crate::spec::types::values::Literal;
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::DataFile;
use crate::writer::arrow_parquet::ArrowParquetWriter;
use crate::writer::base_writer::DataFileWriter;
use crate::writer::config::WriterConfig;
use crate::writer::file_writer::location_generator::{DefaultLocationGenerator, LocationGenerator};
use crate::writer::partition::{
    apply_transform, build_partition_dir, field_name_from_id, scalar_to_literal,
};

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
        data_dir: String,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new_with_data_dir(root, data_dir),
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
        // Assumption: input batch is already partitioned (single partition per batch)
        // Extract partition values from the first row only

        if batch.num_rows() == 0 {
            return Ok(());
        }

        let spec = &self.config.partition_spec;
        let iceberg_schema = &self.config.iceberg_schema;

        // Compute partition values and directory from first row
        let (partition_values, partition_dir) = if spec.fields.is_empty() {
            (Vec::new(), String::new())
        } else {
            let mut values = Vec::with_capacity(spec.fields.len());
            for f in &spec.fields {
                let col_name = field_name_from_id(iceberg_schema, f.source_id)
                    .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
                let col_index = batch
                    .schema()
                    .index_of(&col_name)
                    .map_err(|e| e.to_string())?;
                let lit = scalar_to_literal(batch.column(col_index), 0);
                let field_type = iceberg_schema
                    .field_by_id(f.source_id)
                    .map(|nf| nf.field_type.as_ref())
                    .unwrap_or(&Type::Primitive(PrimitiveType::String));
                values.push(apply_transform(f.transform, field_type, lit));
            }
            let dir = build_partition_dir(spec, iceberg_schema, &values);
            (values, dir)
        };

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

        // Cache partition values for this directory (first wins)
        self.partition_values_map
            .entry(partition_dir.clone())
            .or_insert(partition_values);

        log::trace!(
            "iceberg.table_writer.write: partition='{}' in_rows={} in_cols={}",
            partition_dir,
            batch.num_rows(),
            batch.num_columns()
        );

        let aligned = Self::cast_batch_to_schema(batch, self.config.table_schema.as_ref())?;

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
            let abs_path = format!("/{}", full);
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
