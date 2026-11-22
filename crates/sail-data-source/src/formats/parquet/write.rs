// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};

use crate::formats::parquet::writer::exec::ParquetWriteExec;
use crate::listing::commit::FileWriteCommitExec;
use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::r#gen::ParquetWriteOptions;
use crate::utils::split_parquet_compression_string;

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    pub(super) options: ParquetWriteOptions,
}

#[async_trait]
impl WriteFormat for ParquetWriteFormat {
    async fn sink(
        &self,
        _ctx: &dyn Session,
        input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let encoding_parallelism = self.options.encoding_parallelism;
        let max_open_files_per_task = self.options.max_open_files_per_task;
        // A limit on the rows of a file that is not positive means that there is no limit.
        let max_records_per_file = u64::try_from(self.options.max_records_per_file).unwrap_or(0);
        let max_file_size = self.options.max_file_size;
        let write_success_file = self.options.write_success_file;
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let file_suffix = file_suffix(&options)?;
        let ListingSinkInput {
            input,
            sink,
            sort_order,
            write_id,
            overwrite,
        } = input;
        let table_path = sink
            .table_paths
            .first()
            .ok_or_else(|| internal_datafusion_err!("missing table path for Parquet write"))?
            .prefix()
            .clone();
        let partition_columns = sink
            .table_partition_cols
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        // The ordering of each file is recorded in the file only when
        // the ordering was requested for the files of the table.
        let (ordering, write_sorting_columns) = match sort_order {
            Some(sort_order) => (Some(LexOrdering::from(sort_order)), true),
            None => (sorted_input_ordering(&input), false),
        };
        let writer = Arc::new(
            ParquetWriteExec::try_new(
                input,
                sink.object_store_url.clone(),
                table_path.clone(),
                write_id,
                partition_columns,
                options,
                file_suffix,
                ordering,
                write_sorting_columns,
                encoding_parallelism,
                max_open_files_per_task,
            )?
            .with_file_limits(max_records_per_file, max_file_size),
        );
        Ok(Arc::new(
            FileWriteCommitExec::new(writer, sink.object_store_url, table_path, write_id)
                .with_overwrite(overwrite)
                .with_success_file(write_success_file),
        ))
    }

    fn replaces_existing_files(&self) -> bool {
        true
    }
}

/// Returns the ordering of the input if the input is sorted by the query.
///
/// The writer requires this ordering, so that the sort is kept in the plan
/// and the rows are written in the sorted order. An ordering that the input
/// has for another reason, such as an ordering known for the files of a scan,
/// is not required, since requiring it could add a sort to the plan.
fn sorted_input_ordering(input: &Arc<dyn ExecutionPlan>) -> Option<LexOrdering> {
    let mut plan = input;
    loop {
        if plan.is::<SortExec>() || plan.is::<SortPreservingMergeExec>() {
            return input.output_ordering().cloned();
        }
        match plan.children().as_slice() {
            [child] if plan.maintains_input_order() == [true] => plan = child,
            _ => return None,
        }
    }
}

/// Returns the suffix of the file names, which has the compression codec
/// of the files when the files are compressed.
fn file_suffix(options: &TableParquetOptions) -> Result<String> {
    let Some(compression) = &options.global.compression else {
        return Ok(".parquet".to_string());
    };
    let (codec, _level) = split_parquet_compression_string(&compression.to_lowercase())?;
    let codec = match codec.as_str() {
        "uncompressed" => return Ok(".parquet".to_string()),
        "gzip" => "gz",
        "lz4_raw" => "lz4raw",
        codec => codec,
    };
    Ok(format!(".{codec}.parquet"))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::expressions::{Column, col};
    use datafusion::physical_expr_common::sort_expr::PhysicalSortExpr;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

    use super::*;

    #[test]
    fn test_file_suffix() -> Result<()> {
        let mut options = TableParquetOptions::default();
        for (compression, suffix) in [
            (None, ".parquet"),
            (Some("uncompressed"), ".parquet"),
            (Some("UNCOMPRESSED"), ".parquet"),
            (Some("zstd(3)"), ".zstd.parquet"),
            (Some("ZSTD(19)"), ".zstd.parquet"),
            (Some("gzip(6)"), ".gz.parquet"),
            (Some("snappy"), ".snappy.parquet"),
            (Some("lz4_raw"), ".lz4raw.parquet"),
            (Some("lz4"), ".lz4.parquet"),
            (Some("brotli(3)"), ".brotli.parquet"),
            (Some("lzo"), ".lzo.parquet"),
        ] {
            options.global.compression = compression.map(|x| x.to_string());
            assert_eq!(file_suffix(&options)?, suffix, "{compression:?}");
        }
        Ok(())
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn ordering(schema: &Schema, name: &str) -> Result<LexOrdering> {
        LexOrdering::new([PhysicalSortExpr::new(
            col(name, schema)?,
            SortOptions::default(),
        )])
        .ok_or_else(|| internal_datafusion_err!("empty ordering"))
    }

    /// A projection that keeps the order of the rows and swaps the columns.
    fn swap_columns(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    alias: "y".to_string(),
                },
                ProjectionExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    alias: "x".to_string(),
                },
            ],
            input,
        )?))
    }

    #[test]
    fn test_sorted_input_ordering_for_sort_in_query() -> Result<()> {
        let schema = schema();
        let source = MemorySourceConfig::try_new_exec(&[vec![], vec![]], schema.clone(), None)?;
        for preserve_partitioning in [false, true] {
            let sort: Arc<dyn ExecutionPlan> = Arc::new(
                SortExec::new(ordering(&schema, "a")?, source.clone())
                    .with_preserve_partitioning(preserve_partitioning),
            );
            assert_eq!(sorted_input_ordering(&sort), Some(ordering(&schema, "a")?));
            // The ordering refers to the columns of the input of the writer.
            let projection = swap_columns(sort)?;
            assert_eq!(
                sorted_input_ordering(&projection),
                Some(ordering(&projection.schema(), "x")?)
            );
        }

        let sort = Arc::new(
            SortExec::new(ordering(&schema, "a")?, source.clone()).with_preserve_partitioning(true),
        );
        let merge: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(
            ordering(&schema, "a")?,
            sort.clone(),
        ));
        assert_eq!(sorted_input_ordering(&merge), Some(ordering(&schema, "a")?));

        // The order of the rows is lost above the sort.
        let coalesce: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(sort));
        assert_eq!(sorted_input_ordering(&coalesce), None);
        Ok(())
    }

    #[test]
    fn test_sorted_input_ordering_for_ordered_source() -> Result<()> {
        let schema = schema();
        let source = MemorySourceConfig::try_new(&[vec![], vec![]], schema.clone(), None)?
            .try_with_sort_information(vec![ordering(&schema, "a")?])?;
        let source: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);
        assert_eq!(source.output_ordering(), Some(&ordering(&schema, "a")?));
        assert_eq!(sorted_input_ordering(&source), None);

        let projection = swap_columns(source)?;
        assert!(projection.output_ordering().is_some());
        assert_eq!(sorted_input_ordering(&projection), None);
        Ok(())
    }
}
