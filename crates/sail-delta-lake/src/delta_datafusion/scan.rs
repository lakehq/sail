use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfig,
    FileScanConfigBuilder, ParquetSource,
};
use datafusion::physical_expr::PhysicalExpr;
use deltalake::kernel::Add;
use deltalake::logstore::LogStoreRef;
use object_store::path::Path;

use crate::delta_datafusion::schema_rewriter::DeltaPhysicalExprAdapterFactory;
use crate::delta_datafusion::{
    create_object_store_url, delta_to_datafusion_error, partitioned_file_from_action,
    DataFusionMixins, DeltaScanConfig, DeltaTableStateExt,
};
use crate::table::DeltaTableState;

/// Parameters for building file scan configuration
pub struct FileScanParams<'a> {
    pub pruning_mask: Option<&'a [bool]>,
    pub projection: Option<&'a Vec<usize>>,
    pub limit: Option<usize>,
    pub pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
}

/// Build a FileScanConfig from pruned files and scan configuration
pub fn build_file_scan_config(
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
    files: Vec<Add>,
    scan_config: &DeltaScanConfig,
    params: FileScanParams<'_>,
    session: &dyn Session,
    file_schema: SchemaRef,
) -> Result<FileScanConfig> {
    // Get the complete schema that includes partition columns
    let complete_schema = match scan_config.schema.clone() {
        Some(schema) => schema,
        None => snapshot.input_schema().map_err(delta_to_datafusion_error)?,
    };
    let config = scan_config.clone();
    let table_partition_cols = snapshot.metadata().partition_columns();

    // Build file groups by partition values
    let mut file_groups: HashMap<
        Vec<datafusion::common::scalar::ScalarValue>,
        Vec<PartitionedFile>,
    > = HashMap::new();

    for action in files.iter() {
        let mut part = partitioned_file_from_action(action, table_partition_cols, &complete_schema);

        // Add file column if configured
        if config.file_column_name.is_some() {
            let partition_value = if config.wrap_partition_values {
                wrap_partition_value_in_dict(datafusion::common::scalar::ScalarValue::Utf8(Some(
                    action.path.clone(),
                )))
            } else {
                datafusion::common::scalar::ScalarValue::Utf8(Some(action.path.clone()))
            };
            part.partition_values.push(partition_value);
        }

        file_groups
            .entry(part.partition_values.clone())
            .or_default()
            .push(part);
    }

    // Rewrite file paths with table location prefix
    file_groups.iter_mut().for_each(|(_, files)| {
        files.iter_mut().for_each(|file| {
            file.object_meta.location = Path::from(format!(
                "{}{}{}",
                log_store.config().location.path(),
                object_store::path::DELIMITER,
                file.object_meta.location
            ));
        });
    });

    // Build table partition columns schema
    let mut table_partition_cols_schema = table_partition_cols
        .iter()
        .map(|col| {
            #[allow(clippy::expect_used)]
            let field = complete_schema
                .field_with_name(col)
                .expect("Column should exist in schema");
            let corrected = if config.wrap_partition_values {
                match field.data_type() {
                    ArrowDataType::Utf8
                    | ArrowDataType::LargeUtf8
                    | ArrowDataType::Binary
                    | ArrowDataType::LargeBinary => {
                        wrap_partition_type_in_dict(field.data_type().clone())
                    }
                    _ => field.data_type().clone(),
                }
            } else {
                field.data_type().clone()
            };
            Field::new(col.clone(), corrected, true)
        })
        .collect::<Vec<_>>();

    // Add file column to partition schema if configured
    if let Some(file_column_name) = &config.file_column_name {
        let field_name_datatype = if config.wrap_partition_values {
            wrap_partition_type_in_dict(ArrowDataType::Utf8)
        } else {
            ArrowDataType::Utf8
        };
        table_partition_cols_schema.push(Field::new(
            file_column_name.clone(),
            field_name_datatype,
            false,
        ));
    }

    // Calculate table statistics
    let stats = snapshot
        .datafusion_table_statistics(params.pruning_mask)
        .unwrap_or_else(|| datafusion::common::stats::Statistics::new_unknown(&file_schema));

    // Configure Parquet source with pushdown filter
    let parquet_options = TableParquetOptions {
        global: session.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    let mut parquet_source = ParquetSource::new(parquet_options);

    if let Some(predicate) = params.pushdown_filter {
        if config.enable_parquet_pushdown {
            parquet_source = parquet_source.with_predicate(predicate);
        }
    }

    let file_source = Arc::new(parquet_source);

    // Build the final FileScanConfig
    let object_store_url =
        create_object_store_url(&log_store.config().location).map_err(delta_to_datafusion_error)?;

    let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
        .with_file_groups(
            // If all files were filtered out, we still need to emit at least one partition
            // to pass datafusion sanity checks.
            // See https://github.com/apache/datafusion/issues/11322
            if file_groups.is_empty() {
                vec![FileGroup::from(vec![])]
            } else {
                file_groups.into_values().map(FileGroup::from).collect()
            },
        )
        .with_statistics(stats)
        .with_projection(params.projection.cloned())
        .with_limit(params.limit)
        .with_table_partition_cols(table_partition_cols_schema)
        .with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory {})))
        .build();

    Ok(file_scan_config)
}
