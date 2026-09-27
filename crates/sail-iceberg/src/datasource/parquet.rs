use datafusion::arrow::datatypes::Schema;
use datafusion::config::{ParquetOptions, TableParquetOptions};

pub(crate) fn parquet_options(
    schema: &Schema,
    nan_free: bool,
    mut options: ParquetOptions,
) -> TableParquetOptions {
    if schema
        .flattened_fields()
        .iter()
        .any(|field| field.data_type().is_floating())
    {
        // Bloom filters distinguish signed zeros, while SQL equality does not.
        options.bloom_filter_on_read = false;
        if !nan_free {
            // Parquet bounds omit NaNs and do not prove their absence. Comparisons
            // must still reach row evaluation, including negative/payload NaNs.
            options.pruning = false;
            options.enable_page_index = false;
        }
    }
    TableParquetOptions {
        global: options,
        ..Default::default()
    }
}
