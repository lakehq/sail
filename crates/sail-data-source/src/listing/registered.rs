// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion_common::{Result, ScalarValue, ToDFSchema, internal_datafusion_err};
use datafusion_datasource::TableSchema;
use datafusion_expr::Expr;

use crate::listing::source::reconcile_schema_names_case_insensitive;
use crate::listing::table::{ListingTableSource, ListingTableSourceConfig, RegisteredReadOptions};
use crate::listing::utils::sample_listing_files;

/// Evaluate partition filters on the catalog values before opening any object store.
pub(super) fn prune_paths(
    config: &mut ListingTableSourceConfig,
    session: &dyn Session,
    filters: &[Expr],
) -> Result<()> {
    let Some(predicate) = datafusion_expr::utils::conjunction(filters.iter().cloned()) else {
        return Ok(());
    };
    let fields = config.schema.table_partition_cols();
    let columns = (0..fields.len())
        .map(|i| {
            let values = config
                .table_paths
                .iter()
                .map(|(_, values)| {
                    values
                        .as_ref()
                        .and_then(|v| v.get(i))
                        .cloned()
                        .ok_or_else(|| {
                            internal_datafusion_err!("missing registered partition value")
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            ScalarValue::iter_to_array(values)
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), columns)?;
    let predicate = session.create_physical_expr(predicate, &batch.schema().to_dfschema()?)?;
    let mask = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;
    let mut keep = mask.as_boolean().iter();
    config
        .table_paths
        .retain(|_| keep.next().flatten() == Some(true));
    Ok(())
}

pub(super) async fn prepare_registered_scan(
    source: &ListingTableSource,
    session: &dyn Session,
    filters: &[Expr],
    options: RegisteredReadOptions,
) -> Result<ListingTableSource> {
    let mut config = source.config().clone();
    prune_paths(&mut config, session, filters)?;
    // Parquet uses the catalog schema and resolves physical field names for each file.
    if config.table_paths.is_empty() || options.format == "parquet" {
        return ListingTableSource::try_new(config);
    }

    let urls = config
        .table_paths
        .iter()
        .map(|(url, _)| vec![url.clone()])
        .collect::<Vec<_>>();
    let (samples, _) = sample_listing_files(
        session,
        &urls,
        config.path_glob_filter.as_ref(),
        None,
        false,
        true,
        options.sampling,
    )
    .await?;
    config.compression = config
        .read_format
        .infer_compression(session, &samples)
        .await?;
    if !options.case_sensitive
        && let Ok(physical) = config
            .read_format
            .infer_schema(session, &samples, config.compression)
            .await
    {
        let file_schema = reconcile_schema_names_case_insensitive(
            config.schema.file_schema().as_ref().clone(),
            &physical,
            &[],
        )?;
        config.schema = TableSchema::builder(Arc::new(file_schema))
            .with_table_partition_cols(config.schema.table_partition_cols().clone())
            .build();
    }
    ListingTableSource::try_new(config)
}
