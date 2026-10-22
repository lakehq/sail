use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::TableSource;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::datasource::SourceInfo;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_data_source::options::ResolveOptions;

mod detail;
mod history;

use crate::DeltaLakeSource;
use crate::delta_log::LogStoreRef;
use crate::options::r#gen::DeltaReadOptions;
use crate::snapshot::DeltaSnapshot;
use crate::spec::inspection::DeltaInspectionType;
use crate::spec::{DeltaError, DeltaResult};
use crate::table::load_delta_read_state;

pub(crate) struct InspectionState {
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    table_name: Option<String>,
}

pub(crate) async fn load_state(
    ctx: &dyn Session,
    info: SourceInfo,
    kind: DeltaInspectionType,
) -> Result<InspectionState> {
    let table_name = match &info.lakehouse_table {
        Some(table) => Some(
            ctx.extension::<CatalogManager>()?
                .resolve_fully_qualified_object_name(&table.catalog_table)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .join("."),
        ),
        None => None,
    };
    let table_url = DeltaLakeSource::parse_table_url(ctx, info.paths).await?;
    let options = DeltaReadOptions::resolve(ctx, info.options)?;
    if options.version_as_of.is_some() || options.timestamp_as_of.is_some() {
        return plan_err!("time travel is not supported for DESCRIBE DETAIL or DESCRIBE HISTORY");
    }
    // DETAIL needs active files and domain metadata, which optional CRC fields cannot guarantee.
    let metadata_only = matches!(kind, DeltaInspectionType::History);
    // Reuse the read path so catalog-ratified, unpublished commits participate in the snapshot.
    let (snapshot, log_store, _) = load_delta_read_state(
        ctx,
        table_url,
        None,
        options,
        metadata_only,
        info.lakehouse_table,
    )
    .await?;
    Ok(InspectionState {
        snapshot,
        log_store,
        table_name,
    })
}

pub(crate) async fn record_batch(
    state: &InspectionState,
    kind: DeltaInspectionType,
    history_limit: Option<usize>,
) -> Result<RecordBatch> {
    Ok(match kind {
        DeltaInspectionType::Detail => detail::batch(state).await?,
        DeltaInspectionType::History => history::batch(state, history_limit).await?,
    })
}

pub(crate) fn memory_source(batch: RecordBatch) -> Result<Arc<dyn TableSource>> {
    // The immutable batch pins one reconciled snapshot. The shared memory-source codec handles
    // projection, filtering, joins, and worker transport without a format-specific physical node.
    Ok(provider_as_source(Arc::new(MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?)))
}

fn timestamp_micros(millis: i64) -> DeltaResult<i64> {
    millis
        .checked_mul(1_000)
        .ok_or_else(|| DeltaError::generic("Delta timestamp exceeds microsecond range"))
}
