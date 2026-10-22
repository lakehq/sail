use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, not_impl_err, plan_err};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::TableSource;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::datasource::SourceInfo;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakerelation::{
    LakeRelation, LakeRelationAccess, LakeRelationProvider, LakeRelationResolution,
    LakeRelationTimeTravel,
};
use sail_data_source::options::ResolveOptions;
use serde::Serialize;

mod detail;
mod history;

use crate::DeltaLakeSource;
use crate::delta_log::LogStoreRef;
use crate::options::r#gen::DeltaReadOptions;
use crate::snapshot::DeltaSnapshot;
use crate::spec::inspection::DeltaMetadataRelationType;
use crate::spec::{DeltaError, DeltaResult};
use crate::table::load_delta_read_state;

pub(crate) struct MetadataState {
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    table_name: Option<String>,
}

#[async_trait]
impl LakeRelationProvider for DeltaLakeSource {
    fn resolve_relation(&self, name: &str) -> LakeRelationResolution {
        let Some(kind) = DeltaMetadataRelationType::parse(name) else {
            return LakeRelationResolution::Unrecognized;
        };
        if !kind.is_supported() {
            return LakeRelationResolution::Unsupported {
                reason: kind.unsupported_reason(),
            };
        }
        LakeRelationResolution::Supported(LakeRelation::new(
            kind.name(),
            LakeRelationAccess::MetadataRead,
            LakeRelationTimeTravel::Unsupported,
        ))
    }

    async fn create_relation(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
        relation: LakeRelation,
    ) -> Result<Arc<dyn TableSource>> {
        let Some(kind) = DeltaMetadataRelationType::parse(relation.name()) else {
            return plan_err!("Unknown Delta metadata table: {}", relation.name());
        };
        let state = load_state(ctx, info, kind).await?;
        let batch = record_batch(&state, kind, None).await?;
        memory_source(batch)
    }
}

pub(crate) async fn load_state(
    ctx: &dyn Session,
    info: SourceInfo,
    kind: DeltaMetadataRelationType,
) -> Result<MetadataState> {
    if !kind.is_supported() {
        return not_impl_err!("{}", kind.unsupported_reason());
    }
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
        return plan_err!(
            "time travel is not supported for lake relation '{}'",
            kind.name()
        );
    }
    // CRCs may omit transaction and domain metadata state, so these relations need full replay.
    let metadata_only = !matches!(
        kind,
        DeltaMetadataRelationType::Detail
            | DeltaMetadataRelationType::Files
            | DeltaMetadataRelationType::Tombstones
            | DeltaMetadataRelationType::Transactions
            | DeltaMetadataRelationType::DomainMetadata
    );
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
    Ok(MetadataState {
        snapshot,
        log_store,
        table_name,
    })
}

pub(crate) async fn record_batch(
    state: &MetadataState,
    kind: DeltaMetadataRelationType,
    history_limit: Option<usize>,
) -> Result<RecordBatch> {
    let snapshot = &state.snapshot;
    Ok(match kind {
        DeltaMetadataRelationType::Detail => detail::batch(state).await?,
        DeltaMetadataRelationType::History => history::batch(state, history_limit).await?,
        DeltaMetadataRelationType::Files => action_batch(kind, snapshot.adds())?,
        DeltaMetadataRelationType::Tombstones => action_batch(kind, snapshot.removes())?,
        DeltaMetadataRelationType::Metadata => action_batch(kind, &[snapshot.metadata()])?,
        DeltaMetadataRelationType::Protocol => action_batch(kind, &[snapshot.protocol()])?,
        DeltaMetadataRelationType::Transactions => {
            action_batch(kind, &snapshot.app_txns().values().collect::<Vec<_>>())?
        }
        DeltaMetadataRelationType::DomainMetadata => action_batch(
            kind,
            &snapshot.domain_metadata().values().collect::<Vec<_>>(),
        )?,
        _ => return not_impl_err!("{}", kind.unsupported_reason()),
    })
}

fn action_batch<T: Serialize>(
    kind: DeltaMetadataRelationType,
    rows: &[T],
) -> DeltaResult<RecordBatch> {
    serde_arrow::to_record_batch(kind.schema()?.fields(), &rows).map_err(DeltaError::generic_err)
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
