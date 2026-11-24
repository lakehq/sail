// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use url::Url;

use super::{IcebergMetadataRelationType, files};
use crate::physical_plan::metadata_relation_exec::{
    IcebergMetadataRelationExec, MetadataRelationScan, MetadataScanSource,
};
use crate::table::Table;
use crate::table::files::{
    EntrySelection, ManifestScope, SnapshotScope, balance_by_size, scan_manifests,
};

pub(crate) async fn metadata_relation_provider(
    session: &dyn Session,
    table_url: Url,
    metadata_location: Option<String>,
    relation_type: IcebergMetadataRelationType,
    storage_access: Option<Box<sail_common::storage::StorageAccessSpec>>,
) -> Result<Arc<dyn TableProvider>> {
    if !relation_type.is_supported() {
        return Err(DataFusionError::NotImplemented(
            relation_type.unsupported_reason(),
        ));
    }
    let runtime = match storage_access.as_deref() {
        Some(spec) => sail_object_store::access::storage_runtime(session.runtime_env(), spec)?,
        None => session.runtime_env().clone(),
    };
    let table = Arc::new(
        Table::load_with_metadata_location(runtime.as_ref(), table_url, metadata_location).await?,
    );
    let schema = if relation_type == IcebergMetadataRelationType::Files {
        files::schema(table.metadata())?
    } else {
        relation_type.schema()
    };
    Ok(Arc::new(IcebergMetadataRelationProvider {
        table,
        storage_access,
        runtime,
        relation_type,
        schema,
    }))
}

struct IcebergMetadataRelationProvider {
    storage_access: Option<Box<sail_common::storage::StorageAccessSpec>>,
    runtime: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    table: Arc<Table>,
    relation_type: IcebergMetadataRelationType,
    schema: Arc<ArrowSchema>,
}

impl std::fmt::Debug for IcebergMetadataRelationProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergMetadataRelationProvider")
            .field("table_url", self.table.table_url())
            .field("metadata_location", &self.table.metadata_location())
            .field("relation_type", &self.relation_type)
            .finish()
    }
}

#[async_trait]
impl TableProvider for IcebergMetadataRelationProvider {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = if self.relation_type == IcebergMetadataRelationType::Files {
            MetadataScanSource::ManifestEntries {
                groups: balance_by_size(
                    scan_manifests(
                        self.table.store_context(),
                        self.table.metadata(),
                        &ManifestScope {
                            snapshots: SnapshotScope::Current,
                            content: None,
                        },
                    )
                    .await?,
                    session.config().target_partitions(),
                    |manifest| manifest.manifest_length.max(0) as u64,
                ),
                selection: EntrySelection::live(),
            }
        } else {
            MetadataScanSource::TableMetadata
        };
        // Filters remain residual expressions above this scan.
        let limit = if filters.is_empty() { limit } else { None };
        let plan = Arc::new(IcebergMetadataRelationExec::try_new(
            self.schema.clone(),
            MetadataRelationScan {
                table_url: self.table.table_url().to_string(),
                metadata_location: self.table.metadata_location().to_string(),
                relation: self.relation_type,
                source,
                projection: projection
                    .cloned()
                    .unwrap_or_else(|| (0..self.schema.fields().len()).collect()),
                limit,
            },
        )?);
        match self.storage_access.as_deref() {
            Some(spec) => crate::storage_access::bind_scan_storage(plan, spec, &self.runtime),
            None => Ok(plan),
        }
    }
}
