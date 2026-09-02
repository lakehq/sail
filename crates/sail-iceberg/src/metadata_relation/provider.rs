use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::{MemTable, Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use url::Url;

use super::{IcebergMetadataRelationType, files};
use crate::table::Table;

pub(crate) async fn metadata_relation_provider(
    session: &dyn Session,
    table_url: Url,
    mut metadata_location: Option<String>,
    relation_type: IcebergMetadataRelationType,
) -> Result<Arc<dyn TableProvider>> {
    if !relation_type.is_supported() {
        return Err(DataFusionError::NotImplemented(
            relation_type.unsupported_reason(),
        ));
    }
    let schema = if matches!(relation_type, IcebergMetadataRelationType::Files) {
        let table = Table::load_with_metadata_location(
            session,
            table_url.clone(),
            metadata_location.clone(),
        )
        .await?;
        metadata_location = Some(table.metadata_location().to_string());
        files::schema(table.metadata())?
    } else {
        relation_type.schema()
    };
    Ok(Arc::new(IcebergMetadataRelationProvider {
        table_url,
        metadata_location,
        relation_type,
        schema,
    }))
}

struct IcebergMetadataRelationProvider {
    table_url: Url,
    metadata_location: Option<String>,
    relation_type: IcebergMetadataRelationType,
    schema: Arc<ArrowSchema>,
}

impl std::fmt::Debug for IcebergMetadataRelationProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergMetadataRelationProvider")
            .field("table_url", &self.table_url)
            .field("metadata_location", &self.metadata_location)
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
        let table = Table::load_with_metadata_location(
            session,
            self.table_url.clone(),
            self.metadata_location.clone(),
        )
        .await?;
        let batch = self.relation_type.record_batch(&table).await?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        table.scan(session, projection, filters, limit).await
    }
}
