use std::sync::Arc;

use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;

use crate::table_format::parse_location_to_url;

pub(crate) async fn resolve_catalog_table_url(
    context: &Arc<TaskContext>,
    catalog_table: Option<&[String]>,
    fallback_url: &Url,
) -> Result<Url> {
    let Some(catalog_table) = catalog_table else {
        return Ok(fallback_url.clone());
    };
    let manager = context.extension::<CatalogManager>()?;
    let status = manager
        .get_table(catalog_table)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let TableKind::Table { location, .. } = status.kind else {
        return Ok(fallback_url.clone());
    };
    location
        .as_deref()
        .map(parse_location_to_url)
        .transpose()
        .map(|url| url.unwrap_or_else(|| fallback_url.clone()))
}
