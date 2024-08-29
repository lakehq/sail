use arrow::datatypes as adt;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion_common::plan_err;
use futures::TryStreamExt;
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_listing_urls(
        &self,
        paths: Vec<String>,
    ) -> PlanResult<Vec<ListingTableUrl>> {
        let mut urls = vec![];
        for path in paths {
            let url = ListingTableUrl::parse(&path)?;
            let store = self.ctx.runtime_env().object_store(&url)?;
            if store.head(url.prefix()).await.is_ok() {
                urls.push(url);
            } else {
                // The object at the path does not exist, so we treat it as a directory.
                let path = if path.ends_with(object_store::path::DELIMITER) {
                    path
                } else {
                    format!("{}{}", path, object_store::path::DELIMITER)
                };
                urls.push(ListingTableUrl::parse(path)?);
            }
        }
        Ok(urls)
    }

    pub(super) async fn resolve_listing_schema(
        &self,
        urls: &[ListingTableUrl],
        options: &ListingOptions,
        schema: Option<spec::Schema>,
    ) -> PlanResult<adt::Schema> {
        let schema = match schema {
            // ignore empty schema
            Some(spec::Schema { fields }) if fields.0.is_empty() => None,
            x => x,
        };
        // The logic is similar to `ListingOptions::infer_schema()`
        // but here we also check for the existence of files.
        let state = self.ctx.state();
        let mut file_groups = vec![];
        for url in urls {
            let store = self.ctx.runtime_env().object_store(url)?;
            let files: Vec<_> = url
                .list_all_files(&state, &store, &options.file_extension)
                .await?
                .try_collect()
                .await?;
            file_groups.push((store, files));
        }
        let empty = file_groups.iter().all(|(_, files)| files.is_empty());
        if empty {
            let urls = urls
                .iter()
                .map(|url| url.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return plan_err!("No files found in the specified paths: {urls}")?;
        }
        match schema {
            Some(schema) => Ok(self.resolve_schema(schema)?),
            None => {
                let mut schemas = vec![];
                for (store, files) in file_groups.iter() {
                    let schema = options.format.infer_schema(&state, store, files).await?;
                    schemas.push(schema.as_ref().clone());
                }
                Ok(adt::Schema::try_merge(schemas)?)
            }
        }
    }
}
