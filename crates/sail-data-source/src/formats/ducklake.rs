use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, Result};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_duck_lake::create_ducklake_provider;
use sail_duck_lake::options::DuckLakeOptions;

use crate::options::{load_default_options, load_options, DuckLakeReadOptions};

#[derive(Debug, Default)]
pub struct DuckLakeDataSourceFormat;

#[async_trait]
impl TableFormat for DuckLakeDataSourceFormat {
    fn name(&self) -> &str {
        "ducklake"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths: _,
            schema: _,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let options = resolve_ducklake_read_options(options)?;
        create_ducklake_provider(ctx, options).await
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Writing to DuckLake tables is not yet supported")
    }
}

fn apply_ducklake_read_options(from: DuckLakeReadOptions, to: &mut DuckLakeOptions) -> Result<()> {
    if let Some(url) = from.url {
        to.url = url;
    }
    if let Some(table) = from.table {
        to.table = table;
    }
    if let Some(base_path) = from.base_path {
        to.base_path = base_path;
    }
    if let Some(snapshot_id) = from.snapshot_id {
        to.snapshot_id = Some(snapshot_id);
    }
    if let Some(schema) = from.schema {
        to.schema = Some(schema);
    }
    if let Some(case_sensitive) = from.case_sensitive {
        to.case_sensitive = case_sensitive;
    }
    Ok(())
}

pub fn resolve_ducklake_read_options(
    options: Vec<HashMap<String, String>>,
) -> Result<DuckLakeOptions> {
    let mut ducklake_options = DuckLakeOptions::default();
    apply_ducklake_read_options(load_default_options()?, &mut ducklake_options)?;
    for opt in options {
        apply_ducklake_read_options(load_options(opt)?, &mut ducklake_options)?;
    }
    ducklake_options.validate()?;
    Ok(ducklake_options)
}
