use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, Result};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_duck_lake::create_ducklake_provider;
use sail_duck_lake::options::DuckLakeOptions;
use url::Url;

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
            paths,
            schema: _,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;

        // Prefer location-first (ducklake+sqlite://...) if provided
        let loc_opts = match paths.as_slice() {
            [single] => parse_ducklake_location(single)?,
            _ => None,
        };

        let ducklake_options = if let Some(mut base_opts) = loc_opts {
            // Merge additive options (snapshot_id, case_sensitive) from defaults and provided options
            let mut merged = DuckLakeOptions::default();
            apply_ducklake_read_options(load_default_options()?, &mut merged)?;
            for opt in options {
                apply_ducklake_read_options(load_options(opt)?, &mut merged)?;
            }
            if let Some(snap) = merged.snapshot_id {
                base_opts.snapshot_id = Some(snap);
            }
            base_opts.case_sensitive = merged.case_sensitive;
            base_opts.validate()?;
            base_opts
        } else {
            log::warn!(
                "DuckLake: location not provided; falling back to options; location is preferred"
            );
            resolve_ducklake_read_options(options)?
        };

        create_ducklake_provider(ctx, ducklake_options).await
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

// Parse a location string like:
// ducklake+sqlite:///path/to/metadata.ducklake/analytics/metrics?base_path=file:///path/to/data/&snapshot_id=1
// Returns Ok(None) if the scheme is not ducklake+*
fn parse_ducklake_location(path: &str) -> Result<Option<DuckLakeOptions>> {
    if !path.starts_with("ducklake+") {
        return Ok(None);
    }
    let url =
        Url::parse(path).map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
    let scheme = url.scheme();
    if !scheme.starts_with("ducklake+") {
        return Ok(None);
    }

    let meta_scheme = &scheme["ducklake+".len()..];
    if meta_scheme != "sqlite" && meta_scheme != "postgres" && meta_scheme != "postgresql" {
        return Err(datafusion::common::DataFusionError::Plan(format!(
            "Unsupported DuckLake meta scheme: {}",
            meta_scheme
        )));
    }

    // Identify metadata file (*.ducklake) and table path segments after it
    let segments: Vec<String> = url
        .path_segments()
        .map(|s| s.map(|p| p.to_string()).collect())
        .unwrap_or_else(Vec::new);

    let split_idx = segments
        .iter()
        .position(|s| s.ends_with(".ducklake"))
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "Missing metadata .ducklake file in location".into(),
            )
        })?;

    let db_parts = &segments[..=split_idx];
    let table_parts = &segments[split_idx + 1..];

    let table = match table_parts {
        [t] => t.clone(),
        [s, t] => format!("{}.{t}", s),
        [] => url
            .query_pairs()
            .find(|(k, _)| k == "table")
            .map(|(_, v)| v.to_string())
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan("Missing table in location".into())
            })?,
        _ => {
            return Err(datafusion::common::DataFusionError::Plan(
                "Invalid table path in location".into(),
            ));
        }
    };

    // Reconstruct metadata URL (strip query, keep path up to .ducklake)
    let mut meta_url = url.clone();
    meta_url.set_query(None);
    let db_path = format!("/{}", db_parts.join("/"));
    meta_url.set_path(&db_path);
    let host = meta_url.host_str().unwrap_or("");
    let url_str = if host.is_empty() {
        format!("{}://{}", meta_scheme, meta_url.path())
    } else {
        format!("{}://{}{}", meta_scheme, host, meta_url.path())
    };

    // Required base_path
    let base_path = url
        .query_pairs()
        .find(|(k, _)| k == "base_path")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Plan("Missing base_path query param".into())
        })?;

    // Optional params
    let snapshot_id = url
        .query_pairs()
        .find(|(k, _)| k == "snapshot_id")
        .and_then(|(_, v)| v.parse::<u64>().ok());
    let case_sensitive = url
        .query_pairs()
        .find(|(k, _)| k == "case_sensitive")
        .is_some_and(|(_, v)| v == "true");

    Ok(Some(DuckLakeOptions {
        url: url_str,
        table,
        base_path,
        snapshot_id,
        schema: None,
        case_sensitive,
    }))
}
