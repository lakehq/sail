use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::prelude::SessionContext;
use datafusion_common::{plan_err, Result};
use sail_common::spec::SaveMode;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo};

use crate::registry::{default_registry, TableFormatRegistry};

pub struct TableProviderFactory<'a> {
    ctx: &'a SessionContext,
    registry: Arc<TableFormatRegistry>,
}

impl<'a> TableProviderFactory<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        TableProviderFactory {
            ctx,
            registry: default_registry(),
        }
    }

    pub async fn read_table(
        &self,
        format: &str,
        paths: Vec<String>,
        schema: Option<Schema>,
        options: Vec<(String, String)>,
    ) -> Result<Arc<dyn TableProvider>> {
        if paths.is_empty() {
            return plan_err!("empty data source paths");
        }

        let options: HashMap<String, String> = options.into_iter().collect();

        let format_provider = self.registry.get_format(format)?;

        // FIXME: or try?
        let info = SourceInfo {
            ctx: self.ctx,
            paths,
            schema,
            options,
        };

        format_provider.create_provider(info).await
    }

    pub fn write_table(
        &self,
        source: &str,
        mode: SaveMode,
        options: Vec<(String, String)>,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        let options: HashMap<String, String> = options.into_iter().collect();
        let format = self.registry.get_format(source)?;

        let info = SinkInfo {
            ctx: self.ctx,
            mode,
            options,
        };

        format.create_writer(info)
    }
}
