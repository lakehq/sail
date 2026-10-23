use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use fancy_regex::Regex;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{
    DataSourceRegistry, OptionLayer, SourceInfo, is_lakehouse_format,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;

use super::Resolver;
use crate::analysis::command::LogicalCommand;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) async fn resolve_show_table_properties(
        &mut self,
        table: spec::ObjectName,
        property: Option<String>,
    ) -> PlanResult<LogicalCommand> {
        let registry = self.context.session.extension::<DataSourceRegistry>()?;
        let path_table = match table.parts() {
            [format, path]
                if is_lakehouse_format(format.as_ref())
                    && (Path::new(path.as_ref()).is_absolute()
                        || Url::parse(path.as_ref()).is_ok_and(|url| !url.cannot_be_a_base())) =>
            {
                Some((
                    format.as_ref().to_ascii_lowercase(),
                    path.as_ref().to_string(),
                ))
            }
            _ => None,
        };
        let (properties, temporary) = if let Some((format, path)) = path_table {
            let properties = registry
                .get_lake_source(&format)?
                .table_properties(
                    &self.context.session.state(),
                    SourceInfo {
                        paths: vec![path],
                        lakehouse_table: None,
                        schema: None,
                        constraints: Default::default(),
                        partition_by: vec![],
                        bucket_by: None,
                        sort_order: vec![],
                        options: vec![],
                        read_case_sensitive: self.context.config.case_sensitive,
                    },
                )
                .await?;
            (properties, false)
        } else {
            let status = self
                .context
                .session
                .extension::<CatalogManager>()?
                .get_table_or_view(table.parts())
                .await?;
            let temporary = status.kind.is_temporary();
            let properties = match &status.kind {
                TableKind::Table {
                    format,
                    location,
                    properties,
                    ..
                } if is_lakehouse_format(format) => {
                    let location = location
                        .clone()
                        .ok_or_else(|| PlanError::invalid("table does not have a location"))?;
                    let table_name: Vec<String> = table.clone().into();
                    let context = self
                        .resolve_lakehouse_table_context(
                            &table_name,
                            LakehouseOperation::Read,
                            Some(format),
                            vec![],
                        )
                        .await?;
                    registry
                        .get_lake_source(format)?
                        .table_properties(
                            &self.context.session.state(),
                            SourceInfo {
                                paths: vec![location],
                                lakehouse_table: Some(context),
                                schema: None,
                                constraints: Default::default(),
                                partition_by: vec![],
                                bucket_by: None,
                                sort_order: vec![],
                                options: vec![OptionLayer::TablePropertyList {
                                    items: properties.clone(),
                                }],
                                read_case_sensitive: self.context.config.case_sensitive,
                            },
                        )
                        .await?
                }
                kind => kind.properties().to_vec(),
            };
            (properties, temporary)
        };
        let mut properties: BTreeMap<_, _> = properties
            .into_iter()
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "comment"
                        | "collation"
                        | "location"
                        | "provider"
                        | "owner"
                        | "external"
                        | "is_managed_location"
                        | "table_type"
                )
            })
            .collect();
        // Spark's SQLConf.redactOptions applies both patterns to keys and values.
        for pattern in [
            &self.context.config.redaction_options_regex,
            &self.context.config.redaction_regex,
        ] {
            let pattern = Regex::new(pattern).map_err(|error| {
                PlanError::invalid(format!("invalid property redaction pattern: {error}"))
            })?;
            for (key, value) in &mut properties {
                let is_match = |text: &str| {
                    pattern.is_match(text).map_err(|error| {
                        PlanError::invalid(format!("property redaction failed: {error}"))
                    })
                };
                if is_match(key)? || is_match(value)? {
                    *value = "*********(redacted)".to_string();
                }
            }
        }
        let rows: Vec<_> = if temporary {
            vec![]
        } else if let Some(key) = property {
            let value = properties.remove(&key).unwrap_or_else(|| {
                let name: Vec<String> = table.into();
                format!("Table {} does not have property: {key}", name.join("."))
            });
            vec![(key, value)]
        } else {
            properties.into_iter().collect()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(key, _)| key),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, value)| value),
                )),
            ],
        )?;
        let source = datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])?,
        ));
        Ok(LogicalCommand::Query(self.scan(
            "table_properties".into(),
            source,
            vec![],
        )?))
    }
}
