use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::AlterTableOptions;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) async fn resolve_comment_on_table(
        &self,
        table: spec::ObjectName,
        value: Option<String>,
    ) -> PlanResult<LogicalPlan> {
        let options = match value {
            Some(value) => AlterTableOptions::SetTableProperties {
                properties: vec![("comment".into(), value)],
            },
            None => AlterTableOptions::UnsetTableProperties {
                keys: vec!["comment".into()],
                if_exists: true,
            },
        };
        self.resolve_comment(table.into(), options, "COMMENT ON TABLE")
            .await
    }

    pub(in super::super) async fn resolve_comment_on_column(
        &self,
        column: spec::ObjectName,
        value: Option<String>,
    ) -> PlanResult<LogicalPlan> {
        let mut table: Vec<String> = column.into();
        if table.len() < 2 {
            return Err(PlanError::invalid(
                "COMMENT ON COLUMN requires a table-qualified column name",
            ));
        }
        let column = table.split_off(table.len() - 1);
        let options = AlterTableOptions::AlterColumns {
            changes: vec![spec::ColumnChange::Comment {
                path: column,
                comment: value,
            }],
            case_sensitive: self.config.case_sensitive,
        };
        self.resolve_comment(table, options, "COMMENT ON COLUMN")
            .await
    }

    async fn resolve_comment(
        &self,
        table: Vec<String>,
        options: AlterTableOptions,
        operation: &str,
    ) -> PlanResult<LogicalPlan> {
        let status = self
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&table)
            .await?;
        if !matches!(status.kind, TableKind::Table { .. }) {
            return Err(PlanError::invalid(format!(
                "{operation} requires a table, but '{}' is a view",
                table.join(".")
            )));
        }
        self.resolve_catalog_command(CatalogCommand::AlterTable {
            table,
            if_exists: false,
            options,
        })
    }
}
