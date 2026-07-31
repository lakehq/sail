use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::Session;
use datafusion::common::plan_datafusion_err;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{DFSchemaRef, Result, not_impl_err};
use datafusion_expr::Expr;

use crate::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use crate::data_source_format::DataSourceFormat;
use crate::datasource::OptionLayer;
use crate::extension::SessionExtension;
use crate::logical_expr::ExprWithSource;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFormatCreateTableColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
    pub generated_always_as: Option<String>,
    pub identity: Option<crate::catalog::CatalogTableColumnIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFormatCreateTableInfo {
    pub path: String,
    pub columns: Vec<TableFormatCreateTableColumn>,
    pub comment: Option<String>,
    pub partition_by: Vec<CatalogPartitionField>,
    pub properties: Vec<(String, String)>,
    pub replace: bool,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

impl TableFormatCreateTableInfo {
    pub fn catalog_table(&self) -> Option<&[String]> {
        self.lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableFormatCreateTableResult {
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct DeleteInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub condition: Option<ExprWithSource>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub options: Vec<OptionLayer>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct MergeInfo {
    pub target: Arc<LogicalPlan>,
    pub source: Arc<LogicalPlan>,
    pub options: MergeIntoOptions,
    pub input_schema: DFSchemaRef,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeIntoOptions {
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub target: MergeTargetInfo,
    pub with_schema_evolution: bool,
    pub resolved_target_schema: DFSchemaRef,
    pub resolved_source_schema: DFSchemaRef,
    pub resolved_target_field_names: Vec<String>,
    pub resolved_source_field_names: Vec<String>,
    pub on_condition: ExprWithSource,
    pub matched_clauses: Vec<MergeMatchedClause>,
    pub not_matched_by_source_clauses: Vec<MergeNotMatchedBySourceClause>,
    pub not_matched_by_target_clauses: Vec<MergeNotMatchedByTargetClause>,
    pub join_key_pairs: Vec<(Expr, Expr)>,
    pub residual_predicates: Vec<Expr>,
    pub target_only_predicates: Vec<Expr>,
    pub generated_column_exprs: Vec<(String, Expr)>,
    pub check_constraint_exprs: Vec<DeltaCheckConstraintExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct MergeTargetInfo {
    pub table_name: Vec<String>,
    pub format: String,
    pub location: String,
    pub partition_by: Vec<String>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeMatchedClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeMatchedAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeMatchedAction {
    Delete,
    UpdateAll,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeNotMatchedBySourceClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeNotMatchedBySourceAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeNotMatchedBySourceAction {
    Delete,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeNotMatchedByTargetClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeNotMatchedByTargetAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeNotMatchedByTargetAction {
    InsertAll,
    InsertColumns {
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeAssignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DeltaCheckConstraintExpr {
    pub name: String,
    pub expression: String,
    pub expr: Expr,
    pub violation: DeltaConstraintViolation,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DeltaConstraintViolation {
    Check,
    NotNull { column: String },
    Invariant { column: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFormatAlterTableOperation {
    SetTableProperties {
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    },
    AlterColumnType {
        column_path: Vec<String>,
        data_type: DataType,
    },
    AlterColumnDefault {
        column_path: Vec<String>,
        default: Option<String>,
    },
    AddCheckConstraint {
        name: String,
        expression: String,
    },
}

#[async_trait]
pub trait TableFormat: DataSourceFormat {
    async fn create_table_metadata(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        info: TableFormatCreateTableInfo,
    ) -> Result<TableFormatCreateTableResult> {
        let _ = (runtime_env, info);
        Ok(TableFormatCreateTableResult::default())
    }

    async fn create_deleter(&self, ctx: &dyn Session, info: DeleteInfo) -> Result<LogicalPlan> {
        let _ = (ctx, info);
        not_impl_err!("DELETE is not yet implemented for {} format", self.name())
    }

    async fn create_merger(&self, ctx: &dyn Session, info: MergeInfo) -> Result<LogicalPlan> {
        let _ = (ctx, info);
        not_impl_err!("MERGE is not yet implemented for {} format", self.name())
    }

    async fn alter_table(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        operation: TableFormatAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<()> {
        let _ = lakehouse_table;
        match operation {
            TableFormatAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(runtime_env, path, changes, if_exists)
                    .await
            }
            TableFormatAlterTableOperation::AlterColumnType {
                column_path,
                data_type,
            } => {
                self.alter_table_column_type(runtime_env, path, column_path, data_type)
                    .await
            }
            TableFormatAlterTableOperation::AlterColumnDefault {
                column_path,
                default,
            } => {
                self.alter_table_column_default(runtime_env, path, column_path, default)
                    .await
            }
            TableFormatAlterTableOperation::AddCheckConstraint { .. } => {
                not_impl_err!(
                    "CHECK constraint alteration not supported for {} format",
                    self.name()
                )
            }
        }
    }

    async fn alter_table_properties(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        let _ = (runtime_env, path, changes, if_exists);
        not_impl_err!(
            "Table properties alteration not supported for {} format",
            self.name()
        )
    }

    async fn alter_table_column_type(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        column_path: Vec<String>,
        data_type: DataType,
    ) -> Result<()> {
        let _ = (runtime_env, path, column_path, data_type);
        not_impl_err!(
            "Column type alteration not supported for {} format",
            self.name()
        )
    }

    async fn alter_table_column_default(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        column_path: Vec<String>,
        default: Option<String>,
    ) -> Result<()> {
        let _ = (runtime_env, path, column_path, default);
        not_impl_err!(
            "Column default alteration not supported for {} format",
            self.name()
        )
    }
}

#[derive(Default)]
pub struct TableFormatRegistry {
    formats: RwLock<HashMap<String, Arc<dyn TableFormat>>>,
}

impl TableFormatRegistry {
    pub fn new() -> Self {
        Self {
            formats: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, format: Arc<dyn TableFormat>) -> Result<()> {
        let mut formats = self
            .formats
            .write()
            .map_err(|_| plan_datafusion_err!("table format registry poisoned"))?;
        formats.insert(format.name().to_lowercase(), format);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn TableFormat>> {
        self.get_optional(name)?
            .ok_or_else(|| plan_datafusion_err!("No table format found for: {name}"))
    }

    pub fn get_optional(&self, name: &str) -> Result<Option<Arc<dyn TableFormat>>> {
        let formats = self
            .formats
            .read()
            .map_err(|_| plan_datafusion_err!("table format registry poisoned"))?;
        Ok(formats.get(&name.to_lowercase()).cloned())
    }
}

impl SessionExtension for TableFormatRegistry {
    fn name() -> &'static str {
        "TableFormatRegistry"
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::TableSource;

    use super::*;
    use crate::data_source_format::{DataSourceFormat, DataSourceFormatRegistry};
    use crate::datasource::{SinkInfo, SourceInfo};

    struct TestProviderFormat;

    #[async_trait]
    impl DataSourceFormat for TestProviderFormat {
        fn name(&self) -> &str {
            "DELTA"
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            not_impl_err!("test provider does not create sources")
        }

        async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
            not_impl_err!("test provider does not create writers")
        }
    }

    struct TestTableFormat;

    #[async_trait]
    impl DataSourceFormat for TestTableFormat {
        fn name(&self) -> &str {
            "delta"
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            not_impl_err!("test table format does not create sources")
        }

        async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
            not_impl_err!("test table format does not create writers")
        }
    }

    impl TableFormat for TestTableFormat {}

    #[test]
    fn provider_override_does_not_replace_table_protocol() -> Result<()> {
        let data_source_formats = DataSourceFormatRegistry::new();
        let table_formats = TableFormatRegistry::new();
        let protocol = Arc::new(TestTableFormat);
        data_source_formats.register(protocol.clone())?;
        table_formats.register(protocol)?;
        let original_data_source = data_source_formats.get("Delta")?;
        let original_protocol = table_formats.get("DELTA")?;

        data_source_formats.register(Arc::new(TestProviderFormat))?;

        let overridden_data_source = data_source_formats.get("delta")?;
        let retained_protocol = table_formats.get("DeLtA")?;
        assert!(!Arc::ptr_eq(&original_data_source, &overridden_data_source));
        assert!(Arc::ptr_eq(&original_protocol, &retained_protocol));
        Ok(())
    }
}
