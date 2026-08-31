use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{Result, not_impl_err};

use crate::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use crate::datasource::{DataSource, DeleteInfo, MergeInfo, SourceInfo, UpdateInfo};
use crate::lakeprocedure::LakeProcedureProvider;
use crate::lakerelation::LakeRelationProvider;

/// Metadata about an existing lake source needed during logical planning.
#[derive(Debug, Clone)]
pub struct LakeSourceMetadata {
    pub schema: SchemaRef,
    pub properties: Vec<(String, String)>,
}

/// A column definition used when catalog DDL asks a lake source to create
/// storage metadata before registering the catalog object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSourceCreateTableColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
    pub generated_always_as: Option<String>,
    pub identity: Option<crate::catalog::CatalogTableColumnIdentity>,
}

/// Information needed by a lake source to initialize storage metadata for a
/// plain catalog `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSourceCreateTableInfo {
    pub path: String,
    pub columns: Vec<LakeSourceCreateTableColumn>,
    pub comment: Option<String>,
    pub partition_by: Vec<CatalogPartitionField>,
    pub properties: Vec<(String, String)>,
    pub replace: bool,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

impl LakeSourceCreateTableInfo {
    pub fn catalog_table(&self) -> Option<&[String]> {
        self.lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table())
    }
}

/// Storage metadata created by a lake source before catalog registration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LakeSourceCreateTableResult {
    pub properties: Vec<(String, String)>,
}

/// A row-level operation that requires lake-source-specific planning.
#[derive(Debug, Clone)]
pub enum RowLevelOperation {
    Delete(Box<DeleteInfo>),
    Update(Box<UpdateInfo>),
    Merge(Box<MergeInfo>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LakeSourceAlterTableOperation {
    /// Alters table properties (SET/UNSET TBLPROPERTIES).
    ///
    /// `changes` is a list of `(key, value)` pairs where `value` is `Some(v)` to set a property,
    /// or `None` to unset/remove it. When `if_exists` is `false`, implementations MUST error if
    /// an UNSET key is not present on the table; when `if_exists` is `true`, UNSET for a missing
    /// key is a no-op. The implementation is responsible for committing these changes to the
    /// underlying table storage (e.g., writing a new Delta log entry).
    SetTableProperties {
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    },
    /// Alters the type of a table column.
    AlterColumnType {
        column_path: Vec<String>,
        data_type: DataType,
    },
    /// Alters the default expression of a table column.
    AlterColumnDefault {
        column_path: Vec<String>,
        default: Option<String>,
    },
    /// Adds a CHECK constraint after the caller has validated existing rows.
    AddCheckConstraint { name: String, expression: String },
}

/// A lakehouse data source with table metadata, DML, DDL, and optional
/// relation/procedure capabilities.
#[async_trait]
pub trait LakeSource: DataSource {
    fn relation_provider(self: Arc<Self>) -> Option<Arc<dyn LakeRelationProvider>> {
        None
    }

    fn procedure_provider(self: Arc<Self>) -> Option<Arc<dyn LakeProcedureProvider>> {
        None
    }

    /// Infers table metadata for planning without requiring callers to construct a read source.
    async fn infer_metadata(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<LakeSourceMetadata> {
        Ok(LakeSourceMetadata {
            schema: self.infer_schema(ctx, info).await?,
            properties: vec![],
        })
    }

    /// Creates storage metadata for a plain catalog `CREATE TABLE` before the
    /// catalog object is registered. Lake sources that do not need storage metadata
    /// at DDL time can keep the default no-op.
    async fn create_table_metadata(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        info: LakeSourceCreateTableInfo,
    ) -> Result<LakeSourceCreateTableResult> {
        let _ = (runtime_env, info);
        Ok(LakeSourceCreateTableResult::default())
    }

    /// Creates a logical plan for a row-level operation.
    async fn plan_row_level_operation(
        &self,
        ctx: &dyn Session,
        operation: RowLevelOperation,
    ) -> Result<LogicalPlan> {
        let _ = ctx;
        match operation {
            RowLevelOperation::Delete(_) => not_impl_err!(
                "DELETE is not yet implemented for lake source '{}'",
                self.name()
            ),
            RowLevelOperation::Update(_) => not_impl_err!(
                "UPDATE is not yet implemented for lake source '{}'",
                self.name()
            ),
            RowLevelOperation::Merge(_) => not_impl_err!(
                "MERGE is not yet implemented for lake source '{}'",
                self.name()
            ),
        }
    }

    /// Alters storage metadata for an existing lake source.
    async fn alter_table(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        operation: LakeSourceAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<()> {
        let _ = lakehouse_table;
        match operation {
            LakeSourceAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(runtime_env, path, changes, if_exists)
                    .await
            }
            LakeSourceAlterTableOperation::AlterColumnType {
                column_path,
                data_type,
            } => {
                self.alter_table_column_type(runtime_env, path, column_path, data_type)
                    .await
            }
            LakeSourceAlterTableOperation::AlterColumnDefault {
                column_path,
                default,
            } => {
                self.alter_table_column_default(runtime_env, path, column_path, default)
                    .await
            }
            LakeSourceAlterTableOperation::AddCheckConstraint { .. } => {
                not_impl_err!(
                    "CHECK constraint alteration not supported for lake source '{}'",
                    self.name()
                )
            }
        }
    }

    /// Alters table properties (SET/UNSET TBLPROPERTIES).
    ///
    /// `changes` is a list of `(key, value)` pairs where `value` is `Some(v)` to set a property,
    /// or `None` to unset/remove it. When `if_exists` is `false`, implementations MUST error if
    /// an UNSET key is not present on the table; when `if_exists` is `true`, UNSET for a missing
    /// key is a no-op. The implementation is responsible for committing these changes to the
    /// underlying table storage (e.g., writing a new Delta log entry).
    async fn alter_table_properties(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        let _ = (runtime_env, path, changes, if_exists);
        not_impl_err!(
            "Table properties alteration not supported for lake source '{}'",
            self.name()
        )
    }

    /// Alters the type of a table column.
    async fn alter_table_column_type(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        column_path: Vec<String>,
        data_type: DataType,
    ) -> Result<()> {
        let _ = (runtime_env, path, column_path, data_type);
        not_impl_err!(
            "Column type alteration not supported for lake source '{}'",
            self.name()
        )
    }

    /// Alters the default expression of a table column.
    async fn alter_table_column_default(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        path: &str,
        column_path: Vec<String>,
        default: Option<String>,
    ) -> Result<()> {
        let _ = (runtime_env, path, column_path, default);
        not_impl_err!(
            "Column default alteration not supported for lake source '{}'",
            self.name()
        )
    }
}
