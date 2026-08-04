use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{Result, not_impl_err};

use crate::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use crate::datasource::{DataSource, DeleteInfo, MergeInfo, SourceInfo};

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

/// Information needed by a lake source to define table storage metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSourceCreateTableInfo {
    pub path: String,
    pub columns: Vec<LakeSourceCreateTableColumn>,
    pub comment: Option<String>,
    pub partition_by: Vec<CatalogPartitionField>,
    pub properties: Vec<(String, String)>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

impl LakeSourceCreateTableInfo {
    pub fn catalog_table(&self) -> Option<&[String]> {
        self.lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table())
    }
}

/// A row-level operation that requires lake-source-specific logical planning.
#[derive(Debug, Clone)]
pub enum RowLevelOperation {
    Delete(Box<DeleteInfo>),
    // Update(Box<UpdateInfo>),
    Merge(Box<MergeInfo>),
}

/// A storage metadata change applied by a lake source.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataChange {
    Create(LakeSourceCreateTableInfo),
    Replace(LakeSourceCreateTableInfo),
    Alter {
        path: String,
        operation: LakeSourceAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    },
    // Drop(LakeSourceDropTableInfo),
}

/// Catalog-visible metadata produced by a lake source change.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataChangeResult {
    pub properties: Vec<(String, String)>,
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

/// A lakehouse data source with metadata and row-level operation semantics.
#[async_trait]
pub trait LakeSource: DataSource {
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
            RowLevelOperation::Merge(_) => not_impl_err!(
                "MERGE is not yet implemented for lake source '{}'",
                self.name()
            ),
        }
    }

    /// Applies a storage metadata change.
    ///
    /// `Create` and `Replace` run before catalog registration. Lake sources that
    /// do not need storage metadata at definition time can keep the default no-op.
    async fn apply_metadata_change(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        change: MetadataChange,
    ) -> Result<MetadataChangeResult> {
        match change {
            MetadataChange::Create(_) | MetadataChange::Replace(_) => {
                Ok(MetadataChangeResult::default())
            }
            MetadataChange::Alter {
                path,
                operation,
                lakehouse_table,
            } => {
                let _ = (runtime_env, path, lakehouse_table);
                let operation = match operation {
                    LakeSourceAlterTableOperation::SetTableProperties { .. } => {
                        "table properties alteration"
                    }
                    LakeSourceAlterTableOperation::AlterColumnType { .. } => {
                        "column type alteration"
                    }
                    LakeSourceAlterTableOperation::AlterColumnDefault { .. } => {
                        "column default alteration"
                    }
                    LakeSourceAlterTableOperation::AddCheckConstraint { .. } => {
                        "CHECK constraint alteration"
                    }
                };
                not_impl_err!(
                    "{operation} not supported for lake source '{}'",
                    self.name()
                )
            }
        }
    }
}
