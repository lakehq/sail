use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CreateTableColumnOptions, CreateTableOptions, TableKind, TableStatus,
};
use sail_common::spec;
use sail_common_datafusion::datasource::{BucketBy, SinkMode};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::extension::logical::{FileWriteNode, FileWriteOptions, WithPreconditionsNode};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

pub(super) enum WriteTarget {
    Path(String),
    Table(spec::ObjectName),
}

pub(super) enum WriteMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    CreateOrOverwrite,
    Overwrite,
    OverwriteIf { condition: Box<spec::Expr> },
    OverwritePartitions,
}

/// A unified specification all write or insert operations.
pub(super) struct WriteSpec {
    pub input: Box<spec::QueryPlan>,
    pub partition: Vec<(spec::Identifier, spec::Expr)>,
    pub format: Option<String>,
    pub target: WriteTarget,
    pub mode: WriteMode,
    #[expect(unused)]
    pub columns: spec::WriteColumns,
    pub partition_by: Vec<spec::Identifier>,
    pub bucket_by: Option<spec::SaveBucketBy>,
    pub sort_by: Vec<spec::SortOrder>,
    pub cluster_by: Vec<spec::Identifier>,
    pub options: Vec<(String, String)>,
    pub table_properties: Vec<(String, String)>,
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_write(
        &self,
        write: spec::Write,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::{SaveMode, SaveType, TableSaveMethod, WriteColumns};

        let spec::Write {
            input,
            source,
            save_type,
            mode,
            sort_columns,
            partitioning_columns,
            clustering_columns,
            bucket_by,
            options,
        } = write;
        let (target, columns) = match save_type {
            SaveType::Path(path) => (WriteTarget::Path(path), WriteColumns::ByName),
            SaveType::Table { table, save_method } => match save_method {
                TableSaveMethod::SaveAsTable => (WriteTarget::Table(table), WriteColumns::ByName),
                TableSaveMethod::InsertInto => {
                    (WriteTarget::Table(table), WriteColumns::ByPosition)
                }
            },
        };
        let mode = match mode {
            SaveMode::Overwrite => WriteMode::CreateOrOverwrite,
            SaveMode::Append => WriteMode::Append,
            SaveMode::ErrorIfExists => WriteMode::ErrorIfExists,
            SaveMode::Ignore => WriteMode::IgnoreIfExists,
        };
        let write = WriteSpec {
            input,
            partition: vec![],
            format: source,
            target,
            mode,
            columns,
            partition_by: partitioning_columns,
            bucket_by,
            sort_by: sort_columns,
            cluster_by: clustering_columns,
            options,
            table_properties: vec![],
        };
        self.resolve_write_spec(write, state).await
    }

    pub(super) async fn resolve_command_write_to(
        &self,
        write_to: spec::WriteTo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::{WriteColumns, WriteToMode};

        let spec::WriteTo {
            input,
            provider,
            table,
            mode,
            partitioning_columns,
            clustering_columns,
            options,
            table_properties,
            overwrite_condition,
        } = write_to;
        let mode = match (mode, overwrite_condition) {
            (WriteToMode::Overwrite, Some(condition)) => WriteMode::OverwriteIf {
                condition: Box::new(condition),
            },
            (WriteToMode::Overwrite, None) => {
                return Err(PlanError::invalid("missing overwrite condition"));
            }
            (_, Some(_)) => {
                return Err(PlanError::invalid(
                    "overwrite condition only supported for overwrite mode",
                ));
            }
            (WriteToMode::Append, None) => WriteMode::Append,
            (WriteToMode::Create, None) => WriteMode::ErrorIfExists,
            (WriteToMode::CreateOrReplace, None) => WriteMode::CreateOrOverwrite,
            (WriteToMode::Replace, None) => WriteMode::Overwrite,
            (WriteToMode::OverwritePartitions, None) => WriteMode::OverwritePartitions,
        };
        let partition_by = partitioning_columns
            .into_iter()
            .map(|x| match x {
                spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                    is_metadata_column: false,
                } => {
                    let name: Vec<String> = name.into();
                    Ok(name.one()?.into())
                }
                // TODO: support functions for partitioning columns
                _ => Err(PlanError::invalid(
                    "partitioning column must be a column reference",
                )),
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let write = WriteSpec {
            input,
            partition: vec![],
            format: provider,
            target: WriteTarget::Table(table),
            mode,
            columns: WriteColumns::ByName,
            partition_by,
            bucket_by: None,
            sort_by: vec![],
            cluster_by: clustering_columns,
            options,
            table_properties,
        };
        self.resolve_write_spec(write, state).await
    }

    pub(super) async fn resolve_write_spec(
        &self,
        write: WriteSpec,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let WriteSpec {
            input,
            partition,
            format,
            target,
            mode,
            columns: _, // TODO
            partition_by,
            bucket_by,
            sort_by,
            cluster_by,
            options,
            table_properties,
        } = write;
        if !partition.is_empty() {
            return Err(PlanError::todo("PARTITION for write"));
        }
        if !cluster_by.is_empty() {
            return Err(PlanError::todo("CLUSTER BY for write"));
        }
        let mut preconditions = vec![];
        let input = self.resolve_query_plan(*input, state).await?;
        let format = if let Some(format) = format {
            format
        } else {
            self.config.default_table_file_format.clone()
        };
        let (input, options) = match target {
            WriteTarget::Path(path) => {
                let mode = match mode {
                    WriteMode::ErrorIfExists => SinkMode::ErrorIfExists,
                    WriteMode::IgnoreIfExists => SinkMode::IgnoreIfExists,
                    WriteMode::Append => SinkMode::Append,
                    WriteMode::CreateOrOverwrite | WriteMode::Overwrite => SinkMode::Overwrite,
                    WriteMode::OverwriteIf { condition } => {
                        let condition = self
                            .resolve_expression(*condition, input.schema(), state)
                            .await?;
                        SinkMode::OverwriteIf {
                            condition: Box::new(condition),
                        }
                    }
                    WriteMode::OverwritePartitions => SinkMode::OverwritePartitions,
                };
                let partition_by = self.resolve_write_partition_by(partition_by)?;
                let bucket_by = self.resolve_write_bucket_by(bucket_by)?;
                let sort_by = self
                    .resolve_sort_orders(sort_by, true, input.schema(), state)
                    .await?;
                let options = FileWriteOptions {
                    path,
                    mode,
                    format,
                    partition_by,
                    sort_by,
                    bucket_by,
                    options,
                };
                (input, options)
            }
            WriteTarget::Table(table) => {
                enum WriteTableAction {
                    None,
                    Create { replace: bool },
                }

                let status = match self
                    .ctx
                    .extension::<CatalogManager>()?
                    .get_table(table.parts())
                    .await
                {
                    Ok(x) => Some(x),
                    Err(CatalogError::NotFound(_, _)) => None,
                    Err(e) => return Err(e.into()),
                };
                let (mode, action) = match mode {
                    WriteMode::ErrorIfExists => {
                        if status.is_some() {
                            return Err(PlanError::invalid(format!(
                                "table already exists: {table:?}"
                            )));
                        }
                        (SinkMode::ErrorIfExists, WriteTableAction::None)
                    }
                    WriteMode::IgnoreIfExists => {
                        if status.is_some() {
                            return Ok(LogicalPlanBuilder::empty(false).build()?);
                        }
                        (
                            SinkMode::IgnoreIfExists,
                            WriteTableAction::Create { replace: false },
                        )
                    }
                    WriteMode::Append => (SinkMode::Append, WriteTableAction::None),
                    WriteMode::CreateOrOverwrite => {
                        if status.is_some() {
                            (
                                SinkMode::Overwrite,
                                WriteTableAction::Create { replace: true },
                            )
                        } else {
                            (
                                SinkMode::Append,
                                WriteTableAction::Create { replace: false },
                            )
                        }
                    }
                    WriteMode::Overwrite => (
                        SinkMode::Overwrite,
                        WriteTableAction::Create { replace: true },
                    ),
                    WriteMode::OverwriteIf { condition } => {
                        let condition = self
                            .resolve_expression(*condition, input.schema(), state)
                            .await?;
                        (
                            SinkMode::OverwriteIf {
                                condition: Box::new(condition),
                            },
                            WriteTableAction::None,
                        )
                    }
                    WriteMode::OverwritePartitions => {
                        (SinkMode::OverwritePartitions, WriteTableAction::None)
                    }
                };
                let location = match status {
                    Some(TableStatus {
                        kind: TableKind::Table { location, .. },
                        ..
                    }) => location,
                    Some(_) => {
                        return Err(PlanError::invalid(format!(
                            "invalid table status: {status:?}"
                        )));
                    }
                    None => None,
                };
                let location = if let Some(location) = location {
                    location
                } else {
                    self.resolve_default_table_location(&table)?
                };
                match action {
                    WriteTableAction::None => {}
                    WriteTableAction::Create { replace } => {
                        let columns = input
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| CreateTableColumnOptions {
                                name: f.name().clone(),
                                data_type: f.data_type().clone(),
                                nullable: f.is_nullable(),
                                comment: None,
                                default: None,
                                generated_always_as: None,
                            })
                            .collect();
                        let partition_by = partition_by.iter().map(|x| x.clone().into()).collect();
                        let sort_by = self.resolve_catalog_table_sort(sort_by.clone())?;
                        let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by.clone())?;
                        let command = CatalogCommand::CreateTable {
                            table: table.into(),
                            options: CreateTableOptions {
                                columns,
                                comment: None,
                                constraints: vec![],
                                location: Some(location.clone()),
                                format: format.clone(),
                                partition_by,
                                sort_by,
                                bucket_by,
                                if_not_exists: false,
                                replace,
                                options: options.clone(),
                                properties: table_properties,
                            },
                        };
                        let plan = self.resolve_catalog_command(command)?;
                        preconditions.push(Arc::new(plan));
                    }
                }
                let partition_by = self.resolve_write_partition_by(partition_by)?;
                let bucket_by = self.resolve_write_bucket_by(bucket_by)?;
                let sort_by = self
                    .resolve_sort_orders(sort_by, true, input.schema(), state)
                    .await?;
                let options = FileWriteOptions {
                    path: location,
                    format,
                    mode,
                    partition_by,
                    sort_by,
                    bucket_by,
                    options,
                };
                (input, options)
            }
        };

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(FileWriteNode::new(Arc::new(input), options)),
        });
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(WithPreconditionsNode::new(preconditions, Arc::new(plan))),
        }))
    }

    fn resolve_write_partition_by(
        &self,
        partition_by: Vec<spec::Identifier>,
    ) -> PlanResult<Vec<String>> {
        Ok(partition_by.into_iter().map(|x| x.into()).collect())
    }

    fn resolve_write_bucket_by(
        &self,
        bucket_by: Option<spec::SaveBucketBy>,
    ) -> PlanResult<Option<BucketBy>> {
        Ok(bucket_by.map(|x| {
            let spec::SaveBucketBy {
                bucket_column_names,
                num_buckets,
            } = x;
            BucketBy {
                columns: bucket_column_names.into_iter().map(|x| x.into()).collect(),
                num_buckets,
            }
        }))
    }
}
