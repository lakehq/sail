use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_expr::{col, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CatalogTableBucketBy, CatalogTableSort, CreateTableColumnOptions, CreateTableOptions,
    TableColumnStatus, TableKind,
};
use sail_common::spec;
use sail_common::spec::WriteToMode;
use sail_common_datafusion::datasource::{BucketBy, SinkMode};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::utils::rename_logical_plan;

use crate::error::{PlanError, PlanResult};
use crate::extension::logical::{FileWriteNode, FileWriteOptions, WithPreconditionsNode};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

pub(super) enum WriteTarget {
    Path {
        location: String,
    },
    ExistingTable {
        table: spec::ObjectName,
        column_match: WriteColumnMatch,
    },
    NewTable {
        table: spec::ObjectName,
        action: WriteTableAction,
    },
}

#[expect(clippy::enum_variant_names)]
pub(super) enum WriteColumnMatch {
    ByPosition,
    ByName,
    ByColumns { columns: Vec<spec::Identifier> },
}

pub(super) enum WriteTableAction {
    Create,
    CreateOrReplace,
    Replace,
}

/// A unified logical plan builder for all write or insert operations.
pub(super) struct WritePlanBuilder {
    target: Option<WriteTarget>,
    mode: Option<SinkMode>,
    format: Option<String>,
    partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
    partition_by: Vec<spec::Identifier>,
    bucket_by: Option<spec::SaveBucketBy>,
    sort_by: Vec<spec::SortOrder>,
    cluster_by: Vec<spec::ObjectName>,
    options: Vec<(String, String)>,
    table_properties: Vec<(String, String)>,
}

impl WritePlanBuilder {
    pub fn new() -> Self {
        Self {
            target: None,
            mode: None,
            format: None,
            partition: vec![],
            partition_by: vec![],
            bucket_by: None,
            sort_by: vec![],
            cluster_by: vec![],
            options: vec![],
            table_properties: vec![],
        }
    }

    pub fn with_target(mut self, target: WriteTarget) -> Self {
        self.target = Some(target);
        self
    }

    pub fn with_mode(mut self, mode: SinkMode) -> Self {
        self.mode = Some(mode);
        self
    }

    pub fn with_format(mut self, format: String) -> Self {
        self.format = Some(format);
        self
    }

    pub fn with_partition(
        mut self,
        partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
    ) -> Self {
        self.partition = partition;
        self
    }

    pub fn with_partition_by(mut self, partition_by: Vec<spec::Identifier>) -> Self {
        self.partition_by = partition_by;
        self
    }

    pub fn with_bucket_by(mut self, bucket_by: Option<spec::SaveBucketBy>) -> Self {
        self.bucket_by = bucket_by;
        self
    }

    pub fn with_sort_by(mut self, sort_by: Vec<spec::SortOrder>) -> Self {
        self.sort_by = sort_by;
        self
    }

    pub fn with_cluster_by(mut self, cluster_by: Vec<spec::ObjectName>) -> Self {
        self.cluster_by = cluster_by;
        self
    }

    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options = options;
        self
    }

    pub fn with_table_properties(mut self, properties: Vec<(String, String)>) -> Self {
        self.table_properties = properties;
        self
    }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_write(
        &self,
        write: spec::Write,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::{SaveMode, SaveType, TableSaveMethod};

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

        let input = self.resolve_write_input(*input, state).await?;
        let clustering_columns = self.resolve_write_cluster_by_columns(clustering_columns)?;

        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partitioning_columns)
            .with_bucket_by(bucket_by)
            .with_sort_by(sort_columns)
            .with_cluster_by(clustering_columns)
            .with_options(options);
        if let Some(source) = source {
            builder = builder.with_format(source);
        }
        match save_type {
            SaveType::Path(location) => {
                let mode = match mode {
                    Some(SaveMode::ErrorIfExists) | None => SinkMode::ErrorIfExists,
                    Some(SaveMode::IgnoreIfExists) => SinkMode::IgnoreIfExists,
                    Some(SaveMode::Append) => SinkMode::Append,
                    Some(SaveMode::Overwrite) => SinkMode::Overwrite,
                };
                builder = builder
                    .with_target(WriteTarget::Path { location })
                    .with_mode(mode);
            }
            SaveType::Table {
                table,
                save_method: TableSaveMethod::SaveAsTable,
            } => match mode {
                Some(SaveMode::ErrorIfExists) | None => {
                    builder = builder
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Create,
                        })
                        .with_mode(SinkMode::ErrorIfExists);
                }
                Some(SaveMode::IgnoreIfExists) => {
                    builder = builder
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Create,
                        })
                        .with_mode(SinkMode::IgnoreIfExists);
                }
                Some(SaveMode::Append) => {
                    builder = builder.with_mode(SinkMode::Append).with_target(
                        WriteTarget::ExistingTable {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        },
                    );
                }
                Some(SaveMode::Overwrite) => {
                    builder =
                        builder
                            .with_mode(SinkMode::Overwrite)
                            .with_target(WriteTarget::NewTable {
                                table,
                                action: WriteTableAction::CreateOrReplace,
                            });
                }
            },
            SaveType::Table {
                table,
                save_method: TableSaveMethod::InsertInto,
            } => {
                let mode = match mode {
                    Some(SaveMode::Overwrite) => SinkMode::Overwrite,
                    _ => SinkMode::Append,
                };
                builder = builder
                    .with_mode(mode)
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    });
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }

    pub(super) async fn resolve_command_write_to(
        &self,
        write_to: spec::WriteTo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::WriteTo {
            input,
            provider,
            table,
            mode,
            partitioning_columns,
            clustering_columns,
            options,
            table_properties,
        } = write_to;

        let input = self.resolve_write_input(*input, state).await?;
        let partition_by = self.resolve_write_partition_by_expressions(partitioning_columns)?;
        let cluster_by = self.resolve_write_cluster_by_columns(clustering_columns)?;

        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partition_by)
            .with_cluster_by(cluster_by)
            .with_options(options)
            .with_table_properties(table_properties);

        if let Some(provider) = provider {
            builder = builder.with_format(provider);
        }

        match mode {
            WriteToMode::Append => {
                builder =
                    builder
                        .with_mode(SinkMode::Append)
                        .with_target(WriteTarget::ExistingTable {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        });
            }
            WriteToMode::Create => {
                builder =
                    builder
                        .with_mode(SinkMode::Overwrite)
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Create,
                        })
            }
            WriteToMode::CreateOrReplace => {
                builder =
                    builder
                        .with_mode(SinkMode::Overwrite)
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::CreateOrReplace,
                        });
            }
            WriteToMode::Overwrite { condition } => {
                let condition = self
                    .resolve_expression(*condition, input.schema(), state)
                    .await?;
                builder = builder
                    .with_mode(SinkMode::OverwriteIf {
                        condition: Box::new(condition),
                    })
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    });
            }
            WriteToMode::OverwritePartitions => {
                builder = builder
                    .with_mode(SinkMode::OverwritePartitions)
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    });
            }
            WriteToMode::Replace => {
                builder =
                    builder
                        .with_mode(SinkMode::Overwrite)
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Replace,
                        });
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }

    pub(super) fn resolve_write_partition_by_expressions(
        &self,
        partition_by: Vec<spec::Expr>,
    ) -> PlanResult<Vec<spec::Identifier>> {
        // TODO: support functions for partitioning columns
        partition_by
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
                _ => Err(PlanError::invalid(
                    "partitioning column must be a column reference",
                )),
            })
            .collect()
    }

    pub(super) fn resolve_write_cluster_by_columns(
        &self,
        cluster_by: Vec<spec::Identifier>,
    ) -> PlanResult<Vec<spec::ObjectName>> {
        Ok(cluster_by.into_iter().map(spec::ObjectName::bare).collect())
    }

    pub(super) async fn resolve_write_input(
        &self,
        input: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let fields = Self::get_field_names(input.schema(), state)?;
        Ok(rename_logical_plan(input, &fields)?)
    }

    async fn resolve_table_info(&self, table: &spec::ObjectName) -> PlanResult<Option<TableInfo>> {
        let status = match self
            .ctx
            .extension::<CatalogManager>()?
            .get_table(table.parts())
            .await
        {
            Ok(x) => x,
            Err(CatalogError::NotFound(_, _)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match status.kind {
            TableKind::Table {
                catalog: _,
                database: _,
                columns,
                comment: _,
                constraints: _,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                options,
                properties: _,
            } => Ok(Some(TableInfo {
                columns,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                options,
            })),
            _ => Ok(None),
        }
    }

    pub(super) async fn resolve_write_with_builder(
        &self,
        mut input: LogicalPlan,
        builder: WritePlanBuilder,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let WritePlanBuilder {
            mode,
            target,
            format,
            partition,
            partition_by,
            bucket_by,
            sort_by,
            cluster_by,
            options,
            table_properties,
        } = builder;

        let Some(mode) = mode else {
            return Err(PlanError::internal("mode is required for write builder"));
        };
        let Some(target) = target else {
            return Err(PlanError::internal("target is required for write builder"));
        };
        if !partition.is_empty() {
            return Err(PlanError::todo("PARTITION for write"));
        }
        if !cluster_by.is_empty() {
            return Err(PlanError::todo("CLUSTER BY for write"));
        }
        let mut file_write_options = FileWriteOptions {
            path: String::new(),
            mode,
            format: format.unwrap_or_default(),
            partition_by: self.resolve_write_partition_by(partition_by.clone())?,
            sort_by: self
                .resolve_sort_orders(sort_by.clone(), true, input.schema(), state)
                .await?,
            bucket_by: self.resolve_write_bucket_by(bucket_by.clone())?,
            options: vec![options],
        };
        let mut preconditions = vec![];
        match target {
            WriteTarget::Path { location } => {
                if !table_properties.is_empty() {
                    return Err(PlanError::invalid(
                        "table properties are not supported for writing to a path",
                    ));
                }
                if file_write_options.format.is_empty() {
                    file_write_options.format = self.config.default_table_file_format.clone();
                }
                file_write_options.path = location;
            }
            WriteTarget::ExistingTable {
                table,
                column_match,
            } => {
                let Some(info) = self.resolve_table_info(&table).await? else {
                    return Err(PlanError::invalid(format!(
                        "table does not exist: {table:?}"
                    )));
                };
                if matches!(file_write_options.mode, SinkMode::IgnoreIfExists) {
                    return Ok(LogicalPlanBuilder::empty(false).build()?);
                }
                input = self.rewrite_write_input(input, column_match, &info)?;
                if !file_write_options.partition_by.is_empty() {
                    return Err(PlanError::invalid(
                        "cannot specify partitioning when writing to an existing table",
                    ));
                }
                if file_write_options.bucket_by.is_some() || !file_write_options.sort_by.is_empty()
                {
                    return Err(PlanError::invalid(
                        "cannot specify bucketing when writing to an existing table",
                    ));
                }
                if !table_properties.is_empty() {
                    return Err(PlanError::invalid(
                        "cannot specify table properties when writing to an existing table",
                    ));
                }
                if !file_write_options.format.is_empty()
                    && !file_write_options.format.eq_ignore_ascii_case(&info.format)
                {
                    return Err(PlanError::invalid(format!(
                        "the format '{}' does not match the format '{}' for table {table:?}",
                        file_write_options.format, info.format
                    )));
                }
                let Some(location) = info.location else {
                    return Err(PlanError::invalid(format!(
                        "table does not have a location: {table:?}"
                    )));
                };
                file_write_options.partition_by = info.partition_by;
                file_write_options.sort_by = info.sort_by.into_iter().map(|x| x.into()).collect();
                file_write_options.bucket_by = info.bucket_by.map(|x| x.into());
                file_write_options.path = location;
                file_write_options.format = info.format;
                file_write_options.options.insert(0, info.options);
            }
            WriteTarget::NewTable { table, action } => {
                let info = self.resolve_table_info(&table).await?;
                if matches!(file_write_options.mode, SinkMode::IgnoreIfExists) && info.is_some() {
                    return Ok(LogicalPlanBuilder::empty(false).build()?);
                }
                if file_write_options.format.is_empty() {
                    if let Some(format) = info.as_ref().map(|x| &x.format) {
                        file_write_options.format = format.clone();
                    } else {
                        file_write_options.format = self.config.default_table_file_format.clone();
                    }
                }
                if let Some(location) = info.as_ref().and_then(|x| x.location.as_ref()) {
                    file_write_options.path = location.clone();
                } else {
                    file_write_options.path = self.resolve_default_table_location(&table)?;
                }
                let replace = match action {
                    WriteTableAction::Create => false,
                    WriteTableAction::CreateOrReplace => true,
                    WriteTableAction::Replace => {
                        if info.is_none() {
                            return Err(PlanError::invalid(format!(
                                "table does not exist: {table:?}"
                            )));
                        }
                        true
                    }
                };
                let columns = input
                    .schema()
                    .inner()
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
                let partition_by = partition_by.into_iter().map(|x| x.into()).collect();
                let sort_by = self.resolve_catalog_table_sort(sort_by)?;
                let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by)?;
                let command = CatalogCommand::CreateTable {
                    table: table.into(),
                    options: CreateTableOptions {
                        columns,
                        comment: None,
                        constraints: vec![],
                        location: Some(file_write_options.path.clone()),
                        format: file_write_options.format.clone(),
                        partition_by,
                        sort_by,
                        bucket_by,
                        if_not_exists: false,
                        replace,
                        options: file_write_options
                            .options
                            .last()
                            .cloned()
                            .into_iter()
                            .flatten()
                            .collect(),
                        properties: table_properties,
                    },
                };
                preconditions.push(Arc::new(self.resolve_catalog_command(command)?));
            }
        };
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(FileWriteNode::new(Arc::new(input), file_write_options)),
        });
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(WithPreconditionsNode::new(preconditions, Arc::new(plan))),
        }))
    }

    #[expect(clippy::only_used_in_recursion)]
    fn rewrite_write_input(
        &self,
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableInfo,
    ) -> PlanResult<LogicalPlan> {
        // TODO: handle partitioning columns
        // TODO: handle table column default values and generated columns

        let table_schema = Schema::new(info.columns.iter().map(|x| x.field()).collect::<Vec<_>>());
        if input.schema().fields().len() != table_schema.fields().len() {
            return Err(PlanError::invalid(format!(
                "input schema for INSERT has {} fields, but table schema has {} fields",
                input.schema().fields().len(),
                table_schema.fields().len()
            )));
        }
        let plan = match column_match {
            WriteColumnMatch::ByPosition => {
                let expr = input
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(table_schema.fields().iter())
                    .map(|(column, field)| {
                        Ok(col(column)
                            .cast_to(field.data_type(), input.schema())?
                            .alias(field.name()))
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                LogicalPlanBuilder::new(input).project(expr)?.build()?
            }
            WriteColumnMatch::ByName => {
                let expr = table_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let name = field.name();
                        let matches = input
                            .schema()
                            .fields()
                            .iter()
                            .filter(|f| f.name().eq_ignore_ascii_case(name))
                            .map(|f| Ok(col(f.name()).cast_to(field.data_type(), input.schema())?))
                            .collect::<PlanResult<Vec<_>>>()?;
                        if matches.is_empty() {
                            Err(PlanError::invalid(format!(
                                "column not found for INSERT: {name}"
                            )))
                        } else {
                            matches.one().map_err(|_| {
                                PlanError::invalid(format!("ambiguous column: {name}"))
                            })
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                LogicalPlanBuilder::new(input).project(expr)?.build()?
            }
            WriteColumnMatch::ByColumns { columns } => {
                if input.schema().fields().len() != columns.len() {
                    return Err(PlanError::invalid(format!(
                        "input schema for INSERT has {} fields, but {} columns are specified",
                        input.schema().fields().len(),
                        columns.len()
                    )));
                }
                let expr = input
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(columns)
                    .map(|(column, name)| col(column).alias(name))
                    .collect::<Vec<_>>();
                let plan = LogicalPlanBuilder::new(input).project(expr)?.build()?;
                self.rewrite_write_input(plan, WriteColumnMatch::ByName, info)?
            }
        };
        Ok(plan)
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

struct TableInfo {
    columns: Vec<TableColumnStatus>,
    location: Option<String>,
    format: String,
    partition_by: Vec<String>,
    sort_by: Vec<CatalogTableSort>,
    bucket_by: Option<CatalogTableBucketBy>,
    options: Vec<(String, String)>,
}
