use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::DFSchema;
use datafusion_expr::{col, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder};
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{CatalogPartitionField, CreateTableColumnOptions, CreateTableOptions};
use sail_common::spec;
use sail_common_datafusion::catalog::TableHandle;
use sail_common_datafusion::datasource::{BucketBy, SinkMode};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::file_write::{FileWriteNode, FileWriteOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

pub(super) enum WriteMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwriteIf {
        condition: Box<spec::ExprWithSource>,
    },
    OverwritePartitions,
}

pub(super) enum WriteTarget {
    Path {
        location: String,
    },
    Sink,
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
    CreateIfNotExists,
    CreateOrReplace,
    Replace,
}

/// A unified logical plan builder for all write or insert operations.
pub(super) struct WritePlanBuilder {
    target: Option<WriteTarget>,
    mode: Option<WriteMode>,
    format: Option<String>,
    partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
    partition_by: Vec<CatalogPartitionField>,
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

    pub fn with_mode(mut self, mode: WriteMode) -> Self {
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

    pub fn with_partition_by(mut self, partition_by: Vec<CatalogPartitionField>) -> Self {
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
        let input_schema = input.schema().inner().clone();
        let options_map = options
            .clone()
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let mut file_write_options = FileWriteOptions {
            table: None,
            path: String::new(),
            // The mode will be set later so the value here is just a placeholder.
            mode: SinkMode::ErrorIfExists,
            format: format.unwrap_or_default(),
            partition_by: self.resolve_write_partition_by(partition_by.clone())?,
            sort_by: self
                .resolve_sort_orders(sort_by.clone(), true, input.schema(), state)
                .await?,
            bucket_by: self.resolve_write_bucket_by(bucket_by.clone())?,
            table_properties: vec![],
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
                let schema_for_cond =
                    matches!(mode, WriteMode::OverwriteIf { .. }).then_some(input_schema.as_ref());
                file_write_options.mode = self
                    .resolve_write_mode(mode, schema_for_cond, state)
                    .await?;
            }
            WriteTarget::Sink => {
                if !table_properties.is_empty() {
                    return Err(PlanError::invalid(
                        "table properties are not supported for writing to a sink",
                    ));
                }
                if file_write_options.format.is_empty() {
                    file_write_options.format = self.config.default_table_file_format.clone();
                }
                let schema_for_cond =
                    matches!(mode, WriteMode::OverwriteIf { .. }).then_some(input_schema.as_ref());
                file_write_options.mode = self
                    .resolve_write_mode(mode, schema_for_cond, state)
                    .await?;
            }
            WriteTarget::ExistingTable {
                table,
                column_match,
            } => {
                if !table_properties.is_empty() {
                    return Err(PlanError::invalid(
                        "cannot specify table properties when writing to an existing table",
                    ));
                }
                let Some(info) = self.resolve_table_handle(&table).await? else {
                    return Err(PlanError::invalid(format!(
                        "table does not exist: {table:?}"
                    )));
                };
                if matches!(mode, WriteMode::IgnoreIfExists) {
                    return Ok(LogicalPlanBuilder::empty(false).build()?);
                }
                info.validate_write_layout(
                    &file_write_options.partition_by,
                    &file_write_options.bucket_by,
                    &file_write_options.sort_by,
                    &file_write_options.format,
                )
                .map_err(PlanError::invalid)?;
                input = Self::rewrite_write_input(input, column_match, &info)?;
                file_write_options.mode = self
                    .resolve_write_mode(mode, Some(&info.schema()), state)
                    .await?;
                file_write_options.table = Some(info.clone());
                file_write_options.partition_by = info.partition_by().to_vec();
                file_write_options.sort_by =
                    info.sort_by().iter().cloned().map(Into::into).collect();
                file_write_options.bucket_by = info.bucket_by().cloned().map(Into::into);
                file_write_options.path =
                    info.location().map(ToOwned::to_owned).ok_or_else(|| {
                        PlanError::invalid(format!("table does not have a location: {table:?}"))
                    })?;
                file_write_options.format = info.format().to_string();
                file_write_options
                    .options
                    .insert(0, info.options().to_vec());
                file_write_options.table_properties = info.properties().to_vec();
            }
            WriteTarget::NewTable { table, action } => {
                let info = self.resolve_table_handle(&table).await?;
                if matches!(mode, WriteMode::IgnoreIfExists) && info.is_some() {
                    return Ok(LogicalPlanBuilder::empty(false).build()?);
                }
                if matches!(action, WriteTableAction::CreateIfNotExists) {
                    if let Some(ref info) = info {
                        info.validate_write_layout(
                            &file_write_options.partition_by,
                            &file_write_options.bucket_by,
                            &file_write_options.sort_by,
                            &file_write_options.format,
                        )
                        .map_err(PlanError::invalid)?;
                        input = Self::rewrite_write_input(input, WriteColumnMatch::ByName, info)?;
                    }
                }
                file_write_options.mode = self.resolve_write_mode(mode, None, state).await?;
                if file_write_options.format.is_empty() {
                    if let Some(format) = info.as_ref().map(TableHandle::format) {
                        file_write_options.format = format.to_string();
                    } else {
                        file_write_options.format = self.config.default_table_file_format.clone();
                    }
                }
                if let Some(location) = info.as_ref().and_then(TableHandle::location) {
                    file_write_options.path = location.to_string();
                } else if let Some(location) = options_map.get("location") {
                    file_write_options.path = location.to_string();
                } else if let Some(path) = options_map.get("path") {
                    file_write_options.path = path.to_string();
                } else {
                    file_write_options.path = self.resolve_default_table_location(&table).await?;
                }
                file_write_options.table = info.clone();
                file_write_options.table_properties = table_properties.clone();
                let (if_not_exists, replace) = match action {
                    WriteTableAction::Create => (false, false),
                    WriteTableAction::CreateIfNotExists => (true, false),
                    WriteTableAction::CreateOrReplace => (false, true),
                    WriteTableAction::Replace => {
                        if info.is_none() {
                            return Err(PlanError::invalid(format!(
                                "table does not exist: {table:?}"
                            )));
                        }
                        (false, true)
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
                        if_not_exists,
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
            node: Arc::new(BarrierNode::new(preconditions, Arc::new(plan))),
        }))
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

    /// Resolves the write mode against an optional table schema.
    /// If the table schema is not specified, conditional overwrite is not allowed.
    async fn resolve_write_mode(
        &self,
        mode: WriteMode,
        schema: Option<&Schema>,
        state: &mut PlanResolverState,
    ) -> PlanResult<SinkMode> {
        match mode {
            WriteMode::ErrorIfExists => Ok(SinkMode::ErrorIfExists),
            WriteMode::IgnoreIfExists => Ok(SinkMode::IgnoreIfExists),
            WriteMode::Append => Ok(SinkMode::Append),
            WriteMode::Overwrite => Ok(SinkMode::Overwrite),
            WriteMode::OverwriteIf { condition } => {
                let Some(schema) = schema else {
                    return Err(PlanError::internal(
                        "conditional overwrite is not allowed without a table schema",
                    ));
                };
                let names = state.register_fields(schema.fields());
                let schema = rename_schema(schema, &names)?;
                let schema = Arc::new(DFSchema::try_from(schema)?);
                let expr = self
                    .resolve_expression(condition.expr, &schema, state)
                    .await?;
                let expr = self.rewrite_expression_for_external_schema(expr, state)?;
                Ok(SinkMode::OverwriteIf {
                    condition: Box::new(ExprWithSource::new(expr, condition.source)),
                })
            }
            WriteMode::OverwritePartitions => Ok(SinkMode::OverwritePartitions),
        }
    }

    fn rewrite_write_input(
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableHandle,
    ) -> PlanResult<LogicalPlan> {
        // TODO: handle table column default values and generated columns

        let table_schema = info.schema();
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
                Self::rewrite_write_input(plan, WriteColumnMatch::ByName, info)?
            }
        };
        Ok(plan)
    }

    fn resolve_write_partition_by(
        &self,
        partition_by: Vec<CatalogPartitionField>,
    ) -> PlanResult<Vec<String>> {
        Ok(partition_by.into_iter().map(|x| x.column).collect())
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
