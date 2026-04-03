use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::expr::Sort;
use datafusion_expr::{col, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CatalogPartitionField, CreateTableColumnOptions, CreateTableOptions, PartitionTransform,
};
use sail_common::spec;
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableSort, TableColumnStatus, TableKind,
};
use sail_common_datafusion::datasource::{
    find_option, BucketBy, OptionLayer, SinkMode, SourceInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::file_write::{FileWriteNode, FileWriteOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

/// The write modes for all targets.
///
/// The modes are classified based on the action to take when the target exists or
/// does not exist. More modes can be added if additional actions are required.
/// If the target does not exist, the action is usually either "creating the target
/// and writing the data" or "returning an error". If the target exists, the actions
/// are more diverse.
///
/// We avoid using terms such as "overwrite" since it has different semantics
/// in different APIs, so we introduce more specific terms such as "replace"
/// and "truncate" instead.
pub(super) enum WriteMode {
    /// If the target exists, return an error.
    /// If the target does not exist, create the target and write the data.
    ErrorIfExists,
    /// If the target exists, skip the write operation.
    /// If the target does not exist, create the target and write the data.
    IgnoreIfExists,
    /// If the target exists, add the data to the target.
    /// If the target does not exist, return an error if `error_if_absent` is true,
    /// or create the target and write the data if `error_if_absent` is false.
    ///
    /// The data must have compatible schema if the target exists.
    Append { error_if_absent: bool },
    /// If the target exists, remove and recreate the target and write the data.
    /// If the target does not exist, return an error if `error_if_absent` is true,
    /// or create the target and write the data if `error_if_absent` is false.
    ///
    /// The data can have incompatible schema even if the target exists.
    Replace { error_if_absent: bool },
    /// If the target exists, remove all data and write the data.
    /// If the target does not exist, return an error.
    ///
    /// This is different from [`Self::Replace`] since the existing target is not removed and
    /// recreated.
    /// The data must have compatible schema if the target exists.
    Truncate,
    /// If the target exists, remove all data matching the condition and add the data.
    /// If the target does not exist, return an error.
    ///
    /// The data must have compatible schema if the target exists.
    TruncateIf {
        condition: Box<spec::ExprWithSource>,
    },
    /// If the target exists, remove all data from partitions that overlap with the data to write,
    /// and then add the data.
    /// If the target does not exist, return an error.
    ///
    /// The data must have compatible schema if the target exists.
    TruncatePartitions,
}

pub(super) enum WriteTarget {
    DataSource,
    Table {
        table: spec::ObjectName,
        column_match: WriteColumnMatch,
    },
}

#[expect(clippy::enum_variant_names)]
pub(super) enum WriteColumnMatch {
    ByPosition,
    ByName,
    ByColumns { columns: Vec<spec::Identifier> },
}

/// A unified logical plan builder for all write or insert operations.
pub(super) struct WritePlanBuilder {
    target: Option<WriteTarget>,
    mode: Option<WriteMode>,
    format: Option<String>,
    partition_by: Vec<CatalogPartitionField>,
    bucket_by: Option<spec::SaveBucketBy>,
    sort_by: Vec<spec::SortOrder>,
    cluster_by: Vec<spec::ObjectName>,
    options: Vec<Vec<(String, String)>>,
    table_properties: Vec<(String, String)>,
}

impl WritePlanBuilder {
    pub fn new() -> Self {
        Self {
            target: None,
            mode: None,
            format: None,
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
        self.options.push(options);
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
        if !cluster_by.is_empty() {
            return Err(PlanError::todo("CLUSTER BY for write"));
        }
        let input_schema = input.schema().inner().clone();
        let mut file_write_options = FileWriteOptions {
            // The mode will be set later so the value here is just a placeholder.
            mode: SinkMode::ErrorIfExists,
            format: format.unwrap_or_default(),
            partition_by: partition_by.clone(),
            sort_by: self
                .resolve_sort_orders(sort_by.clone(), true, input.schema(), state)
                .await?,
            bucket_by: self.resolve_write_bucket_by(bucket_by.clone())?,
            table_properties: vec![],
            options,
        };
        let mut preconditions = vec![];
        match target {
            WriteTarget::DataSource => {
                if !table_properties.is_empty() {
                    return Err(PlanError::invalid(
                        "table properties are not supported for writing to a data source",
                    ));
                }
                if file_write_options.format.is_empty() {
                    file_write_options.format = self.config.default_table_file_format.clone();
                }
                let schema_for_cond =
                    matches!(mode, WriteMode::TruncateIf { .. }).then_some(input_schema.as_ref());
                file_write_options.mode = self
                    .resolve_write_mode(mode, schema_for_cond, state)
                    .await?;
            }
            WriteTarget::Table {
                table,
                column_match,
            } => {
                let info = self.resolve_table_info(&table).await?;

                // Return early if the target exists and the mode says to skip
                if matches!(mode, WriteMode::IgnoreIfExists) && info.is_some() {
                    return Ok(LogicalPlanBuilder::empty(false).build()?);
                }

                // Error if the mode requires an existing target but it does not exist
                let requires_existing = matches!(
                    mode,
                    WriteMode::Append {
                        error_if_absent: true
                    } | WriteMode::Replace {
                        error_if_absent: true
                    } | WriteMode::Truncate
                        | WriteMode::TruncateIf { .. }
                        | WriteMode::TruncatePartitions
                );
                if requires_existing && info.is_none() {
                    return Err(PlanError::invalid(format!(
                        "table does not exist: {table:?}"
                    )));
                }

                // Compute the schema for conditional truncation before potentially consuming info
                let schema_for_cond = if matches!(mode, WriteMode::TruncateIf { .. }) {
                    info.as_ref().map(|i| i.schema())
                } else {
                    None
                };

                // Use the existing table metadata when the table exists and the mode is
                // "append or truncate" (as opposed to "replace or create").
                let use_existing = matches!(
                    mode,
                    WriteMode::Append { .. }
                        | WriteMode::Truncate
                        | WriteMode::TruncateIf { .. }
                        | WriteMode::TruncatePartitions
                );
                if let Some(info) = info.as_ref().filter(|_| use_existing) {
                    if !table_properties.is_empty() {
                        return Err(PlanError::invalid(
                            "cannot specify table properties when writing to an existing table",
                        ));
                    }
                    info.validate_file_write_options(&file_write_options)?;
                    input = Self::rewrite_write_input(input, column_match, info)?;
                    if file_write_options.partition_by.is_empty()
                        || !info.format.eq_ignore_ascii_case("iceberg")
                    {
                        file_write_options.partition_by = info.partition_by.clone();
                    }
                    file_write_options.sort_by =
                        info.sort_by.iter().cloned().map(|x| x.into()).collect();
                    file_write_options.bucket_by = info.bucket_by.clone().map(|x| x.into());
                    let location = info.location.clone().ok_or_else(|| {
                        PlanError::invalid(format!("table does not have a location: {table:?}"))
                    })?;
                    file_write_options
                        .options
                        .push(vec![("path".to_string(), location)]);
                    file_write_options.format = info.format.clone();
                    file_write_options.options.insert(0, info.options.clone());
                    file_write_options.table_properties = info.properties.clone();
                } else {
                    // Create or replace the table
                    file_write_options.table_properties = table_properties.clone();
                    if file_write_options.format.is_empty() {
                        if let Some(format) = info.as_ref().map(|x| &x.format) {
                            file_write_options.format = format.clone();
                        } else {
                            file_write_options.format =
                                self.config.default_table_file_format.clone();
                        }
                    }
                    if let Some(location) = info.as_ref().and_then(|x| x.location.as_ref()) {
                        file_write_options
                            .options
                            .push(vec![("path".to_string(), location.clone())]);
                    } else {
                        let default_location = self.resolve_default_table_location(&table).await?;
                        file_write_options
                            .options
                            .insert(0, vec![("path".to_string(), default_location)]);
                    };
                    if file_write_options
                        .partition_by
                        .iter()
                        .any(|field| field.transform.is_some())
                        && !file_write_options.format.eq_ignore_ascii_case("iceberg")
                    {
                        return Err(PlanError::unsupported(
                            "partition transforms are only supported for Iceberg tables",
                        ));
                    }
                    let all_options: Vec<std::collections::HashMap<String, String>> =
                        file_write_options
                            .options
                            .iter()
                            .map(|set| set.iter().cloned().collect())
                            .collect();
                    let table_location = find_option(&all_options, "path")
                        .or_else(|| find_option(&all_options, "location"))
                        .unwrap_or_default();
                    let (if_not_exists, replace) = if matches!(mode, WriteMode::Append { .. }) {
                        (true, false)
                    } else if matches!(mode, WriteMode::Replace { .. }) {
                        (false, true)
                    } else {
                        // ErrorIfExists or IgnoreIfExists
                        (false, false)
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
                    // TODO: Revisit passing write options to CreateTableOptions.
                    let create_table_options: Vec<(String, String)> = file_write_options
                        .options
                        .iter()
                        .flatten()
                        .filter(|(k, _)| {
                            !k.eq_ignore_ascii_case("path") && !k.eq_ignore_ascii_case("location")
                        })
                        .cloned()
                        .collect();
                    let sort_by = self.resolve_catalog_table_sort(sort_by)?;
                    let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by)?;
                    let command = CatalogCommand::CreateTable {
                        table: table.into(),
                        options: CreateTableOptions {
                            columns,
                            comment: None,
                            constraints: vec![],
                            location: Some(table_location),
                            format: file_write_options.format.clone(),
                            partition_by,
                            sort_by,
                            bucket_by,
                            if_not_exists,
                            replace,
                            options: create_table_options,
                            properties: table_properties,
                        },
                    };
                    preconditions.push(Arc::new(self.resolve_catalog_command(command)?));
                }

                file_write_options.mode = self
                    .resolve_write_mode(mode, schema_for_cond.as_ref(), state)
                    .await?;
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
            WriteMode::Append { .. } => Ok(SinkMode::Append),
            WriteMode::Replace { .. } | WriteMode::Truncate => Ok(SinkMode::Overwrite),
            WriteMode::TruncateIf { condition } => {
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
            WriteMode::TruncatePartitions => Ok(SinkMode::OverwritePartitions),
        }
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
                mut columns,
                comment: _,
                constraints: _,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                options,
                properties,
            } => {
                // When a table is created without column definitions
                // (e.g. `CREATE TABLE t USING fmt`), the catalog stores an empty column list.
                // Discover the schema from the table format so that write operations
                // (INSERT INTO) can validate the input schema correctly.
                if columns.is_empty() {
                    let registry = self.ctx.extension::<TableFormatRegistry>().map_err(|e| {
                        PlanError::invalid(format!(
                            "failed to access table format registry for table `{table:?}`: {e}",
                        ))
                    })?;
                    let table_format = registry.get(&format).map_err(|e| {
                        PlanError::invalid(format!(
                            "failed to resolve table format `{format}` for table `{table:?}`: {e}",
                        ))
                    })?;
                    let info = SourceInfo {
                        paths: location.iter().cloned().collect(),
                        schema: None,
                        constraints: Default::default(),
                        partition_by: vec![],
                        bucket_by: None,
                        sort_order: vec![],
                        options: vec![OptionLayer::OptionList {
                            items: options.to_vec(),
                        }],
                    };
                    let provider = table_format
                        .create_provider(&self.ctx.state(), info)
                        .await
                        .map_err(|e| {
                            PlanError::invalid(format!(
                                "failed to infer schema for table `{table:?}` from format `{format}`: {e}",
                            ))
                        })?;
                    columns = provider
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| TableColumnStatus {
                            name: f.name().clone(),
                            data_type: f.data_type().clone(),
                            nullable: f.is_nullable(),
                            comment: None,
                            default: None,
                            generated_always_as: None,
                            is_partition: false,
                            is_bucket: false,
                            is_cluster: false,
                        })
                        .collect();
                }
                Ok(Some(TableInfo {
                    columns,
                    location,
                    format,
                    partition_by,
                    sort_by,
                    bucket_by,
                    options,
                    properties,
                }))
            }
            _ => Ok(None),
        }
    }

    fn rewrite_write_input(
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableInfo,
    ) -> PlanResult<LogicalPlan> {
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
                Self::rewrite_write_input(plan, WriteColumnMatch::ByName, info)?
            }
        };
        Ok(plan)
    }

    pub(super) fn resolve_write_partition_by_expressions(
        &self,
        partition_by: Vec<spec::Expr>,
    ) -> PlanResult<Vec<CatalogPartitionField>> {
        partition_by
            .into_iter()
            .map(|x| match x {
                spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                    is_metadata_column: false,
                } => {
                    let name: Vec<String> = name.into();
                    Ok(CatalogPartitionField {
                        column: name.one()?,
                        transform: None,
                    })
                }
                spec::Expr::UnresolvedFunction(f) => resolve_partition_transform_function(f),
                _ => Err(PlanError::invalid(
                    "partitioning column must be a column reference or transform function",
                )),
            })
            .collect()
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

fn resolve_partition_transform_function(
    func: spec::UnresolvedFunction,
) -> PlanResult<CatalogPartitionField> {
    let function_name: Vec<String> = func.function_name.into();
    let function_name = function_name.one()?;
    let function_name_lower = function_name.to_lowercase();

    match function_name_lower.as_str() {
        "years" | "months" | "days" | "hours" => {
            let transform = match function_name_lower.as_str() {
                "years" => PartitionTransform::Year,
                "months" => PartitionTransform::Month,
                "days" => PartitionTransform::Day,
                "hours" => PartitionTransform::Hour,
                _ => unreachable!(),
            };
            let column = extract_partition_column_from_args(&func.arguments, 0)?;
            Ok(CatalogPartitionField {
                column,
                transform: Some(transform),
            })
        }
        "bucket" => {
            let num_buckets = extract_partition_int_arg(&func.arguments, 0, "bucket count")?;
            let column = extract_partition_column_from_args(&func.arguments, 1)?;
            Ok(CatalogPartitionField {
                column,
                transform: Some(PartitionTransform::Bucket(num_buckets)),
            })
        }
        "truncate" => {
            let (column, width) = extract_partition_truncate_args(&func.arguments)?;
            Ok(CatalogPartitionField {
                column,
                transform: Some(PartitionTransform::Truncate(width)),
            })
        }
        _ => Err(PlanError::invalid(format!(
            "unsupported partition transform function: {function_name}"
        ))),
    }
}

fn extract_partition_truncate_args(args: &[spec::Expr]) -> PlanResult<(String, u32)> {
    if let (Ok(column), Ok(width)) = (
        extract_partition_column_from_args(args, 0),
        extract_partition_int_arg(args, 1, "truncate width"),
    ) {
        return Ok((column, width));
    }
    if let (Ok(width), Ok(column)) = (
        extract_partition_int_arg(args, 0, "truncate width"),
        extract_partition_column_from_args(args, 1),
    ) {
        return Ok((column, width));
    }
    Err(PlanError::invalid(
        "truncate() expects a column reference and an integer literal width",
    ))
}

fn extract_partition_column_from_args(args: &[spec::Expr], index: usize) -> PlanResult<String> {
    let arg = args.get(index).ok_or_else(|| {
        PlanError::invalid(format!(
            "partition transform function requires argument at index {index}"
        ))
    })?;
    match arg {
        spec::Expr::UnresolvedAttribute {
            name,
            plan_id: None,
            is_metadata_column: false,
        } => {
            let name: Vec<String> = name.clone().into();
            Ok(name.one()?)
        }
        _ => Err(PlanError::invalid(
            "partition transform function argument must be a column reference",
        )),
    }
}

fn extract_partition_int_arg(
    args: &[spec::Expr],
    index: usize,
    description: &str,
) -> PlanResult<u32> {
    let arg = args.get(index).ok_or_else(|| {
        PlanError::invalid(format!(
            "partition transform function requires {description} at index {index}"
        ))
    })?;
    match arg {
        spec::Expr::Literal(lit) => match lit {
            spec::Literal::Int8 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                PlanError::invalid(format!("{description} must be a positive integer"))
            }),
            spec::Literal::Int16 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                PlanError::invalid(format!("{description} must be a positive integer"))
            }),
            spec::Literal::Int32 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                PlanError::invalid(format!("{description} must be a positive integer"))
            }),
            spec::Literal::Int64 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                PlanError::invalid(format!("{description} must be a positive integer"))
            }),
            spec::Literal::UInt8 { value: Some(v) } => Ok(u32::from(*v)),
            spec::Literal::UInt16 { value: Some(v) } => Ok(u32::from(*v)),
            spec::Literal::UInt32 { value: Some(v) } => Ok(*v),
            spec::Literal::UInt64 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                PlanError::invalid(format!("{description} must fit in a 32-bit integer"))
            }),
            _ => Err(PlanError::invalid(format!(
                "{description} must be an integer literal"
            ))),
        },
        _ => Err(PlanError::invalid(format!(
            "{description} must be an integer literal"
        ))),
    }
}

struct TableInfo {
    columns: Vec<TableColumnStatus>,
    location: Option<String>,
    format: String,
    partition_by: Vec<CatalogPartitionField>,
    sort_by: Vec<CatalogTableSort>,
    bucket_by: Option<CatalogTableBucketBy>,
    options: Vec<(String, String)>,
    properties: Vec<(String, String)>,
}

impl TableInfo {
    fn schema(&self) -> Schema {
        let fields = self
            .columns
            .iter()
            .map(|col| col.field())
            .collect::<Vec<_>>();
        Schema::new(fields)
    }

    fn validate_file_write_options(&self, options: &FileWriteOptions) -> PlanResult<()> {
        if !self.format.eq_ignore_ascii_case("iceberg")
            && !self.is_empty_or_equivalent_partitioning(&options.partition_by)
        {
            return Err(PlanError::invalid(
                "cannot specify a different partitioning when writing to an existing table",
            ));
        }
        if !self.is_empty_or_equivalent_bucketing(&options.bucket_by, &options.sort_by) {
            return Err(PlanError::invalid(
                "cannot specify a different bucketing when writing to an existing table",
            ));
        }
        if !options.format.is_empty() && !options.format.eq_ignore_ascii_case(&self.format) {
            return Err(PlanError::invalid(format!(
                "the format '{}' does not match the table format '{}'",
                options.format, self.format
            )));
        }
        Ok(())
    }

    fn is_empty_or_equivalent_partitioning(&self, partition_by: &[CatalogPartitionField]) -> bool {
        partition_by.is_empty()
            || (partition_by.len() == self.partition_by.len()
                && partition_by
                    .iter()
                    .zip(self.partition_by.iter())
                    .all(|(a, b)| Self::partition_fields_match(a, b)))
    }

    fn partition_fields_match(a: &CatalogPartitionField, b: &CatalogPartitionField) -> bool {
        fn normalize_transform(
            transform: Option<PartitionTransform>,
        ) -> Option<PartitionTransform> {
            match transform {
                None | Some(PartitionTransform::Identity) => None,
                Some(transform) => Some(transform),
            }
        }

        a.column.eq_ignore_ascii_case(&b.column)
            && normalize_transform(a.transform) == normalize_transform(b.transform)
    }

    fn is_empty_or_equivalent_bucketing(
        &self,
        bucket_by: &Option<BucketBy>,
        sort_by: &[Sort],
    ) -> bool {
        let bucket_by_match = match (bucket_by, &self.bucket_by) {
            (None, _) => true,
            (Some(b1), Some(b2)) => {
                b1.num_buckets == b2.num_buckets
                    && b1.columns.len() == b2.columns.len()
                    && b1
                        .columns
                        .iter()
                        .zip(&b2.columns)
                        .all(|(a, b)| a.eq_ignore_ascii_case(b))
            }
            (Some(_), None) => false,
        };
        let sort_by_match = match (sort_by, self.sort_by.as_slice()) {
            ([], _) => true,
            (_, []) => false,
            (s1, s2) => {
                s1.len() == s2.len()
                    && s1.iter().zip(s2.iter()).all(|(a, b)| {
                        let Sort {
                            expr:
                                Expr::Column(Column {
                                    relation: _,
                                    name,
                                    spans: _,
                                }),
                            asc,
                            nulls_first: _,
                        } = a
                        else {
                            return false;
                        };
                        name.eq_ignore_ascii_case(&b.column) && *asc == b.ascending
                    })
            }
        };
        bucket_by_match && sort_by_match
    }
}
