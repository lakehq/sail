use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::expr::{FieldMetadata, Sort};
use datafusion_expr::{
    col, lit, when, BinaryExpr, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder,
    Operator, ScalarUDF,
};
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
use sail_common_datafusion::column_features::{ColumnFeatures, ColumnFeaturesBuilder};
use sail_common_datafusion::datasource::{
    find_option, BucketBy, OptionLayer, SinkMode, SourceInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::raise_error::RaiseError;
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
    /// Creates an empty write plan builder.
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

    /// Sets the write target for the builder.
    pub fn with_target(mut self, target: WriteTarget) -> Self {
        self.target = Some(target);
        self
    }

    /// Sets the write mode for the builder.
    pub fn with_mode(mut self, mode: WriteMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Sets the output format for the builder.
    pub fn with_format(mut self, format: String) -> Self {
        self.format = Some(format);
        self
    }

    /// Sets the partitioning fields for the builder.
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

    /// Appends a set of write options to the builder.
    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options.push(options);
        self
    }

    /// Sets the table properties to apply when creating the target table.
    pub fn with_table_properties(mut self, properties: Vec<(String, String)>) -> Self {
        self.table_properties = properties;
        self
    }
}

impl PlanResolver<'_> {
    /// Builds a write logical plan and any catalog preconditions needed before executing it.
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
                    input = self
                        .rewrite_write_input(input, column_match, info, state)
                        .await?;
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
                        .map(|f| {
                            // Read the Delta generation expression from Arrow field metadata.
                            // `ColumnFeatures` transparently JSON-unwraps the value if the
                            // table was created externally and the expression was stored as
                            // a JSON-encoded string.
                            let generated_always_as =
                                ColumnFeatures::from_field(f).generation_expression();
                            TableColumnStatus {
                                name: f.name().clone(),
                                data_type: f.data_type().clone(),
                                nullable: f.is_nullable(),
                                comment: None,
                                default: None,
                                generated_always_as,
                                is_partition: false,
                                is_bucket: false,
                                is_cluster: false,
                            }
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

    async fn rewrite_write_input(
        &self,
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableInfo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let has_generated = info.columns.iter().any(|c| c.generated_always_as.is_some());
        if has_generated {
            self.rewrite_write_input_with_column_expressions(input, column_match, info, state)
                .await
        } else {
            Self::rewrite_standard_write_input(input, column_match, info)
        }
    }

    /// Fast path for tables without column-level expressions (no generated columns,
    /// no defaults, no identity). Pure schema coercion with alias/cast.
    fn rewrite_standard_write_input(
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableInfo,
    ) -> PlanResult<LogicalPlan> {
        let table_schema = Schema::new(info.columns.iter().map(|x| x.field()).collect::<Vec<_>>());
        if input.schema().fields().len() != table_schema.fields().len() {
            return Err(PlanError::invalid(format!(
                "input schema for INSERT has {} fields, but table schema has {} fields",
                input.schema().fields().len(),
                table_schema.fields().len()
            )));
        }
        match column_match {
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
                Ok(LogicalPlanBuilder::new(input).project(expr)?.build()?)
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
                Ok(LogicalPlanBuilder::new(input).project(expr)?.build()?)
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
                Self::rewrite_standard_write_input(plan, WriteColumnMatch::ByName, info)
            }
        }
    }

    /// Rewrite the write input when the target table has column-level expressions
    /// (currently: generated columns). This is the unified path that governs:
    ///
    /// - Column matching: `ByPosition` / `ByName` / `ByColumns` are all normalized
    ///   to a per-table-column provenance map of `input_provided | generated_only`.
    /// - Expression resolution: generation expressions are parsed, analyzed, and
    ///   resolved against an intermediate projection whose fields carry the
    ///   non-generated input values (keyed by registered field IDs).
    /// - Protocol enforcement: when the user explicitly provides a value for a
    ///   generated column, the Delta protocol requires
    ///   `value IS NULL OR value <=> expr IS TRUE`. We enforce this with
    ///   `CASE WHEN cond THEN gen_expr ELSE raise_error(msg) END`, so mismatches
    ///   fail at runtime instead of being silently overwritten.
    /// - Metadata propagation: `delta.generationExpression` is attached via
    ///   `Alias::with_metadata` so the final arrow schema reaching the writer
    ///   carries it — this is the canonical carrier for column-level metadata.
    ///
    /// This helper is structured so that identity columns and default columns
    /// can be introduced here using the same column-expression plumbing.
    async fn rewrite_write_input_with_column_expressions(
        &self,
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableInfo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let table_field_count = info.columns.len();
        let generated_count = info
            .columns
            .iter()
            .filter(|c| c.generated_always_as.is_some())
            .count();
        let non_generated_count = table_field_count - generated_count;
        let input_field_count = input.schema().fields().len();

        // Determine, for each table column, whether the user provided a value (`Some(expr)`
        // where `expr` is taken from the input plan) or not (`None`, generated cols only).
        // This produces a deterministic mapping regardless of the column-match strategy.
        let provided_by_input = Self::classify_input_to_table_columns(
            &input,
            &column_match,
            info,
            input_field_count,
            table_field_count,
            non_generated_count,
        )?;

        // Register field IDs for each table column. Non-generated cols get their input
        // aliased to this field ID in the intermediate plan; generated cols are computed
        // against the intermediate plan in the final projection.
        let field_ids: Vec<String> = info
            .columns
            .iter()
            .map(|c| state.register_field_name(c.name.clone()))
            .collect();

        // Field IDs for user-provided values of generated columns — only used when the
        // user explicitly supplied a value, so the final projection can reach the value
        // via the intermediate plan to enforce the CHECK.
        let mut gen_check_field_ids: Vec<Option<String>> = vec![None; table_field_count];

        // Build intermediate plan: one alias expression per non-generated table column,
        // plus one extra alias per user-provided generated-column value.
        // Cast each expression to the target column type so that generation expressions
        // are resolved against correctly-typed values (e.g. INT -> BIGINT, string ->
        // timestamp) and the mismatch check compares type-compatible values.
        let mut intermediate_aliases: Vec<Expr> = Vec::new();
        for (idx, provided) in provided_by_input.iter().enumerate() {
            let col = &info.columns[idx];
            if col.generated_always_as.is_some() {
                if let Some(user_expr) = provided {
                    let check_id = state.register_hidden_field_name(format!("{}__user", col.name));
                    let cast_expr = user_expr
                        .clone()
                        .cast_to(col.field().data_type(), input.schema())?;
                    intermediate_aliases.push(cast_expr.alias(check_id.clone()));
                    gen_check_field_ids[idx] = Some(check_id);
                }
                continue;
            }
            let Some(input_expr) = provided else {
                return Err(PlanError::invalid(format!(
                    "INSERT is missing value for non-generated column `{}`",
                    col.name
                )));
            };
            let cast_expr = input_expr
                .clone()
                .cast_to(col.field().data_type(), input.schema())?;
            intermediate_aliases.push(cast_expr.alias(field_ids[idx].clone()));
        }
        let intermediate = LogicalPlanBuilder::new(input)
            .project(intermediate_aliases)?
            .build()?;
        let intermediate_schema = intermediate.schema().clone();

        // Resolve each generation expression against the intermediate schema.
        let mut gen_exprs: HashMap<String, Expr> = HashMap::new();
        for col in &info.columns {
            let Some(gen_expr_str) = col.generated_always_as.as_deref() else {
                continue;
            };
            let ast_expr =
                sail_sql_analyzer::parser::parse_expression(gen_expr_str).map_err(|e| {
                    PlanError::invalid(format!(
                        "failed to parse generation expression `{gen_expr_str}`: {e}"
                    ))
                })?;
            let spec_expr =
                sail_sql_analyzer::expression::from_ast_expression(ast_expr).map_err(|e| {
                    PlanError::invalid(format!(
                        "failed to analyze generation expression `{gen_expr_str}`: {e}"
                    ))
                })?;
            let resolved = self
                .resolve_expression(spec_expr, &intermediate_schema, state)
                .await?;
            gen_exprs.insert(col.name.clone(), resolved);
        }

        // Build the final projection (one expression per table column, in table order).
        // The output alias is the human-readable column name so the arrow schema reaching
        // the writer matches the catalog's table schema by name.
        let mut final_exprs: Vec<Expr> = Vec::with_capacity(info.columns.len());
        for (idx, table_col) in info.columns.iter().enumerate() {
            let out_name = table_col.name.clone();
            let field = table_col.field();
            let expr = if let Some(gen_expr_str) = table_col.generated_always_as.as_deref() {
                let gen_expr = gen_exprs.get(&table_col.name).cloned().ok_or_else(|| {
                    PlanError::internal(format!(
                        "expected resolved generation expression for `{}`",
                        table_col.name
                    ))
                })?;
                let final_expr = if let Some(check_id) = &gen_check_field_ids[idx] {
                    // User explicitly provided a value. Enforce Delta protocol:
                    //     value IS NULL OR value <=> generation_expression IS TRUE
                    // Use `<=>` (null-safe equal) so NULL on either side is handled
                    // without spurious "IS TRUE" wrapping.
                    // Cast both sides to the target column type so that type-compatible
                    // values (e.g. INT 2024 vs BIGINT 2024) are not treated as mismatches.
                    let user_value = col(Column::from_name(check_id));
                    let user_value_cast = user_value
                        .clone()
                        .cast_to(field.data_type(), &intermediate_schema)?;
                    let gen_expr_cast = gen_expr
                        .clone()
                        .cast_to(field.data_type(), &intermediate_schema)?;
                    let check = user_value
                        .clone()
                        .is_null()
                        .or(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(user_value_cast),
                            Operator::IsNotDistinctFrom,
                            Box::new(gen_expr_cast),
                        )));
                    let err_msg = format!(
                        "[DELTA_GENERATED_COLUMNS_VALUE_MISMATCH] \
                         CHECK constraint for generated column `{}` \
                         (expression: {}) violated: user-provided value does not match.",
                        table_col.name, gen_expr_str
                    );
                    let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(err_msg)]);
                    when(check, gen_expr).otherwise(raise)?
                } else {
                    gen_expr
                };
                final_expr.cast_to(field.data_type(), &intermediate_schema)?
            } else {
                // Non-generated column: reference the aliased field in intermediate plan.
                col(Column::from_name(&field_ids[idx]))
                    .cast_to(field.data_type(), &intermediate_schema)?
            };
            let gen_meta = table_col
                .generated_always_as
                .as_deref()
                .map(Self::make_gen_field_metadata);
            let alias = if let Some(meta) = gen_meta {
                expr.alias_with_metadata(out_name, Some(meta))
            } else {
                expr.alias(out_name)
            };
            final_exprs.push(alias);
        }
        Ok(LogicalPlanBuilder::new(intermediate)
            .project(final_exprs)?
            .build()?)
    }

    /// Build an `Expr` for each table column that references a user-provided input
    /// value (if any). `None` indicates the user did not supply a value for that
    /// table column (only valid for generated columns).
    ///
    /// Rules per `column_match`:
    ///
    /// - `ByPosition`:
    ///   - If input has `table_field_count` fields: column `i` maps to input `i`.
    ///   - If input has `non_generated_count` fields: the input lines up with the
    ///     non-generated table columns in table order; generated columns get `None`.
    ///   - Otherwise: error.
    /// - `ByName`: each table column looks up (case-insensitive) in the input schema.
    ///   Columns not found map to `None` (allowed only for generated cols).
    /// - `ByColumns { columns }`: `columns` enumerates the table columns being
    ///   provided. Input field `i` binds to the table column whose name matches
    ///   `columns[i]` (case-insensitive). Anything not listed maps to `None`.
    fn classify_input_to_table_columns(
        input: &LogicalPlan,
        column_match: &WriteColumnMatch,
        info: &TableInfo,
        input_field_count: usize,
        table_field_count: usize,
        non_generated_count: usize,
    ) -> PlanResult<Vec<Option<Expr>>> {
        let input_cols = input.schema().columns();
        let mut out: Vec<Option<Expr>> = vec![None; table_field_count];
        match column_match {
            WriteColumnMatch::ByPosition => {
                if input_field_count == table_field_count {
                    for (i, input_col) in input_cols.iter().enumerate() {
                        out[i] = Some(col(input_col.clone()));
                    }
                } else if input_field_count == non_generated_count {
                    let mut non_gen_idx = 0usize;
                    for (i, table_col) in info.columns.iter().enumerate() {
                        if table_col.generated_always_as.is_some() {
                            continue;
                        }
                        out[i] = Some(col(input_cols[non_gen_idx].clone()));
                        non_gen_idx += 1;
                    }
                } else {
                    return Err(PlanError::invalid(format!(
                        "input schema for INSERT has {input_field_count} fields, but table schema has {table_field_count} fields (with {} generated)",
                        table_field_count - non_generated_count
                    )));
                }
            }
            WriteColumnMatch::ByName => {
                for (i, table_col) in info.columns.iter().enumerate() {
                    let mut matches = input
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| f.name().eq_ignore_ascii_case(&table_col.name));
                    let first = matches.next();
                    if matches.next().is_some() {
                        return Err(PlanError::invalid(format!(
                            "ambiguous column for INSERT by name: {}",
                            table_col.name
                        )));
                    }
                    if let Some(f) = first {
                        out[i] = Some(col(Column::from_name(f.name())));
                    } else if table_col.generated_always_as.is_none() {
                        return Err(PlanError::invalid(format!(
                            "column not found for INSERT by name: {}",
                            table_col.name
                        )));
                    }
                }
            }
            WriteColumnMatch::ByColumns { columns } => {
                if columns.len() != input_field_count {
                    return Err(PlanError::invalid(format!(
                        "input schema for INSERT has {input_field_count} fields, but {} columns are specified",
                        columns.len()
                    )));
                }
                let name_to_pos: HashMap<String, usize> = info
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.name.to_lowercase(), i))
                    .collect();
                for (input_col, user_col) in input_cols.iter().zip(columns.iter()) {
                    let key = user_col.as_ref().to_lowercase();
                    let Some(&pos) = name_to_pos.get(&key) else {
                        return Err(PlanError::invalid(format!(
                            "column not found in target table: {}",
                            user_col.as_ref()
                        )));
                    };
                    if out[pos].is_some() {
                        return Err(PlanError::invalid(format!(
                            "column `{}` specified more than once in INSERT column list",
                            user_col.as_ref()
                        )));
                    }
                    out[pos] = Some(col(input_col.clone()));
                }
            }
        }
        Ok(out)
    }

    /// Build arrow field metadata carrying the Delta generation expression.
    fn make_gen_field_metadata(gen_expr: &str) -> FieldMetadata {
        FieldMetadata::from(
            ColumnFeaturesBuilder::new()
                .with_generation_expression(gen_expr)
                .build(),
        )
    }

    pub(super) fn resolve_partition_by_expression(
        &self,
        expr: spec::Expr,
    ) -> PlanResult<CatalogPartitionField> {
        match expr {
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
        }
    }

    /// Resolves partition expressions into catalog partition fields for a write.
    pub(super) fn resolve_write_partition_by_expressions(
        &self,
        partition_by: Vec<spec::Expr>,
    ) -> PlanResult<Vec<CatalogPartitionField>> {
        partition_by
            .into_iter()
            .map(|x| self.resolve_partition_by_expression(x))
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
