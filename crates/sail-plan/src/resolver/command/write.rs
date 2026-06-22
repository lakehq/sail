use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::expr::{self, FieldMetadata, Sort, WindowFunctionParams};
use datafusion_expr::{
    col, lit, when, BinaryExpr, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder,
    Operator, Projection, ScalarUDF, WindowFrame, WindowFunctionDefinition,
};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::LakehouseCreateRequest;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CatalogPartitionField, CreateTableColumnOptions, CreateTableOptions, PartitionTransform,
};
use sail_common::spec::{self, DEFAULT_COLUMN_VALUE_PLACEHOLDER_ID};
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableColumnIdentity, CatalogTableSort, LakehouseExecutionContext,
    LakehouseOperation, TableColumnStatus, TableKind,
};
use sail_common_datafusion::column_features::{
    ColumnFeatures, ColumnFeaturesBuilder, SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY,
};
use sail_common_datafusion::datasource::{
    find_path_in_options, BucketBy, OptionLayer, SinkInfo, SinkMode, SourceInfo,
    TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_logical_plan::barrier::BarrierNode;

use super::delta::parse_delta_generation_expr;
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
    catalog_sort_by: Vec<CatalogTableSort>,
    cluster_by: Vec<spec::ObjectName>,
    options: Vec<Vec<(String, String)>>,
    table_properties: Vec<(String, String)>,
    table_is_external: bool,
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
            catalog_sort_by: vec![],
            cluster_by: vec![],
            options: vec![],
            table_properties: vec![],
            table_is_external: false,
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

    pub fn with_catalog_sort_by(mut self, catalog_sort_by: Vec<CatalogTableSort>) -> Self {
        self.catalog_sort_by = catalog_sort_by;
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

    pub fn with_table_is_external(mut self, is_external: bool) -> Self {
        self.table_is_external = is_external;
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
            catalog_sort_by,
            cluster_by,
            options,
            table_properties,
            table_is_external,
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
        let mut write_format = format.unwrap_or_default();
        let mut sink_info = SinkInfo {
            input: input.clone(),
            // The mode will be set later so the value here is just a placeholder.
            mode: SinkMode::ErrorIfExists,
            partition_by: partition_by.clone(),
            sort_order: self
                .resolve_sort_orders(sort_by.clone(), true, input.schema(), state)
                .await?,
            bucket_by: self.resolve_write_bucket_by(bucket_by.clone())?,
            options: options
                .into_iter()
                .map(|items| OptionLayer::OptionList { items })
                .collect(),
            lakehouse_table: None,
        };
        let mut preconditions = vec![];
        match target {
            WriteTarget::DataSource => {
                if !table_properties.is_empty() {
                    return Err(PlanError::invalid(
                        "table properties are not supported for writing to a data source",
                    ));
                }
                if write_format.is_empty() {
                    write_format = self.config.default_table_file_format.clone();
                }
                let should_rewrite_existing_delta_table_features =
                    !(matches!(&mode, WriteMode::ErrorIfExists | WriteMode::IgnoreIfExists)
                        || matches!(&mode, WriteMode::Replace { .. })
                            && Self::has_truthy_option(
                                &sink_info.options,
                                &["overwriteSchema", "overwrite_schema"],
                            ));
                if should_rewrite_existing_delta_table_features {
                    input = self
                        .rewrite_data_source_delta_table_features(
                            input,
                            &write_format,
                            &sink_info,
                            state,
                        )
                        .await?;
                }
                let schema_for_cond =
                    matches!(mode, WriteMode::TruncateIf { .. }).then_some(input_schema.as_ref());
                sink_info.mode = self
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
                    info.validate_write_info(&write_format, &sink_info)?;
                    let table_format_merge_schema = (info.format.eq_ignore_ascii_case("delta")
                        || info.format.eq_ignore_ascii_case("iceberg"))
                        && Self::has_truthy_option(
                            &sink_info.options,
                            &["mergeSchema", "merge_schema"],
                        );
                    if !table_format_merge_schema {
                        input = self
                            .rewrite_write_input(input, column_match, info, state)
                            .await?;
                    }
                    input = self
                        .apply_delta_table_constraints(input, info, state)
                        .await?;
                    if sink_info.partition_by.is_empty()
                        || !info.format.eq_ignore_ascii_case("iceberg")
                    {
                        sink_info.partition_by = info.partition_by.clone();
                    }
                    sink_info.sort_order = info.sort_by.iter().cloned().map(|x| x.into()).collect();
                    sink_info.bucket_by = info.bucket_by.clone().map(|x| x.into());
                    let location = info.location.clone().ok_or_else(|| {
                        PlanError::invalid(format!("table does not have a location: {table:?}"))
                    })?;
                    sink_info.options.push(OptionLayer::OptionList {
                        items: vec![("path".to_string(), location)],
                    });
                    write_format = info.format.clone();
                    sink_info.options.insert(
                        0,
                        OptionLayer::TablePropertyList {
                            items: info.properties.clone(),
                        },
                    );
                } else {
                    let write_options_had_location =
                        find_path_in_options(&sink_info.options).is_some();
                    sink_info.options.insert(
                        0,
                        OptionLayer::TablePropertyList {
                            items: table_properties,
                        },
                    );
                    // Create or replace the table
                    if write_format.is_empty() {
                        if let Some(format) = info.as_ref().map(|x| &x.format) {
                            write_format = format.clone();
                        } else {
                            write_format = self.config.default_table_file_format.clone();
                        }
                    }
                    if let Some(location) = info.as_ref().and_then(|x| x.location.as_ref()) {
                        sink_info.options.push(OptionLayer::OptionList {
                            items: vec![("path".to_string(), location.clone())],
                        });
                    } else {
                        let default_location = self.resolve_default_table_location(&table).await?;
                        sink_info.options.insert(
                            0,
                            OptionLayer::OptionList {
                                items: vec![("path".to_string(), default_location)],
                            },
                        );
                    };
                    if sink_info
                        .partition_by
                        .iter()
                        .any(|field| field.transform.is_some())
                        && !write_format.eq_ignore_ascii_case("iceberg")
                    {
                        return Err(PlanError::unsupported(
                            "partition transforms are only supported for Iceberg tables",
                        ));
                    }
                    let table_location = find_path_in_options(&sink_info.options);
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
                            identity: None,
                        })
                        .collect();
                    // TODO: Revisit passing write options as table properties.
                    let properties: Vec<(String, String)> = sink_info
                        .options
                        .clone()
                        .into_iter()
                        .flat_map(|layer| match layer {
                            OptionLayer::OptionList { items } => items
                                .into_iter()
                                .filter_map(|(k, v)| {
                                    if !k.eq_ignore_ascii_case("path")
                                        && !k.eq_ignore_ascii_case("location")
                                    {
                                        Some((format!("option.{k}"), v))
                                    } else {
                                        None
                                    }
                                })
                                .collect(),
                            OptionLayer::TablePropertyList { items } => items,
                            _ => vec![],
                        })
                        .collect();
                    let sort_by = if !catalog_sort_by.is_empty() {
                        // Ensure the write produces globally sorted output matching the
                        // declared file sort order, so SortExec elimination is safe.
                        if file_write_options.sort_by.is_empty() {
                            file_write_options.sort_by = catalog_sort_by
                                .iter()
                                .cloned()
                                .map(datafusion_expr::expr::Sort::from)
                                .collect();
                        }
                        catalog_sort_by
                    } else {
                        self.resolve_catalog_table_sort(sort_by)?
                    };
                    let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by)?;
                    let catalog_table = table
                        .parts()
                        .iter()
                        .map(|part| part.as_ref().to_string())
                        .collect::<Vec<_>>();
                    let create_options = CreateTableOptions {
                        columns,
                        comment: None,
                        constraints: vec![],
                        location: table_location,
                        format: write_format.clone(),
                        partition_by,
                        sort_by,
                        bucket_by,
                        if_not_exists,
                        replace,
                        properties,
                        is_external: table_is_external || write_options_had_location,
                        is_write_precondition: true,
                    };
                    let create_plan = self
                        .ctx
                        .extension::<CatalogManager>()?
                        .plan_lakehouse_create(
                            &catalog_table,
                            LakehouseCreateRequest {
                                catalog_table: catalog_table.clone(),
                                options: create_options.clone(),
                            },
                        )
                        .await?;
                    sink_info.lakehouse_table = Some(
                        create_plan
                            .table
                            .execution
                            .for_operation(LakehouseOperation::Write),
                    );
                    let command = CatalogCommand::CreateTable {
                        table: table.clone().into(),
                        options: create_options,
                    };
                    preconditions.push(Arc::new(self.resolve_catalog_command(command)?));
                }

                if sink_info.lakehouse_table.is_none() {
                    sink_info.lakehouse_table = info
                        .as_ref()
                        .and_then(|info| info.lakehouse_table.as_ref())
                        .map(|context| context.for_operation(LakehouseOperation::Write));
                }

                sink_info.mode = self
                    .resolve_write_mode(mode, schema_for_cond.as_ref(), state)
                    .await?;
            }
        };
        input = self
            .rewrite_delta_check_constraints_from_options(input, &write_format, &sink_info, state)
            .await?;
        sink_info.input = input;
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let plan = registry
            .get(&write_format)?
            .create_writer(&self.ctx.state(), sink_info)
            .await?;
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
                mut properties,
                is_external: _,
            } => {
                let catalog_table = table
                    .parts()
                    .iter()
                    .map(|part| part.as_ref().to_string())
                    .collect::<Vec<_>>();
                let lakehouse_table = self
                    .resolve_lakehouse_table_context(
                        &catalog_table,
                        LakehouseOperation::Read,
                        Some(&format),
                        vec![],
                    )
                    .await?;
                let write_precondition_lakehouse_table =
                    Some(lakehouse_table.for_operation(LakehouseOperation::WritePrecondition));
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
                        lakehouse_table: write_precondition_lakehouse_table.clone(),
                        schema: None,
                        constraints: Default::default(),
                        partition_by: vec![],
                        bucket_by: None,
                        sort_order: vec![],
                        options: vec![OptionLayer::TablePropertyList {
                            items: properties.to_vec(),
                        }],
                        read_case_sensitive: self.config.case_sensitive,
                    };
                    let metadata = table_format
                        .infer_metadata(&self.ctx.state(), info)
                        .await
                        .map_err(|e| {
                            PlanError::invalid(format!(
                                "failed to infer metadata for table `{table:?}` from format `{format}`: {e}",
                            ))
                        })?;
                    columns = Self::table_columns_from_format_schema(metadata.schema.as_ref());
                    if !metadata.properties.is_empty() {
                        let mut merged_properties = metadata.properties;
                        merged_properties.extend(properties);
                        properties = merged_properties;
                    }
                } else if format.eq_ignore_ascii_case("delta") && location.is_some() {
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
                        lakehouse_table: write_precondition_lakehouse_table,
                        schema: None,
                        constraints: Default::default(),
                        partition_by: vec![],
                        bucket_by: None,
                        sort_order: vec![],
                        options: vec![OptionLayer::TablePropertyList {
                            items: properties.to_vec(),
                        }],
                        read_case_sensitive: self.config.case_sensitive,
                    };
                    match table_format.infer_metadata(&self.ctx.state(), info).await {
                        Ok(metadata) => {
                            Self::merge_format_columns(&mut columns, metadata.schema.as_ref());
                            if !metadata.properties.is_empty() {
                                let mut merged_properties = metadata.properties;
                                merged_properties.extend(properties);
                                properties = merged_properties;
                            }
                        }
                        Err(e) => {
                            log::debug!(
                                "using catalog metadata for Delta table `{table:?}` because the Delta log metadata could not be loaded yet: {e}"
                            );
                        }
                    }
                }
                Ok(Some(TableInfo {
                    lakehouse_table: Some(lakehouse_table),
                    columns,
                    location,
                    format,
                    partition_by,
                    sort_by,
                    bucket_by,
                    properties,
                }))
            }
            _ => Ok(None),
        }
    }

    fn table_columns_from_format_schema(schema: &Schema) -> Vec<TableColumnStatus> {
        schema
            .fields()
            .iter()
            .map(|field| Self::table_column_from_format_field(field.as_ref()))
            .collect()
    }

    fn table_column_from_format_field(field: &Field) -> TableColumnStatus {
        // Read Delta column features from Arrow field metadata. `ColumnFeatures`
        // transparently JSON-unwraps generation expressions when external
        // writers store them as JSON-encoded strings.
        let features = ColumnFeatures::from_field(field);
        TableColumnStatus {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            comment: None,
            default: features.current_default(),
            generated_always_as: features.generation_expression(),
            identity: features.identity(),
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        }
    }

    fn merge_format_columns(columns: &mut Vec<TableColumnStatus>, schema: &Schema) {
        let fields_by_name = schema
            .fields()
            .iter()
            .map(|field| (field.name().to_lowercase(), field.as_ref()))
            .collect::<HashMap<_, _>>();
        let existing_names = columns
            .iter()
            .map(|column| column.name.to_lowercase())
            .collect::<HashSet<_>>();

        for column in columns.iter_mut() {
            let Some(field) = fields_by_name.get(&column.name.to_lowercase()) else {
                continue;
            };
            let features = ColumnFeatures::from_field(field);
            column.data_type = field.data_type().clone();
            column.nullable = field.is_nullable();
            column.default = features.current_default();
            column.generated_always_as = features.generation_expression();
            column.identity = features.identity();
        }

        columns.extend(
            schema
                .fields()
                .iter()
                .filter(|field| !existing_names.contains(&field.name().to_lowercase()))
                .map(|field| Self::table_column_from_format_field(field.as_ref())),
        );
    }

    pub(super) async fn rewrite_write_input(
        &self,
        input: LogicalPlan,
        column_match: WriteColumnMatch,
        info: &TableInfo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let has_column_expressions = info.columns.iter().any(|c| {
            c.generated_always_as.is_some() || c.default.is_some() || c.identity.is_some()
        });
        let has_default_values = Self::plan_has_default_column_value(&input)?;
        let requires_delta_not_null_metadata =
            info.format.eq_ignore_ascii_case("delta") && info.columns.iter().any(|c| !c.nullable);
        if has_column_expressions || has_default_values || requires_delta_not_null_metadata {
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
                        let expr = col(column).cast_to(field.data_type(), input.schema())?;
                        Ok(Self::make_nullable_if_needed(expr, field, input.schema())?
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
                            .map(|f| {
                                let expr =
                                    col(f.name()).cast_to(field.data_type(), input.schema())?;
                                Self::make_nullable_if_needed(expr, field, input.schema())
                            })
                            .collect::<PlanResult<Vec<_>>>()?;
                        if matches.is_empty() {
                            Err(PlanError::invalid(format!(
                                "column not found for INSERT: {name}"
                            )))
                        } else {
                            matches
                                .one()
                                .map_err(|_| {
                                    PlanError::invalid(format!("ambiguous column: {name}"))
                                })
                                .map(|expr| expr.alias(name))
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
        let optional_count = info
            .columns
            .iter()
            .filter(|c| Self::table_column_is_omittable(c))
            .count();
        let identity_count = info.columns.iter().filter(|c| c.identity.is_some()).count();
        let required_count = table_field_count - optional_count;
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
            required_count,
        )?;
        let default_input_exprs = self.resolve_default_input_expressions(info, state).await?;
        let input = self.rewrite_default_column_values_in_input(
            input,
            &provided_by_input,
            &default_input_exprs,
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

        // Build intermediate plan: one alias expression per non-generated/non-identity table column,
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
            if let Some(identity) = &col.identity {
                if let Some(user_expr) = provided {
                    if !identity.allow_explicit_insert {
                        return Err(PlanError::invalid(format!(
                            "Providing values for GENERATED ALWAYS AS IDENTITY column `{}` is not supported",
                            col.name
                        )));
                    }
                    let cast_expr = user_expr
                        .clone()
                        .cast_to(col.field().data_type(), input.schema())?;
                    intermediate_aliases.push(cast_expr.alias(field_ids[idx].clone()));
                }
                continue;
            }
            let input_expr = if let Some(input_expr) = provided {
                input_expr.clone()
            } else if col.default.is_some() {
                default_input_exprs[idx].clone()
            } else {
                return Err(PlanError::invalid(format!(
                    "INSERT is missing value for non-generated column `{}`",
                    col.name
                )));
            };
            let cast_expr = input_expr.cast_to(col.field().data_type(), input.schema())?;
            intermediate_aliases.push(cast_expr.alias(field_ids[idx].clone()));
        }
        let intermediate = LogicalPlanBuilder::new(input)
            .project(intermediate_aliases)?
            .build()?;
        let identity_row_number = if identity_count > 0 {
            Some(state.register_hidden_field_name("__identity_row_number"))
        } else {
            None
        };
        let plan_for_final_projection = if let Some(alias) = identity_row_number.as_deref() {
            let row_number_window = Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                params: WindowFunctionParams {
                    args: vec![],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: WindowFrame::new(None),
                    filter: None,
                    null_treatment: Some(NullTreatment::RespectNulls),
                    distinct: false,
                },
            }))
            .alias(alias);
            LogicalPlanBuilder::from(intermediate)
                .window(vec![row_number_window])?
                .build()?
        } else {
            intermediate
        };
        let intermediate_schema = plan_for_final_projection.schema().clone();

        // Resolve each generation expression against the intermediate schema.
        let mut gen_exprs: HashMap<String, Expr> = HashMap::new();
        for col in &info.columns {
            let Some(gen_expr_str) = col.generated_always_as.as_deref() else {
                continue;
            };
            let spec_expr = parse_delta_generation_expr(gen_expr_str)?;
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
            } else if let Some(identity) = &table_col.identity {
                if provided_by_input[idx].is_some() {
                    col(Column::from_name(&field_ids[idx]))
                        .cast_to(field.data_type(), &intermediate_schema)?
                } else {
                    let row_number_alias = identity_row_number.as_deref().ok_or_else(|| {
                        PlanError::internal(format!(
                            "expected row number for identity column `{}`",
                            table_col.name
                        ))
                    })?;
                    Self::make_identity_value_expr(
                        &table_col.name,
                        identity,
                        row_number_alias,
                        &intermediate_schema,
                    )?
                    .cast_to(field.data_type(), &intermediate_schema)?
                }
            } else {
                // Regular column: reference the aliased field in intermediate plan.
                col(Column::from_name(&field_ids[idx]))
                    .cast_to(field.data_type(), &intermediate_schema)?
            };
            let expr = Self::make_nullable_if_needed(expr, &field, &intermediate_schema)?;
            let field_meta = Self::make_column_field_metadata(
                table_col,
                info.format.eq_ignore_ascii_case("delta") && !table_col.nullable,
            );
            let alias = if let Some(meta) = field_meta {
                expr.alias_with_metadata(out_name, Some(meta))
            } else {
                expr.alias(out_name)
            };
            final_exprs.push(alias);
        }
        Ok(LogicalPlanBuilder::new(plan_for_final_projection)
            .project(final_exprs)?
            .build()?)
    }

    /// Build an `Expr` for each table column that references a user-provided input
    /// value (if any). `None` indicates the user did not supply a value for that
    /// table column (only valid for generated columns and identity columns).
    ///
    /// Rules per `column_match`:
    ///
    /// - `ByPosition`:
    ///   - If input has `table_field_count` fields: column `i` maps to input `i`.
    ///   - If input has `required_count` fields: the input lines up with the
    ///     required table columns in table order; generated/default/identity columns get `None`.
    ///   - Otherwise: error.
    /// - `ByName`: each table column looks up (case-insensitive) in the input schema.
    ///   Columns not found map to `None` (allowed only for generated/default/identity cols).
    /// - `ByColumns { columns }`: `columns` enumerates the table columns being
    ///   provided. Input field `i` binds to the table column whose name matches
    ///   `columns[i]` (case-insensitive). Anything not listed maps to `None`.
    fn classify_input_to_table_columns(
        input: &LogicalPlan,
        column_match: &WriteColumnMatch,
        info: &TableInfo,
        input_field_count: usize,
        table_field_count: usize,
        required_count: usize,
    ) -> PlanResult<Vec<Option<Expr>>> {
        let input_cols = input.schema().columns();
        let mut out: Vec<Option<Expr>> = vec![None; table_field_count];
        match column_match {
            WriteColumnMatch::ByPosition => {
                if input_field_count == table_field_count {
                    if let Some(column) = info.columns.iter().find(|c| {
                        c.identity
                            .as_ref()
                            .is_some_and(|i| !i.allow_explicit_insert)
                    }) {
                        return Err(PlanError::invalid(format!(
                            "Providing values for GENERATED ALWAYS AS IDENTITY column `{}` is not supported",
                            column.name
                        )));
                    }
                    for (i, input_col) in input_cols.iter().enumerate() {
                        out[i] = Some(col(input_col.clone()));
                    }
                } else if input_field_count == required_count {
                    let mut required_idx = 0usize;
                    for (i, table_col) in info.columns.iter().enumerate() {
                        if Self::table_column_is_omittable(table_col) {
                            continue;
                        }
                        out[i] = Some(col(input_cols[required_idx].clone()));
                        required_idx += 1;
                    }
                } else {
                    return Err(PlanError::invalid(format!(
                        "input schema for INSERT has {input_field_count} fields, but table schema has {table_field_count} fields (with {} generated/default/identity)",
                        table_field_count - required_count
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
                        if table_col
                            .identity
                            .as_ref()
                            .is_some_and(|identity| !identity.allow_explicit_insert)
                        {
                            return Err(PlanError::invalid(format!(
                                "Providing values for GENERATED ALWAYS AS IDENTITY column `{}` is not supported",
                                table_col.name
                            )));
                        }
                    } else if !Self::table_column_is_omittable(table_col) {
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
                    if info.columns[pos]
                        .identity
                        .as_ref()
                        .is_some_and(|identity| !identity.allow_explicit_insert)
                    {
                        return Err(PlanError::invalid(format!(
                            "Providing values for GENERATED ALWAYS AS IDENTITY column `{}` is not supported",
                            info.columns[pos].name
                        )));
                    }
                    out[pos] = Some(col(input_col.clone()));
                }
                for table_col in &info.columns {
                    if !Self::table_column_is_omittable(table_col)
                        && !columns
                            .iter()
                            .any(|name| name.as_ref().eq_ignore_ascii_case(&table_col.name))
                    {
                        return Err(PlanError::invalid(format!(
                            "INSERT is missing value for non-generated column `{}`",
                            table_col.name
                        )));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn resolve_default_input_expressions(
        &self,
        info: &TableInfo,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        let empty_schema = Arc::new(DFSchema::empty());
        let mut out = Vec::with_capacity(info.columns.len());
        for column in &info.columns {
            let expr = if let Some(default) = column.default.as_deref() {
                let ast_expr =
                    sail_sql_analyzer::parser::parse_expression(default).map_err(|e| {
                        PlanError::invalid(format!(
                            "failed to parse default expression `{default}`: {e}"
                        ))
                    })?;
                let spec_expr = sail_sql_analyzer::expression::from_ast_expression(ast_expr)
                    .map_err(|e| {
                        PlanError::invalid(format!(
                            "failed to analyze default expression `{default}`: {e}"
                        ))
                    })?;
                // A column reference can never be a valid default value. Such text
                // comes from metadata that stored a raw string value (e.g. the
                // JSON-encoded string `"hello"` for an Iceberg column default)
                // rather than SQL expression text, so it is interpreted as a
                // string literal.
                let spec_expr = if matches!(spec_expr, spec::Expr::UnresolvedAttribute { .. }) {
                    spec::Expr::Literal(spec::Literal::Utf8 {
                        value: Some(default.to_string()),
                    })
                } else {
                    spec_expr
                };
                self.resolve_expression(spec_expr, &empty_schema, state)
                    .await?
            } else {
                lit(ScalarValue::try_from(column.field().data_type())?)
            };
            out.push(expr);
        }
        Ok(out)
    }

    fn rewrite_default_column_values_in_input(
        &self,
        input: LogicalPlan,
        provided_by_input: &[Option<Expr>],
        default_input_exprs: &[Expr],
    ) -> PlanResult<LogicalPlan> {
        if !Self::plan_has_default_column_value(&input)? {
            return Ok(input);
        }

        let input_cols = input.schema().columns();
        let mut target_by_input = vec![None; input_cols.len()];
        for (target_idx, provided) in provided_by_input.iter().enumerate() {
            let Some(Expr::Column(column)) = provided else {
                continue;
            };
            let Some(input_idx) = input_cols.iter().position(|input_col| input_col == column)
            else {
                continue;
            };
            target_by_input[input_idx] = Some(target_idx);
        }

        let input = Self::rewrite_values_default_column_values(
            input,
            &target_by_input,
            default_input_exprs,
        )?;
        if Self::plan_has_default_column_value(&input)? {
            return Err(PlanError::invalid(
                "DEFAULT is only supported as a standalone VALUES item in INSERT",
            ));
        }
        Ok(input)
    }

    fn rewrite_values_default_column_values(
        input: LogicalPlan,
        target_by_input: &[Option<usize>],
        default_input_exprs: &[Expr],
    ) -> PlanResult<LogicalPlan> {
        match input {
            LogicalPlan::Values(values) => {
                let rows = values
                    .values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .enumerate()
                            .map(|(idx, expr)| {
                                if Self::is_standalone_default_column_value_expr(&expr) {
                                    let Some(target_idx) =
                                        target_by_input.get(idx).and_then(|x| *x)
                                    else {
                                        return Err(PlanError::invalid(
                                            "DEFAULT could not be matched to a target column",
                                        ));
                                    };
                                    Ok(default_input_exprs[target_idx].clone())
                                } else if Self::expr_contains_default_column_value(&expr)? {
                                    Err(PlanError::invalid(
                                        "DEFAULT must be a standalone INSERT value",
                                    ))
                                } else {
                                    Ok(expr)
                                }
                            })
                            .collect::<PlanResult<Vec<_>>>()
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(LogicalPlanBuilder::values(rows)?.build()?)
            }
            LogicalPlan::Projection(projection) => {
                let mut child_targets = vec![None; projection.input.schema().fields().len()];
                let child_cols = projection.input.schema().columns();
                for (output_idx, target_idx) in target_by_input.iter().enumerate() {
                    let Some(target_idx) = target_idx else {
                        continue;
                    };
                    let Some(expr) = projection.expr.get(output_idx) else {
                        continue;
                    };
                    let child_column = match expr {
                        Expr::Column(column) => Some(column),
                        Expr::Alias(alias) => match alias.expr.as_ref() {
                            Expr::Column(column) => Some(column),
                            _ => None,
                        },
                        _ => None,
                    };
                    let Some(child_column) = child_column else {
                        continue;
                    };
                    let Some(child_idx) =
                        child_cols.iter().position(|column| column == child_column)
                    else {
                        continue;
                    };
                    child_targets[child_idx] = Some(*target_idx);
                }
                let input = Self::rewrite_values_default_column_values(
                    projection.input.as_ref().clone(),
                    &child_targets,
                    default_input_exprs,
                )?;
                Ok(LogicalPlan::Projection(Projection::try_new(
                    projection.expr,
                    Arc::new(input),
                )?))
            }
            other => Ok(other),
        }
    }

    pub(super) fn plan_has_default_column_value(plan: &LogicalPlan) -> PlanResult<bool> {
        let mut found = false;
        plan.apply(|node| {
            node.apply_expressions(|expr| {
                if Self::expr_contains_default_column_value(expr)? {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })?;
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(found)
    }

    fn make_nullable_if_needed(expr: Expr, field: &Field, schema: &DFSchema) -> PlanResult<Expr> {
        if field.is_nullable() && !expr.nullable(schema)? {
            let null = lit(ScalarValue::try_from(field.data_type())?);
            Ok(when(lit(true), expr).otherwise(null)?)
        } else {
            Ok(expr)
        }
    }

    fn expr_contains_default_column_value(expr: &Expr) -> datafusion_common::Result<bool> {
        let mut found = false;
        expr.apply(|expr| {
            if Self::is_default_column_value_expr(expr) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        Ok(found)
    }

    fn is_standalone_default_column_value_expr(expr: &Expr) -> bool {
        if Self::is_default_column_value_expr(expr) {
            return true;
        }
        match expr {
            Expr::Cast(cast) => Self::is_default_column_value_expr(cast.expr.as_ref()),
            Expr::TryCast(cast) => Self::is_default_column_value_expr(cast.expr.as_ref()),
            _ => false,
        }
    }

    fn is_default_column_value_expr(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Placeholder(placeholder)
                if placeholder.id == DEFAULT_COLUMN_VALUE_PLACEHOLDER_ID
        )
    }

    fn table_column_is_omittable(column: &TableColumnStatus) -> bool {
        column.generated_always_as.is_some()
            || column.default.is_some()
            || column.identity.is_some()
    }

    fn make_column_field_metadata(
        column: &TableColumnStatus,
        preserve_delta_not_null: bool,
    ) -> Option<FieldMetadata> {
        let mut builder = ColumnFeaturesBuilder::new();
        if let Some(gen_expr) = column.generated_always_as.as_deref() {
            builder = builder.with_generation_expression(gen_expr);
        }
        if let Some(default) = column.default.as_deref() {
            builder = builder.with_current_default(default);
        }
        if let Some(identity) = column.identity.as_ref() {
            builder = builder.with_identity(identity);
        }
        if preserve_delta_not_null {
            builder = builder.with_not_null_constraint();
        }
        let mut metadata = builder.build();
        metadata.insert(
            SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY.to_string(),
            column.nullable.to_string(),
        );
        Some(FieldMetadata::from(metadata))
    }

    fn make_identity_value_expr(
        column_name: &str,
        identity: &CatalogTableColumnIdentity,
        row_number_alias: &str,
        schema: &DFSchema,
    ) -> PlanResult<Expr> {
        let base = match identity.high_water_mark {
            Some(high_water_mark) => high_water_mark.checked_add(identity.step).ok_or_else(|| {
                PlanError::invalid(format!(
                    "identity column `{column_name}` cannot generate a next value without overflowing BIGINT"
                ))
            })?,
            None => identity.start,
        };
        let row_number = col(Column::from_name(row_number_alias))
            .cast_to(&datafusion::arrow::datatypes::DataType::Int64, schema)?;
        let row_offset = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(row_number),
            Operator::Minus,
            Box::new(lit(1_i64)),
        ));
        let product =
            ScalarUDF::from(SparkTryMult::new()).call(vec![row_offset, lit(identity.step)]);
        let value = ScalarUDF::from(SparkTryAdd::new()).call(vec![lit(base), product.clone()]);
        let err_msg = format!(
            "[DELTA_IDENTITY_COLUMN_VALUE_OVERFLOW] identity column `{column_name}` overflowed BIGINT while generating values"
        );
        let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(err_msg)]);
        Ok(when(product.is_null().or(value.clone().is_null()), raise).otherwise(value)?)
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

    fn has_truthy_option(options: &[OptionLayer], keys: &[&str]) -> bool {
        options.iter().rev().any(|layer| match layer {
            OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                items.iter().any(|(key, value)| {
                    keys.iter().any(|k| key.eq_ignore_ascii_case(k))
                        && value.eq_ignore_ascii_case("true")
                })
            }
            _ => false,
        })
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

pub(super) struct TableInfo {
    pub(super) lakehouse_table: Option<LakehouseExecutionContext>,
    pub(super) columns: Vec<TableColumnStatus>,
    pub(super) location: Option<String>,
    pub(super) format: String,
    pub(super) partition_by: Vec<CatalogPartitionField>,
    pub(super) sort_by: Vec<CatalogTableSort>,
    pub(super) bucket_by: Option<CatalogTableBucketBy>,
    pub(super) properties: Vec<(String, String)>,
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

    fn validate_write_info(&self, format: &str, info: &SinkInfo) -> PlanResult<()> {
        if !self.format.eq_ignore_ascii_case("iceberg")
            && !self.is_empty_or_equivalent_partitioning(&info.partition_by)
        {
            return Err(PlanError::invalid(
                "cannot specify a different partitioning when writing to an existing table",
            ));
        }
        if !self.is_empty_or_equivalent_bucketing(&info.bucket_by, &info.sort_order) {
            return Err(PlanError::invalid(
                "cannot specify a different bucketing when writing to an existing table",
            ));
        }
        if !format.is_empty() && !format.eq_ignore_ascii_case(&self.format) {
            return Err(PlanError::invalid(format!(
                "the format '{}' does not match the table format '{}'",
                format, self.format
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
