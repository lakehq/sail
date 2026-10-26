use std::collections::{HashMap, HashSet};

use arrow_schema::{Field, Schema};
use datafusion_common::ScalarValue;
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::LakehouseCreateRequest;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CatalogPartitionField, CreateTableColumnOptions, CreateTableMode, CreateTableOptions,
    PartitionTransform,
};
use sail_common::spec;
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableSort, LakehouseExecutionContext, LakehouseOperation,
    TableColumnStatus, TableKind,
};
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::datasource::{
    BucketBy, DataSourceRegistry, OptionLayer, SourceInfo, find_path_in_options,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
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
pub(in crate::resolver) enum WriteMode {
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

pub(in crate::resolver) enum WriteTarget {
    DataSource,
    Table {
        table: spec::ObjectName,
        column_match: WriteColumnMatch,
    },
}

#[expect(clippy::enum_variant_names)]
pub(in crate::resolver) enum WriteColumnMatch {
    ByPosition,
    ByName,
    ByColumns { columns: Vec<spec::Identifier> },
}

/// A unified logical plan builder for all write or insert operations.
pub(in crate::resolver) struct WritePlanBuilder {
    target: Option<WriteTarget>,
    mode: Option<WriteMode>,
    format: Option<String>,
    partition_by: Vec<CatalogPartitionField>,
    bucket_by: Option<spec::SaveBucketBy>,
    sort_by: Vec<spec::SortOrder>,
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
    pub(in crate::resolver) async fn resolve_write_with_builder(
        &mut self,
        mut input: Query,
        builder: WritePlanBuilder,
    ) -> PlanResult<CommandKind> {
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
        let input_schema = input.schema.resolved("write input")?.arrow_schema()?;
        let use_session_overwrite_mode = self.context.config.dynamic_partition_overwrite
            && (matches!(mode, WriteMode::Truncate)
                || matches!(
                    (&target, &mode),
                    (WriteTarget::DataSource, WriteMode::Replace { .. })
                ));
        let mut write_format = format.unwrap_or_default();
        let mut sink_info = WriteOperation {
            format: String::new(),
            preconditions: vec![],
            input: input.clone(),
            // The mode will be set later so the value here is just a placeholder.
            mode: WriteDisposition::ErrorIfExists,
            partition_by: partition_by.clone(),
            sort_order: self.write_sort_orders(&input, sort_by.clone()).await?,
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
                    write_format = self.context.config.default_table_file_format.clone();
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
                        .rewrite_data_source_delta_table_features(input, &write_format, &sink_info)
                        .await?;
                }
                let schema_for_cond =
                    matches!(mode, WriteMode::TruncateIf { .. }).then_some(&input_schema);
                sink_info.mode = self.resolve_write_mode(mode, schema_for_cond).await?;
            }
            WriteTarget::Table {
                table,
                column_match,
            } => {
                let info = self.resolve_table_info(&table).await?;

                // Return early if the target exists and the mode says to skip
                if matches!(mode, WriteMode::IgnoreIfExists) && info.is_some() {
                    return Ok(CommandKind::Query(Self::empty(false)));
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
                    return Err(PlanError::analysis(format!(
                        "[TABLE_OR_VIEW_NOT_FOUND] The table or view {} cannot be found",
                        table
                            .parts()
                            .iter()
                            .map(|part| format!("`{}`", part.as_ref().replace('`', "``")))
                            .collect::<Vec<_>>()
                            .join(".")
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
                    let lake_source_merge_schema = (info.format.eq_ignore_ascii_case("delta")
                        || info.format.eq_ignore_ascii_case("iceberg"))
                        && Self::has_truthy_option(
                            &sink_info.options,
                            &["mergeSchema", "merge_schema"],
                        );
                    if !lake_source_merge_schema {
                        input = self.rewrite_write_input(input, column_match, info).await?;
                    }
                    input = self.apply_delta_table_constraints(input, info).await?;
                    if sink_info.partition_by.is_empty()
                        || !info.format.eq_ignore_ascii_case("iceberg")
                    {
                        sink_info.partition_by = info.partition_by.clone();
                    }
                    sink_info.sort_order = self.catalog_write_sorts(&input, &info.sort_by)?;
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
                            write_format = self.context.config.default_table_file_format.clone();
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
                    let create_mode = match &mode {
                        WriteMode::Append { .. } => CreateTableMode::CreateIfNotExists,
                        WriteMode::Replace {
                            error_if_absent: true,
                        } => CreateTableMode::Replace,
                        WriteMode::Replace {
                            error_if_absent: false,
                        } => CreateTableMode::CreateOrReplace,
                        // ErrorIfExists or IgnoreIfExists
                        _ => CreateTableMode::Create,
                    };
                    let columns = input
                        .schema
                        .resolved("write columns")?
                        .arrow_schema()?
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
                    let sort_by = self.resolve_catalog_table_sort(sort_by)?;
                    let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by)?;
                    let catalog_table = table
                        .parts()
                        .iter()
                        .map(|part| part.as_ref().to_string())
                        .collect::<Vec<_>>();
                    let mut create_options = CreateTableOptions {
                        columns,
                        comment: None,
                        constraints: vec![],
                        location: table_location,
                        format: write_format.clone(),
                        partition_by,
                        sort_by,
                        bucket_by,
                        mode: create_mode,
                        properties,
                        is_external: table_is_external || write_options_had_location,
                        is_write_precondition: true,
                    };
                    let create_plan = self
                        .context
                        .session
                        .extension::<CatalogManager>()?
                        .plan_lakehouse_create(
                            &catalog_table,
                            LakehouseCreateRequest {
                                catalog_table: catalog_table.clone(),
                                options: create_options.clone(),
                            },
                        )
                        .await?;
                    if let Some(location) = create_plan.table.status.location.clone()
                        && create_options.location.as_deref() != Some(location.as_str())
                    {
                        create_options.location = Some(location.clone());
                        sink_info.options.push(OptionLayer::OptionList {
                            items: vec![("path".to_string(), location)],
                        });
                    }
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
                    preconditions.push(self.resolve_catalog_command(command)?.into());
                }

                if sink_info.lakehouse_table.is_none() {
                    sink_info.lakehouse_table = info
                        .as_ref()
                        .and_then(|info| info.lakehouse_table.as_ref())
                        .map(|context| context.for_operation(LakehouseOperation::Write));
                }

                sink_info.mode = self
                    .resolve_write_mode(mode, schema_for_cond.as_ref())
                    .await?;
            }
        };
        input = self
            .rewrite_delta_check_constraints_from_options(input, &write_format, &sink_info)
            .await?;
        if use_session_overwrite_mode && matches!(sink_info.mode, WriteDisposition::Overwrite) {
            let option = if write_format.eq_ignore_ascii_case("delta") {
                Some("partitionOverwriteMode")
            } else if write_format.eq_ignore_ascii_case("iceberg") {
                Some("overwrite-mode")
            } else {
                None
            };
            if let Some(option) = option {
                sink_info.options.insert(
                    0,
                    OptionLayer::OptionList {
                        items: vec![(option.into(), "dynamic".into())],
                    },
                );
            }
        }
        let input = crate::analyzer::row_identity::materialize(input, &mut self.state.ids)?;
        sink_info.input =
            crate::analyzer::scalar_iterator::materialize(input, &mut self.state.ids)?;
        sink_info.format = write_format;
        sink_info.preconditions = preconditions;
        Ok(CommandKind::Write(Box::new(sink_info)))
    }

    pub(in crate::resolver) async fn resolve_table_info(
        &mut self,
        table: &spec::ObjectName,
    ) -> PlanResult<Option<TableInfo>> {
        let status = match self
            .context
            .session
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
                let mut default_values = HashMap::new();
                // When a table is created without column definitions
                // (e.g. `CREATE TABLE t USING fmt`), the catalog stores an empty column list.
                // Discover the schema from the data source so that write operations
                // (INSERT INTO) can validate the input schema correctly.
                if columns.is_empty() || format.eq_ignore_ascii_case("iceberg") {
                    let registry = self
                        .context
                        .session
                        .extension::<DataSourceRegistry>()
                        .map_err(|e| {
                            PlanError::invalid(format!(
                                "failed to access source registry for table `{table:?}`: {e}",
                            ))
                        })?;
                    let data_source = registry.get_data_source(&format).map_err(|e| {
                        PlanError::invalid(format!(
                            "failed to resolve data source `{format}` for table `{table:?}`: {e}",
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
                        read_case_sensitive: self.context.config.case_sensitive,
                    };
                    if let Some(lake_source) = registry
                        .get_lake_source_if_supported(&format)
                        .map_err(PlanError::from)?
                    {
                        let metadata = lake_source
                            .infer_metadata(&self.context.session.state(), info)
                            .await
                            .map_err(|e| {
                                PlanError::invalid(format!(
                                    "failed to infer metadata for table `{table:?}` from format `{format}`: {e}",
                                ))
                            })?;
                        if columns.is_empty() {
                            columns =
                                Self::table_columns_from_format_schema(metadata.schema.as_ref());
                        } else {
                            columns.retain(|column| {
                                metadata.schema.field_with_name(&column.name).is_ok()
                            });
                            Self::merge_format_columns(&mut columns, metadata.schema.as_ref());
                        }
                        for field in metadata.schema.fields() {
                            if let Some(value) =
                                ColumnFeatures::from_field(field).current_default_value()?
                            {
                                default_values.insert(field.name().clone(), value);
                            }
                        }
                        if !metadata.properties.is_empty() {
                            let mut merged_properties = metadata.properties;
                            merged_properties.extend(properties);
                            properties = merged_properties;
                        }
                    } else {
                        let schema = data_source
                            .infer_schema(&self.context.session.state(), info)
                            .await
                            .map_err(|e| {
                                PlanError::invalid(format!(
                                    "failed to infer schema for table `{table:?}` from data source `{format}`: {e}",
                                ))
                            })?;
                        columns = Self::table_columns_from_format_schema(schema.as_ref());
                    }
                } else if format.eq_ignore_ascii_case("delta") && location.is_some() {
                    let registry = self
                        .context
                        .session
                        .extension::<DataSourceRegistry>()
                        .map_err(|e| {
                            PlanError::invalid(format!(
                                "failed to access lake source registry for table `{table:?}`: {e}",
                            ))
                        })?;
                    let lake_source = registry.get_lake_source(&format).map_err(|e| {
                        PlanError::invalid(format!(
                            "failed to resolve lake source `{format}` for table `{table:?}`: {e}",
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
                        read_case_sensitive: self.context.config.case_sensitive,
                    };
                    match lake_source
                        .infer_metadata(&self.context.session.state(), info)
                        .await
                    {
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
                    default_values,
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

    fn resolve_write_bucket_by(
        &mut self,
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
pub(in crate::resolver) struct TableInfo {
    pub(in crate::resolver) lakehouse_table: Option<LakehouseExecutionContext>,
    pub(in crate::resolver) columns: Vec<TableColumnStatus>,
    pub(in crate::resolver) default_values: HashMap<String, ScalarValue>,
    pub(in crate::resolver) location: Option<String>,
    pub(in crate::resolver) format: String,
    pub(in crate::resolver) partition_by: Vec<CatalogPartitionField>,
    pub(in crate::resolver) sort_by: Vec<CatalogTableSort>,
    pub(in crate::resolver) bucket_by: Option<CatalogTableBucketBy>,
    pub(in crate::resolver) properties: Vec<(String, String)>,
}

impl TableInfo {
    pub(in crate::resolver) fn has_column_default(&self, column: &TableColumnStatus) -> bool {
        column.default.is_some() || self.default_values.contains_key(&column.name)
    }

    pub(in crate::resolver) fn column_is_omittable(
        &self,
        column: &TableColumnStatus,
        column_match: &WriteColumnMatch,
    ) -> bool {
        column.generated_always_as.is_some()
            || self.has_column_default(column)
            || column.identity.is_some()
            || ((self.format.eq_ignore_ascii_case("iceberg")
                || self.format.eq_ignore_ascii_case("delta"))
                && column.nullable
                && !matches!(column_match, WriteColumnMatch::ByPosition))
    }

    pub(in crate::resolver) fn schema(&self) -> Schema {
        let fields = self
            .columns
            .iter()
            .map(|col| col.field())
            .collect::<Vec<_>>();
        Schema::new(fields)
    }

    pub(in crate::resolver) fn validate_write_info(
        &self,
        format: &str,
        info: &WriteOperation,
    ) -> PlanResult<()> {
        if !self.format.eq_ignore_ascii_case("iceberg")
            && !self.is_empty_or_equivalent_partitioning(&info.partition_by)
        {
            return Err(PlanError::invalid(
                "cannot specify a different partitioning when writing to an existing table",
            ));
        }
        if !self.is_empty_or_equivalent_bucketing(
            &info.bucket_by,
            &info.sort_order,
            info.input.schema.resolved("write sorting")?,
        ) {
            return Err(PlanError::invalid(
                "cannot specify a different bucketing when writing to an existing table",
            ));
        }
        if !format.is_empty() && !format.eq_ignore_ascii_case(&self.format) {
            return Err(PlanError::invalid(format!(
                "the format '{}' does not match the table data source format '{}'",
                format, self.format
            )));
        }
        Ok(())
    }

    pub(in crate::resolver) fn is_empty_or_equivalent_partitioning(
        &self,
        partition_by: &[CatalogPartitionField],
    ) -> bool {
        partition_by.is_empty()
            || (partition_by.len() == self.partition_by.len()
                && partition_by
                    .iter()
                    .zip(self.partition_by.iter())
                    .all(|(a, b)| Self::partition_fields_match(a, b)))
    }

    pub(in crate::resolver) fn partition_fields_match(
        a: &CatalogPartitionField,
        b: &CatalogPartitionField,
    ) -> bool {
        pub(in crate::resolver) fn normalize_transform(
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

    pub(in crate::resolver) fn is_empty_or_equivalent_bucketing(
        &self,
        bucket_by: &Option<BucketBy>,
        sort_by: &[SortKey],
        schema: &QuerySchema,
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
                        let Deferred::Resolved(ExprKind::Attribute(id)) = &a.expression.kind else {
                            return false;
                        };
                        schema
                            .attribute(*id)
                            .is_ok_and(|attribute| attribute.name.eq_ignore_ascii_case(&b.column))
                            && a.ascending == b.ascending
                    })
            }
        };
        bucket_by_match && sort_by_match
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn resolve_write_input(
        &mut self,
        input: spec::QueryPlan,
    ) -> PlanResult<Query> {
        self.resolve_query_plan(input).await
    }

    pub(in crate::resolver) fn resolve_write_cluster_by_columns(
        &self,
        values: Vec<spec::Identifier>,
    ) -> PlanResult<Vec<spec::ObjectName>> {
        Ok(values.into_iter().map(spec::ObjectName::bare).collect())
    }

    pub(in crate::resolver) fn resolve_write_partition_by_expressions(
        &self,
        values: Vec<spec::Expr>,
    ) -> PlanResult<Vec<CatalogPartitionField>> {
        values
            .into_iter()
            .map(|value| self.resolve_partition_by_expression(value))
            .collect()
    }

    async fn write_sort_orders(
        &mut self,
        input: &Query,
        requests: Vec<spec::SortOrder>,
    ) -> PlanResult<Vec<SortKey>> {
        let schema = input.schema.resolved("write sorting input")?;
        let mut result = vec![];
        for request in requests {
            result.push(self.resolve_sort_order(request, schema).await?);
        }
        Ok(result)
    }

    fn catalog_write_sorts(
        &mut self,
        input: &Query,
        requests: &[CatalogTableSort],
    ) -> PlanResult<Vec<SortKey>> {
        let schema = input.schema.resolved("catalog write sorting input")?;
        requests
            .iter()
            .map(|sort| {
                let attribute = schema
                    .attributes
                    .iter()
                    .find(|attribute| attribute.name.eq_ignore_ascii_case(&sort.column))
                    .ok_or_else(|| {
                        PlanError::invalid(format!("sort column not found: {}", sort.column))
                    })?;
                Ok(SortKey {
                    expression: self.reference(attribute)?,
                    ascending: sort.ascending,
                    nulls_first: sort.ascending,
                })
            })
            .collect()
    }

    async fn resolve_write_mode(
        &mut self,
        mode: WriteMode,
        schema: Option<&Schema>,
    ) -> PlanResult<WriteDisposition> {
        Ok(match mode {
            WriteMode::ErrorIfExists => WriteDisposition::ErrorIfExists,
            WriteMode::IgnoreIfExists => WriteDisposition::IgnoreIfExists,
            WriteMode::Append { .. } => WriteDisposition::Append,
            WriteMode::Replace { .. } | WriteMode::Truncate => WriteDisposition::Overwrite,
            WriteMode::TruncatePartitions => WriteDisposition::OverwritePartitions,
            WriteMode::TruncateIf { condition } => {
                let schema = schema.ok_or_else(|| {
                    PlanError::internal(
                        "conditional overwrite is not allowed without a table schema",
                    )
                })?;
                let schema = QuerySchema {
                    attributes: schema
                        .fields()
                        .iter()
                        .map(|field| {
                            self.context
                                .function_context(&mut self.state.ids)
                                .attribute(field.name().clone(), ExprType::from_field(field))
                        })
                        .collect(),
                };
                let expression = self
                    .resolve_named_expression(condition.expr, &schema)
                    .await?
                    .expression;
                let expression = self
                    .context
                    .function_context(&mut self.state.ids)
                    .cast(expression, arrow_schema::DataType::Boolean)?;
                WriteDisposition::OverwriteIf {
                    condition: Box::new(expression),
                    schema,
                    source: condition.source,
                }
            }
        })
    }
}

use crate::analyzer::writer::{WriteDisposition, WriteOperation};
