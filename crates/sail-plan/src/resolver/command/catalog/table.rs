use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    AlterTableOptions, CatalogPartitionField, CreateTableColumnOptions, CreateTableOptions,
};
use sail_common::spec;
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort, DatabaseStatus,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::utils::items::ItemTaker;

use super::validate_location_identifier;
use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) async fn resolve_catalog_create_table(
        &self,
        table: spec::ObjectName,
        definition: spec::TableDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TableDefinition {
            external,
            columns,
            comment,
            constraints,
            location,
            file_format,
            row_format,
            partition_by,
            sort_by,
            bucket_by,
            cluster_by,
            if_not_exists,
            replace,
            options,
            properties,
        } = definition;

        let mut options = options;
        let is_external = external || spec::has_path_or_location(location.as_deref(), &options);
        if row_format.is_some() {
            return Err(PlanError::todo("ROW FORMAT in CREATE TABLE statement"));
        }
        if !cluster_by.is_empty() {
            return Err(PlanError::todo("CLUSTER BY in CREATE TABLE statement"));
        }
        let mut columns = self.resolve_table_columns(columns, state)?;
        let constraints = self.resolve_table_constraints(constraints)?;
        let format = self.resolve_catalog_table_format(file_format)?;
        let location = if let Some(location) = location {
            Some(
                self.resolve_explicit_table_location(&table, &location)
                    .await?,
            )
        } else if let Some(location) = table_location_from_options(&options) {
            let location = self
                .resolve_explicit_table_location(&table, location)
                .await?;
            self.qualify_table_location_options(&table, &mut options)
                .await?;
            Some(location)
        } else if self.uses_spark_default_table_location(&table)? {
            Some(self.resolve_default_table_location(&table).await?)
        } else {
            None
        };
        let partition_by =
            self.resolve_catalog_table_partition_by(partition_by, &mut columns, state)?;
        let sort_by = self.resolve_catalog_table_sort(sort_by)?;
        let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by)?;
        let properties = properties
            .into_iter()
            .chain(options.into_iter().map(|(k, v)| (format!("option.{k}"), v)))
            .collect();

        let command = CatalogCommand::CreateTable {
            table: table.into(),
            options: CreateTableOptions {
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                if_not_exists,
                replace,
                properties,
                is_external,
            },
        };
        self.resolve_catalog_command(command)
    }

    pub(in super::super) async fn resolve_catalog_create_table_as_select(
        &self,
        table: spec::ObjectName,
        definition: spec::TableDefinition,
        query: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use super::super::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
        let spec::TableDefinition {
            external,
            columns,
            comment,
            constraints,
            location,
            file_format,
            row_format,
            partition_by,
            sort_by,
            bucket_by,
            cluster_by,
            if_not_exists,
            replace,
            options,
            properties,
        } = definition;

        let is_external = external || spec::has_path_or_location(location.as_deref(), &options);
        if row_format.is_some() {
            return Err(PlanError::todo(
                "ROW FORMAT in CREATE TABLE AS SELECT statement",
            ));
        }
        if !cluster_by.is_empty() {
            return Err(PlanError::todo(
                "CLUSTER BY in CREATE TABLE AS SELECT statement",
            ));
        }
        if !sort_by.is_empty() {
            return Err(PlanError::todo(
                "SORT_BY in CREATE TABLE AS SELECT statement",
            ));
        }
        if bucket_by.is_some() {
            return Err(PlanError::todo(
                "BUCKET_BY in CREATE TABLE AS SELECT statement",
            ));
        }
        if comment.is_some() {
            return Err(PlanError::todo(
                "COMMENT in CREATE TABLE AS SELECT statement",
            ));
        }

        if !constraints.is_empty() {
            return Err(PlanError::todo(
                "CONSTRAINTS in CREATE TABLE AS SELECT statement",
            ));
        }

        if !columns.is_empty() {
            // Follow Spark's semantics here, do not allow columns in CTAS
            return Err(PlanError::invalid(
                "Schema may not be specified in a Create Table As Select (CTAS) statement.",
            ));
        }
        // Column definitions are not allowed in PARTITIONED BY for CTAS.
        let partition_by_exprs = partition_by
            .into_iter()
            .map(|item| match item {
                spec::PartitionColumn::Definition(_) => Err(PlanError::invalid(
                    "column definitions are not allowed in PARTITIONED BY for CREATE TABLE AS SELECT statement",
                )),
                spec::PartitionColumn::Expression(expr) => Ok(expr),
            })
            .collect::<PlanResult<Vec<_>>>()?;

        // Rename the input using names in the PlanResolverState, opaque field ID -> fieldInfo.name
        let input = self.resolve_query_plan(query, state).await?;
        let column_names = PlanResolver::get_field_names(input.schema(), state)?;
        let input = rename_logical_plan(input, &column_names)?;
        let format = self.resolve_catalog_table_format(file_format)?;
        let mut write_options = options;
        if let Some(location) = location {
            let location = self
                .resolve_explicit_table_location(&table, &location)
                .await?;
            write_options.push(("path".to_string(), location));
        } else {
            self.qualify_table_location_options(&table, &mut write_options)
                .await?;
        }

        // Set write mode based on create-or-replace / if-not-exists semantics.
        let write_mode = if replace {
            WriteMode::Replace {
                error_if_absent: false,
            }
        } else if if_not_exists {
            WriteMode::IgnoreIfExists
        } else {
            WriteMode::ErrorIfExists
        };
        let partition_by = self.resolve_write_partition_by_expressions(partition_by_exprs)?;
        let builder = WritePlanBuilder::new()
            .with_target(WriteTarget::Table {
                table,
                column_match: WriteColumnMatch::ByName,
            })
            .with_mode(write_mode)
            .with_format(format)
            .with_partition_by(partition_by)
            .with_table_properties(properties)
            .with_table_is_external(is_external)
            .with_options(write_options);

        self.resolve_write_with_builder(input, builder, state).await
    }

    async fn resolve_explicit_table_location(
        &self,
        table: &spec::ObjectName,
        location: &str,
    ) -> PlanResult<String> {
        use crate::config::{qualify_absolute_table_location, qualify_table_location};
        let [qualifier @ .., _] = table.parts() else {
            return Err(PlanError::invalid("missing table name"));
        };
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let provider = catalog_manager.database_provider_by_qualifier(qualifier)?;
        if provider.uses_spark_table_location_qualification() {
            let db_location = catalog_manager
                .get_database_by_qualifier(qualifier)
                .await
                .map(|status| database_location(&status))?;
            Ok(qualify_table_location(
                location,
                db_location.as_deref(),
                &self.config.default_warehouse_directory,
            ))
        } else {
            Ok(qualify_absolute_table_location(location))
        }
    }

    pub(in super::super) async fn resolve_default_table_location(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<String> {
        use crate::config::qualify_table_location;
        let [qualifier @ .., last] = table.parts() else {
            return Err(PlanError::invalid("missing table name"));
        };
        let name: String = last.clone().into();
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let provider = catalog_manager.database_provider_by_qualifier(qualifier)?;
        if provider.requires_identifier_validation_for_default_table_location() {
            validate_location_identifier(&name, "table")?;
        }
        let location = catalog_manager
            .get_database_by_qualifier(qualifier)
            .await
            .map(|status| database_location(&status))?;

        Ok(qualify_table_location(
            &name,
            location.as_deref(),
            &self.config.default_warehouse_directory,
        ))
    }

    pub(in super::super) fn uses_spark_default_table_location(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<bool> {
        let [qualifier @ .., _] = table.parts() else {
            return Err(PlanError::invalid("missing table name"));
        };
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let provider = catalog_manager.database_provider_by_qualifier(qualifier)?;
        Ok(provider.uses_spark_default_table_location())
    }

    async fn qualify_table_location_options(
        &self,
        table: &spec::ObjectName,
        options: &mut [(String, String)],
    ) -> PlanResult<()> {
        for (key, value) in options {
            if is_table_location_option(key) && !value.trim().is_empty() {
                let location = value.clone();
                *value = self
                    .resolve_explicit_table_location(table, &location)
                    .await?;
            }
        }
        Ok(())
    }

    fn resolve_catalog_table_partition_by(
        &self,
        partition_by: Vec<spec::PartitionColumn>,
        columns: &mut Vec<CreateTableColumnOptions>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<CatalogPartitionField>> {
        let mut result = Vec::with_capacity(partition_by.len());
        for item in partition_by {
            match item {
                spec::PartitionColumn::Expression(expr) => {
                    result.push(self.resolve_partition_by_expression(expr)?);
                }
                spec::PartitionColumn::Definition(column) => {
                    let resolved = self.resolve_table_columns(vec![column], state)?.one()?;
                    if let Some(existing) = columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(&resolved.name))
                    {
                        if existing.data_type != resolved.data_type {
                            return Err(PlanError::invalid(format!(
                                "partition column '{}' has incompatible type: column definition has {:?} but PARTITIONED BY clause has {:?}",
                                resolved.name, existing.data_type, resolved.data_type
                            )));
                        }
                    } else {
                        columns.push(resolved.clone());
                    }
                    result.push(CatalogPartitionField {
                        column: resolved.name,
                        transform: None,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Resolves a table file format clause to the concrete format name to write.
    fn resolve_catalog_table_format(
        &self,
        file_format: Option<spec::TableFileFormat>,
    ) -> PlanResult<String> {
        use spec::TableFileFormat;

        if let Some(file_format) = file_format {
            match file_format {
                TableFileFormat::General { format } => Ok(format),
                TableFileFormat::Table { .. } => Err(PlanError::todo(
                    "STORED AS INPUTFORMAT ... OUTPUTFORMAT ... in CREATE TABLE statement",
                )),
            }
        } else {
            Ok(self.config.default_table_file_format.clone())
        }
    }

    fn resolve_table_columns(
        &self,
        columns: Vec<spec::TableColumnDefinition>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<CreateTableColumnOptions>> {
        columns
            .into_iter()
            .map(|x| {
                let spec::TableColumnDefinition {
                    name,
                    data_type,
                    nullable,
                    default,
                    comment,
                    generated_always_as,
                } = x;
                Ok(CreateTableColumnOptions {
                    name,
                    data_type: self.resolve_data_type(&data_type, state)?,
                    nullable,
                    comment,
                    default,
                    generated_always_as,
                })
            })
            .collect()
    }

    fn resolve_table_constraints(
        &self,
        constraints: Vec<spec::TableConstraint>,
    ) -> PlanResult<Vec<CatalogTableConstraint>> {
        Ok(constraints
            .into_iter()
            .map(|x| match x {
                spec::TableConstraint::PrimaryKey { name, columns } => {
                    let name = name.map(|x| x.into());
                    let columns = columns.into_iter().map(|x| x.into()).collect();
                    CatalogTableConstraint::PrimaryKey { name, columns }
                }
                spec::TableConstraint::Unique { name, columns } => {
                    let name = name.map(|x| x.into());
                    let columns = columns.into_iter().map(|x| x.into()).collect();
                    CatalogTableConstraint::Unique { name, columns }
                }
            })
            .collect())
    }

    pub(in super::super) fn resolve_catalog_table_sort(
        &self,
        sort: Vec<spec::SortOrder>,
    ) -> PlanResult<Vec<CatalogTableSort>> {
        sort.into_iter()
            .map(|x| {
                let spec::SortOrder {
                    child,
                    direction,
                    null_ordering,
                } = x;
                let column = match *child {
                    spec::Expr::UnresolvedAttribute {
                        name,
                        plan_id: None,
                        is_metadata_column: false,
                    } => {
                        let name: Vec<String> = name.into();
                        name.one()?
                    }
                    spec::Expr::UnresolvedFunction(function) => {
                        resolve_catalog_sort_transform_function(function)?
                    }
                    _ => {
                        return Err(PlanError::unsupported(
                            "sort column must be a column reference or transform function in CREATE TABLE statement",
                        ));
                    }
                };
                let ascending = match direction {
                    spec::SortDirection::Ascending | spec::SortDirection::Unspecified => true,
                    spec::SortDirection::Descending => false,
                };
                if !matches!(null_ordering, spec::NullOrdering::Unspecified) {
                    return Err(PlanError::unsupported(
                        "sort column null ordering in CREATE TABLE statement",
                    ));
                }
                Ok(CatalogTableSort { column, ascending })
            })
            .collect()
    }

    pub(in super::super) fn resolve_catalog_table_bucket_by(
        &self,
        bucket_by: Option<spec::SaveBucketBy>,
    ) -> PlanResult<Option<CatalogTableBucketBy>> {
        Ok(bucket_by.map(|x| {
            let spec::SaveBucketBy {
                bucket_column_names,
                num_buckets,
            } = x;
            CatalogTableBucketBy {
                columns: bucket_column_names.into_iter().map(|x| x.into()).collect(),
                num_buckets,
            }
        }))
    }

    pub(in super::super) async fn resolve_catalog_alter_table(
        &self,
        table: spec::ObjectName,
        if_exists: bool,
        operation: spec::AlterTableOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let options = match operation {
            spec::AlterTableOperation::SetTableProperties { properties } => {
                AlterTableOptions::SetTableProperties { properties }
            }
            spec::AlterTableOperation::UnsetTableProperties { keys, if_exists } => {
                AlterTableOptions::UnsetTableProperties { keys, if_exists }
            }
            spec::AlterTableOperation::AlterColumnType { name, data_type } => {
                AlterTableOptions::AlterColumnType {
                    name: name.into(),
                    data_type: self.resolve_data_type(&data_type, state)?,
                }
            }
            spec::AlterTableOperation::Unknown => {
                return Err(PlanError::todo("unsupported ALTER TABLE operation"));
            }
        };
        self.resolve_catalog_command(CatalogCommand::AlterTable {
            table: table.into(),
            if_exists,
            options,
        })
    }
}

/// Resolves a CREATE TABLE sort transform into the string format consumed by catalogs.
///
/// Supported transforms are year/month/day/hour (singular or plural), bucket(count, column),
/// and truncate(width, column) or truncate(column, width).
fn resolve_catalog_sort_transform_function(func: spec::UnresolvedFunction) -> PlanResult<String> {
    let function_name: Vec<String> = func.function_name.into();
    let function_name = function_name.one()?;
    let function_name_lower = function_name.to_lowercase();

    match function_name_lower.as_str() {
        "year" | "years" | "month" | "months" | "day" | "days" | "hour" | "hours" => {
            if func.arguments.len() != 1 {
                return Err(PlanError::invalid(format!(
                    "{function_name} sort transform expects a single column"
                )));
            }
            let column = extract_sort_column_from_args(&func.arguments, 0)?;
            Ok(format!("{function_name_lower}({column})"))
        }
        "bucket" => {
            let num_buckets = extract_sort_int_arg(&func.arguments, 0, "bucket count")?;
            let column = extract_sort_column_from_args(&func.arguments, 1)?;
            Ok(format!("bucket({num_buckets}, {column})"))
        }
        "truncate" => {
            let (column, width) = extract_sort_truncate_args(&func.arguments)?;
            Ok(format!("truncate({width}, {column})"))
        }
        _ => Err(PlanError::invalid(format!(
            "unsupported sort transform function: {function_name}"
        ))),
    }
}

fn extract_sort_truncate_args(args: &[spec::Expr]) -> PlanResult<(String, u32)> {
    if let (Ok(column), Ok(width)) = (
        extract_sort_column_from_args(args, 0),
        extract_sort_int_arg(args, 1, "truncate width"),
    ) {
        return Ok((column, width));
    }
    if let (Ok(width), Ok(column)) = (
        extract_sort_int_arg(args, 0, "truncate width"),
        extract_sort_column_from_args(args, 1),
    ) {
        return Ok((column, width));
    }
    Err(PlanError::invalid(
        "truncate sort transform expects a column reference and an integer literal width",
    ))
}

fn extract_sort_column_from_args(args: &[spec::Expr], index: usize) -> PlanResult<String> {
    let arg = args.get(index).ok_or_else(|| {
        PlanError::invalid(format!(
            "sort transform function requires argument at index {index}"
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
            "sort transform function argument must be a column reference",
        )),
    }
}

fn extract_sort_int_arg(args: &[spec::Expr], index: usize, description: &str) -> PlanResult<u32> {
    let arg = args.get(index).ok_or_else(|| {
        PlanError::invalid(format!(
            "sort transform function requires {description} at index {index}"
        ))
    })?;
    let value = match arg {
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
                PlanError::invalid(format!("{description} must be a positive integer"))
            }),
            _ => Err(PlanError::invalid(format!(
                "{description} must be an integer literal"
            ))),
        },
        _ => Err(PlanError::invalid(format!(
            "{description} must be an integer literal"
        ))),
    }?;
    if value == 0 {
        return Err(PlanError::invalid(format!(
            "{description} must be a positive integer"
        )));
    }
    Ok(value)
}

fn database_location(status: &DatabaseStatus) -> Option<String> {
    status.location.clone()
}

fn table_location_from_options(options: &[(String, String)]) -> Option<&str> {
    let find = |key: &str| {
        options.iter().find_map(|(k, v)| {
            if k.eq_ignore_ascii_case(key) && !v.trim().is_empty() {
                Some(v.as_str())
            } else {
                None
            }
        })
    };
    find("path").or_else(|| find("location"))
}

fn is_table_location_option(key: &str) -> bool {
    key.eq_ignore_ascii_case("path") || key.eq_ignore_ascii_case("location")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
    use sail_catalog::provider::{
        AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
        CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    };
    use sail_common::spec;
    use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
    use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
    use sail_common_datafusion::session::plan::PlanService;
    use sail_logical_plan::barrier::BarrierNode;
    use sail_logical_plan::file_write::{FileWriteNode, FileWriteOptions};

    use super::database_location;
    use crate::catalog::{CatalogCommandNode, SparkCatalogObjectDisplay};
    use crate::error::{PlanError, PlanResult};
    use crate::formatter::SparkPlanFormatter;
    use crate::resolver::state::PlanResolverState;
    use crate::resolver::PlanResolver;
    use crate::PlanConfig;

    struct NativeNamespaceProvider;

    #[async_trait::async_trait]
    impl CatalogProvider for NativeNamespaceProvider {
        fn get_name(&self) -> &str {
            "native"
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> sail_catalog::error::CatalogResult<DatabaseStatus> {
            unreachable!()
        }

        async fn get_database(
            &self,
            database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "native".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn list_databases(
            &self,
            prefix: Option<&Namespace>,
        ) -> sail_catalog::error::CatalogResult<Vec<DatabaseStatus>> {
            Ok(vec![DatabaseStatus {
                catalog: "native".to_string(),
                database: prefix
                    .cloned()
                    .map(Into::into)
                    .unwrap_or_else(|| vec!["default".to_string()]),
                comment: None,
                location: None,
                properties: vec![],
            }])
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            table: &str,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            Err(sail_catalog::error::CatalogError::NotFound(
                sail_catalog::error::CatalogObject::Table,
                table.to_string(),
            ))
        }

        async fn list_tables(
            &self,
            _database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_view(
            &self,
            _database: &Namespace,
            _view: &str,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_views(
            &self,
            _database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }
    }

    fn create_session(default_catalog: &str) -> PlanResult<SessionContext> {
        let mut state = SessionStateBuilder::new().build();
        let catalog_manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([
                (
                    "sail".to_string(),
                    Arc::new(sail_catalog_memory::MemoryCatalogProvider::new(
                        "sail".to_string(),
                        vec![Arc::from("default")].try_into()?,
                        None,
                        None,
                    )) as Arc<dyn CatalogProvider>,
                ),
                (
                    "native".to_string(),
                    Arc::new(NativeNamespaceProvider) as Arc<dyn CatalogProvider>,
                ),
                (
                    "nested/table".to_string(),
                    Arc::new(NativeNamespaceProvider) as Arc<dyn CatalogProvider>,
                ),
            ]),
            default_catalog: default_catalog.to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })?;
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        state.config_mut().set_extension(Arc::new(catalog_manager));
        state.config_mut().set_extension(Arc::new(plan_service));
        Ok(SessionContext::new_with_state(state))
    }

    fn table_definition(
        location: Option<&str>,
        file_format: Option<&str>,
    ) -> spec::TableDefinition {
        spec::TableDefinition {
            external: false,
            columns: vec![],
            comment: None,
            constraints: vec![],
            location: location.map(ToString::to_string),
            file_format: file_format.map(|format| spec::TableFileFormat::General {
                format: format.to_string(),
            }),
            row_format: None,
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            cluster_by: vec![],
            if_not_exists: false,
            replace: false,
            options: vec![],
            properties: vec![],
        }
    }

    fn ctas_query() -> spec::QueryPlan {
        spec::QueryPlan::new(spec::QueryNode::LocalRelation {
            data: None,
            schema: Some(spec::Schema {
                fields: vec![spec::Field {
                    name: "id".to_string(),
                    data_type: spec::DataType::Int32,
                    nullable: true,
                    metadata: vec![],
                }]
                .into(),
            }),
        })
    }

    fn resolved_create_table_options(plan: LogicalPlan) -> PlanResult<CreateTableOptions> {
        let LogicalPlan::Extension(extension) = plan else {
            return Err(PlanError::internal("expected extension logical plan"));
        };
        let barrier = extension
            .node
            .as_any()
            .downcast_ref::<BarrierNode>()
            .ok_or_else(|| PlanError::internal("expected barrier node"))?;
        let precondition = barrier
            .preconditions()
            .first()
            .ok_or_else(|| PlanError::internal("expected catalog precondition"))?;
        let LogicalPlan::Extension(extension) = precondition.as_ref() else {
            return Err(PlanError::internal("expected extension precondition"));
        };
        let command = extension
            .node
            .as_any()
            .downcast_ref::<CatalogCommandNode>()
            .ok_or_else(|| PlanError::internal("expected catalog command node"))?
            .command()
            .clone();
        let sail_catalog::command::CatalogCommand::CreateTable { options, .. } = command else {
            return Err(PlanError::internal("expected create table command"));
        };
        Ok(options)
    }

    fn resolved_file_write_options(plan: &LogicalPlan) -> PlanResult<FileWriteOptions> {
        let LogicalPlan::Extension(extension) = plan else {
            return Err(PlanError::internal("expected extension logical plan"));
        };
        let barrier = extension
            .node
            .as_any()
            .downcast_ref::<BarrierNode>()
            .ok_or_else(|| PlanError::internal("expected barrier node"))?;
        let LogicalPlan::Extension(extension) = barrier.plan() else {
            return Err(PlanError::internal("expected extension write plan"));
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<FileWriteNode>()
            .ok_or_else(|| PlanError::internal("expected file write node"))?;
        Ok(node.options().clone())
    }

    fn direct_create_table_options(plan: LogicalPlan) -> PlanResult<CreateTableOptions> {
        let LogicalPlan::Extension(extension) = plan else {
            return Err(PlanError::internal("expected extension logical plan"));
        };
        let command = extension
            .node
            .as_any()
            .downcast_ref::<CatalogCommandNode>()
            .ok_or_else(|| PlanError::internal("expected catalog command node"))?
            .command()
            .clone();
        let sail_catalog::command::CatalogCommand::CreateTable { options, .. } = command else {
            return Err(PlanError::internal("expected create table command"));
        };
        Ok(options)
    }

    #[test]
    fn database_location_prefers_explicit_location_over_properties() {
        let status = DatabaseStatus {
            catalog: "test".to_string(),
            database: vec!["db".to_string()],
            comment: None,
            location: Some("s3://bucket/explicit".to_string()),
            properties: vec![("warehouse".to_string(), "s3://bucket/warehouse".to_string())],
        };
        assert_eq!(
            database_location(&status).as_deref(),
            Some("s3://bucket/explicit")
        );
    }

    #[test]
    fn database_location_does_not_interpret_provider_properties() {
        let status = DatabaseStatus {
            catalog: "test".to_string(),
            database: vec!["db".to_string()],
            comment: None,
            location: None,
            properties: vec![("warehouse".to_string(), "s3://bucket/warehouse".to_string())],
        };
        assert_eq!(database_location(&status), None);
    }

    #[tokio::test]
    async fn test_create_table_rejects_invalid_name_for_default_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let err = match resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "../escaped"]),
                table_definition(None, None),
                &mut state,
            )
            .await
        {
            Ok(plan) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {plan:?}"
                )))
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid table name"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_allows_invalid_name_for_explicit_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "nested/table"]),
                table_definition(Some("relative/table"), None),
                &mut state,
            )
            .await
            .map(|_| ())?;
        Ok(())
    }

    #[tokio::test]
    async fn test_native_create_table_preserves_explicit_relative_location() -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "explicit_location"]),
                table_definition(Some("relative/table"), None),
                &mut state,
            )
            .await?;
        let options = direct_create_table_options(plan)?;

        assert_eq!(options.location.as_deref(), Some("relative/table"));
        Ok(())
    }

    #[tokio::test]
    async fn test_native_create_table_without_location_delegates_location_to_catalog(
    ) -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "catalog_owned_location"]),
                table_definition(None, None),
                &mut state,
            )
            .await?;
        let options = direct_create_table_options(plan)?;

        assert_eq!(options.location, None);
        assert!(!options.is_external);
        Ok(())
    }

    #[tokio::test]
    async fn test_spark_create_table_without_location_synthesizes_default_location(
    ) -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "managed_location"]),
                table_definition(None, None),
                &mut state,
            )
            .await?;
        let options = direct_create_table_options(plan)?;
        let location = options
            .location
            .ok_or_else(|| PlanError::internal("expected location"))?;

        assert!(
            location.ends_with("/managed_location"),
            "unexpected location: {location}"
        );
        assert!(!options.is_external);
        Ok(())
    }

    #[tokio::test]
    async fn test_spark_create_table_qualifies_explicit_relative_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "explicit_location"]),
                table_definition(Some("relative/table"), None),
                &mut state,
            )
            .await?;
        let options = direct_create_table_options(plan)?;
        let location = options
            .location
            .ok_or_else(|| PlanError::internal("expected location"))?;

        assert!(
            location.ends_with("/relative/table"),
            "unexpected location: {location}"
        );
        assert_ne!(location, "relative/table");
        Ok(())
    }

    #[tokio::test]
    async fn test_spark_create_table_with_path_option_uses_qualified_path_as_location(
    ) -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let mut definition = table_definition(None, None);
        definition
            .options
            .push(("path".to_string(), "relative/table".to_string()));

        let plan = resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "path_option"]),
                definition,
                &mut state,
            )
            .await?;
        let options = direct_create_table_options(plan)?;
        let location = options
            .location
            .ok_or_else(|| PlanError::internal("expected location"))?;

        assert!(options.is_external);
        assert!(
            location.ends_with("/relative/table"),
            "unexpected location: {location}"
        );
        assert_ne!(location, "relative/table");
        Ok(())
    }

    #[tokio::test]
    async fn test_native_create_table_with_path_option_preserves_relative_location(
    ) -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let mut definition = table_definition(None, None);
        definition
            .options
            .push(("path".to_string(), "relative/table".to_string()));

        let plan = resolver
            .resolve_catalog_create_table(
                spec::ObjectName::from(["default", "path_option"]),
                definition,
                &mut state,
            )
            .await?;
        let options = direct_create_table_options(plan)?;

        assert!(options.is_external);
        assert_eq!(options.location.as_deref(), Some("relative/table"));
        Ok(())
    }

    #[tokio::test]
    async fn test_ctas_rejects_invalid_name_for_default_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let err = match resolver
            .resolve_default_table_location(&spec::ObjectName::from(["default", "../escaped"]))
            .await
        {
            Ok(location) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {location:?}"
                )));
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_native_default_location_allows_invalid_name() -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        resolver
            .resolve_default_table_location(&spec::ObjectName::from(["default", "nested/table"]))
            .await
            .map(|_| ())?;
        Ok(())
    }

    #[tokio::test]
    async fn test_default_table_location_uses_qualifier_provider_not_table_name_provider(
    ) -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let err = match resolver
            .resolve_default_table_location(&spec::ObjectName::bare("nested/table"))
            .await
        {
            Ok(location) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {location:?}"
                )));
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_ctas_without_location_remains_managed() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table_as_select(
                spec::ObjectName::from(["default", "ctas_managed"]),
                table_definition(None, Some("parquet")),
                ctas_query(),
                &mut state,
            )
            .await?;

        let options = resolved_create_table_options(plan)?;
        assert!(!options.is_external);
        assert!(options.location.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_ctas_with_location_is_external() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table_as_select(
                spec::ObjectName::from(["default", "ctas_location"]),
                table_definition(Some("relative/table"), Some("parquet")),
                ctas_query(),
                &mut state,
            )
            .await?;

        let options = resolved_create_table_options(plan)?;
        assert!(options.is_external);
        assert!(options.location.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_native_ctas_preserves_explicit_relative_location() -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table_as_select(
                spec::ObjectName::from(["default", "ctas_location"]),
                table_definition(Some("relative/table"), Some("parquet")),
                ctas_query(),
                &mut state,
            )
            .await?;

        let options = resolved_create_table_options(plan)?;
        assert!(options.is_external);
        assert_eq!(options.location.as_deref(), Some("relative/table"));
        Ok(())
    }

    #[tokio::test]
    async fn test_ctas_with_path_option_is_external() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let mut definition = table_definition(None, Some("parquet"));
        definition
            .options
            .push(("path".to_string(), "relative/table".to_string()));

        let plan = resolver
            .resolve_catalog_create_table_as_select(
                spec::ObjectName::from(["default", "ctas_path_option"]),
                definition,
                ctas_query(),
                &mut state,
            )
            .await?;

        let options = resolved_create_table_options(plan)?;
        assert!(options.is_external);
        let location = options
            .location
            .ok_or_else(|| PlanError::internal("expected location"))?;
        assert!(
            location.ends_with("/relative/table"),
            "unexpected location: {location}"
        );
        assert_ne!(location, "relative/table");
        Ok(())
    }

    #[tokio::test]
    async fn test_native_ctas_without_location_delegates_location_to_catalog() -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();

        let plan = resolver
            .resolve_catalog_create_table_as_select(
                spec::ObjectName::from(["default", "ctas_without_location"]),
                table_definition(None, Some("parquet")),
                ctas_query(),
                &mut state,
            )
            .await?;

        let write_options = resolved_file_write_options(&plan)?;
        let options = resolved_create_table_options(plan)?;
        assert_eq!(options.location, None);
        assert!(!options.is_external);
        assert_eq!(
            write_options.catalog_table,
            Some(vec![
                "default".to_string(),
                "ctas_without_location".to_string()
            ])
        );
        Ok(())
    }
}
