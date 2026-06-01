use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    AlterTableOptions, CatalogPartitionField, CreateTableColumnOptions, CreateTableOptions,
};
use sail_common::spec;
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::utils::items::ItemTaker;

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

        let is_external = external || spec::has_path_or_location(location.as_deref(), &options);
        if row_format.is_some() {
            return Err(PlanError::todo("ROW FORMAT in CREATE TABLE statement"));
        }
        if !cluster_by.is_empty() {
            return Err(PlanError::todo("CLUSTER BY in CREATE TABLE statement"));
        }
        let mut columns = self.resolve_table_columns(columns, state)?;
        let constraints = self.resolve_table_constraints(constraints)?;
        let location = if let Some(location) = location {
            location
        } else {
            self.resolve_default_table_location(&table).await?
        };
        let format = self.resolve_catalog_table_format(file_format)?;
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
                location: Some(location),
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
                    "column definitions are not allowed in PARTITIONED BY \
                     for CREATE TABLE AS SELECT statement",
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
            write_options.push(("path".to_string(), location));
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

    pub(in super::super) async fn resolve_default_table_location(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<String> {
        let [qualifier @ .., last] = table.parts() else {
            return Err(PlanError::invalid("missing table name"));
        };
        let name: String = last.clone().into();
        // For characters in the table name that are not alphanumeric, `-`, or `_`,
        // replace with a fixed-width hex encoding of the Unicode code point:
        // `u+XXXX` for U+0000..U+FFFF and `U+XXXXXXXX` for U+10000..U+10FFFF.
        let name: String = name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c.to_string()
                } else {
                    let v = c as u32;
                    if v <= 0xFFFF {
                        format!("u+{v:04X}")
                    } else {
                        format!("U+{v:08X}")
                    }
                }
            })
            .collect();
        // We use our own logic to map tables to locations. This avoids conflicts
        // and avoids issues with special characters in table names.
        // Note that this is different from how Spark handles table locations
        // for the default catalog.
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let location = catalog_manager
            .get_database_by_qualifier(qualifier)
            .await?
            .location;
        let (base, suffix) = match &location {
            Some(loc) => (
                loc.trim_end_matches(object_store::path::DELIMITER),
                String::new(),
            ),
            None => (
                self.config
                    .default_warehouse_directory
                    .trim_end_matches(object_store::path::DELIMITER),
                format!("-{}", uuid::Uuid::new_v4()),
            ),
        };
        Ok(format!(
            "{}{}{}{}",
            base,
            object_store::path::DELIMITER,
            name,
            suffix,
        ))
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
                                "partition column '{}' has incompatible type: \
                                 column definition has {:?} but PARTITIONED BY clause has {:?}",
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
            spec::AlterTableOperation::AddCheckConstraint { .. } => {
                return Err(PlanError::unsupported(
                    "ALTER TABLE ADD CONSTRAINT is only supported for Delta Lake tables",
                ));
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
