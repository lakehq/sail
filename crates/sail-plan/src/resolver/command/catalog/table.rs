use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{CreateTableColumnOptions, CreateTableOptions};
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

        if row_format.is_some() {
            return Err(PlanError::todo("ROW FORMAT in CREATE TABLE statement"));
        }
        if !cluster_by.is_empty() {
            return Err(PlanError::todo("CLUSTER BY in CREATE TABLE statement"));
        }
        let columns = self.resolve_table_columns(columns, state)?;
        let constraints = self.resolve_table_constraints(constraints)?;
        let location = if let Some(location) = location {
            location
        } else {
            self.resolve_default_table_location(&table).await?
        };
        let format = self.resolve_catalog_table_format(file_format)?;
        let partition_by = self.resolve_write_partition_by_expressions(partition_by)?;
        let sort_by = self.resolve_catalog_table_sort(sort_by)?;
        let bucket_by = self.resolve_catalog_table_bucket_by(bucket_by)?;

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
                options,
                properties,
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
        if replace {
            return Err(PlanError::todo(
                "REPLACE in CREATE TABLE AS SELECT statement",
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

        // Rename the input using names in the PlanResolverState, opaque field ID -> fieldInfo.name
        let input = self.resolve_query_plan(query, state).await?;
        let column_names = PlanResolver::get_field_names(input.schema(), state)?;
        let input = rename_logical_plan(input, &column_names)?;
        let format = self.resolve_catalog_table_format(file_format)?;
        let mut write_options = options;
        if let Some(location) = location {
            write_options.push(("path".to_string(), location));
        }

        // Set write mode based on if_not_exists
        let write_mode = if if_not_exists {
            WriteMode::IgnoreIfExists
        } else {
            WriteMode::ErrorIfExists
        };
        let partition_by = self.resolve_write_partition_by_expressions(partition_by)?;
        let builder = WritePlanBuilder::new()
            .with_target(WriteTarget::Table {
                table,
                column_match: WriteColumnMatch::ByName,
            })
            .with_mode(write_mode)
            .with_format(format)
            .with_partition_by(partition_by)
            .with_table_properties(properties)
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
                    _ => {
                        return Err(PlanError::unsupported(
                            "sort column must be a column reference in CREATE TABLE statement",
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
}
