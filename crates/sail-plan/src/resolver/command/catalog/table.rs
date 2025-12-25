use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{CreateTableColumnOptions, CreateTableOptions};
use sail_common::spec;
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort,
};
use sail_common_datafusion::utils::items::ItemTaker;
use uuid::Uuid;

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
            self.resolve_default_table_location(&table)?
        };
        let format = self.resolve_catalog_table_format(file_format)?;
        let partition_by = partition_by.into_iter().map(|x| x.into()).collect();
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
        _table: spec::ObjectName,
        _definition: spec::TableDefinition,
        _query: spec::QueryPlan,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("CREATE TABLE ... AS SELECT ..."))
    }

    pub(in super::super) fn resolve_default_table_location(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<String> {
        let name: String = table
            .parts()
            .last()
            .ok_or_else(|| PlanError::invalid("missing table name"))?
            .clone()
            .into();
        let name = name
            .replace(|c: char| !c.is_alphanumeric() && c != '-', "-")
            .to_lowercase();
        // We use our own logic to map tables to locations. This avoids conflicts
        // and avoids issues with special characters in table names.
        // Note that this is different from how Spark handles table locations
        // for the default catalog.
        Ok(format!(
            "{}{}{}-{}",
            self.config.default_warehouse_directory,
            object_store::path::DELIMITER,
            name,
            Uuid::new_v4()
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
