use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::Constraints;
use datafusion_expr::expr::Sort;

use crate::catalog::{
    CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort, TableColumnStatus, TableKind,
    TableStatus,
};
use crate::datasource::{BucketBy, SourceInfo, SourceTarget};

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct TableHandle {
    pub catalog: Option<String>,
    pub database: Vec<String>,
    pub name: String,
    schema_data: Arc<TableSchemaData>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
struct TableSchemaData {
    columns: Vec<TableColumnStatus>,
    comment: Option<String>,
    constraints: Vec<CatalogTableConstraint>,
    location: Option<String>,
    format: String,
    partition_by: Vec<String>,
    sort_by: Vec<CatalogTableSort>,
    bucket_by: Option<CatalogTableBucketBy>,
    options: Vec<(String, String)>,
    properties: Vec<(String, String)>,
}

impl TableHandle {
    pub fn from_status(status: TableStatus) -> Result<Self, TableStatus> {
        let TableStatus {
            catalog,
            database,
            name,
            kind,
        } = status;
        match kind {
            TableKind::Table {
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                options,
                properties,
            } => Ok(Self {
                catalog,
                database,
                name,
                schema_data: Arc::new(TableSchemaData {
                    columns,
                    comment,
                    constraints,
                    location,
                    format,
                    partition_by,
                    sort_by,
                    bucket_by,
                    options,
                    properties,
                }),
            }),
            kind => Err(TableStatus {
                catalog,
                database,
                name,
                kind,
            }),
        }
    }

    pub fn columns(&self) -> &[TableColumnStatus] {
        &self.schema_data.columns
    }

    pub fn comment(&self) -> Option<&str> {
        self.schema_data.comment.as_deref()
    }

    pub fn constraints(&self) -> &[CatalogTableConstraint] {
        &self.schema_data.constraints
    }

    pub fn location(&self) -> Option<&str> {
        self.schema_data.location.as_deref()
    }

    pub fn format(&self) -> &str {
        &self.schema_data.format
    }

    pub fn partition_by(&self) -> &[String] {
        &self.schema_data.partition_by
    }

    pub fn sort_by(&self) -> &[CatalogTableSort] {
        &self.schema_data.sort_by
    }

    pub fn bucket_by(&self) -> Option<&CatalogTableBucketBy> {
        self.schema_data.bucket_by.as_ref()
    }

    pub fn options(&self) -> &[(String, String)] {
        &self.schema_data.options
    }

    pub fn properties(&self) -> &[(String, String)] {
        &self.schema_data.properties
    }

    pub fn full_name(&self) -> Vec<String> {
        self.catalog
            .iter()
            .cloned()
            .chain(self.database.iter().cloned())
            .chain(std::iter::once(self.name.clone()))
            .collect()
    }

    pub fn schema(&self) -> Schema {
        Schema::new(
            self.columns()
                .iter()
                .map(|column| column.field())
                .collect::<Vec<_>>(),
        )
    }

    pub fn with_columns(&self, columns: Vec<TableColumnStatus>) -> Self {
        let mut schema_data = self.schema_data.as_ref().clone();
        schema_data.columns = columns;
        Self {
            catalog: self.catalog.clone(),
            database: self.database.clone(),
            name: self.name.clone(),
            schema_data: Arc::new(schema_data),
        }
    }

    pub fn validate_write_layout(
        &self,
        partition_by: &[String],
        bucket_by: &Option<BucketBy>,
        sort_by: &[Sort],
        format: &str,
    ) -> Result<(), String> {
        if !self.is_empty_or_equivalent_partitioning(partition_by) {
            return Err(
                "cannot specify a different partitioning when writing to an existing table"
                    .to_string(),
            );
        }
        if !self.is_empty_or_equivalent_bucketing(bucket_by, sort_by) {
            return Err(
                "cannot specify a different bucketing when writing to an existing table"
                    .to_string(),
            );
        }
        if !format.is_empty() && !format.eq_ignore_ascii_case(self.format()) {
            return Err(format!(
                "the format '{}' does not match the table format '{}'",
                format,
                self.format()
            ));
        }
        Ok(())
    }

    pub fn to_source_info(
        &self,
        schema: Option<Schema>,
        constraints: Constraints,
        additional_options: Vec<HashMap<String, String>>,
    ) -> SourceInfo {
        let mut options = Vec::with_capacity(additional_options.len() + 1);
        options.push(self.options().iter().cloned().collect());
        options.extend(additional_options);
        SourceInfo {
            target: SourceTarget::Table(self.clone()),
            schema,
            constraints,
            options,
        }
    }

    fn is_empty_or_equivalent_partitioning(&self, partition_by: &[String]) -> bool {
        partition_by.is_empty()
            || (partition_by.len() == self.partition_by().len()
                && partition_by
                    .iter()
                    .zip(self.partition_by().iter())
                    .all(|(left, right)| left.eq_ignore_ascii_case(right)))
    }

    fn is_empty_or_equivalent_bucketing(
        &self,
        bucket_by: &Option<BucketBy>,
        sort_by: &[Sort],
    ) -> bool {
        let bucket_by_match = match (bucket_by, self.bucket_by()) {
            (None, _) => true,
            (Some(left), Some(right)) => {
                left.num_buckets == right.num_buckets
                    && left.columns.len() == right.columns.len()
                    && left
                        .columns
                        .iter()
                        .zip(&right.columns)
                        .all(|(a, b)| a.eq_ignore_ascii_case(b))
            }
            (Some(_), None) => false,
        };
        let sort_by_match = match (sort_by, self.sort_by()) {
            ([], _) => true,
            (_, []) => false,
            (left, right) => {
                left.len() == right.len()
                    && left.iter().zip(right.iter()).all(|(a, b)| {
                        let Sort {
                            expr: datafusion_expr::Expr::Column(column),
                            asc,
                            nulls_first: _,
                        } = a
                        else {
                            return false;
                        };
                        column.name.eq_ignore_ascii_case(&b.column) && *asc == b.ascending
                    })
            }
        };
        bucket_by_match && sort_by_match
    }
}
