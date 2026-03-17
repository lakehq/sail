use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::Constraints;
use datafusion_expr::expr::Sort;

use crate::catalog::{
    CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort, TableColumnStatus, TableKind,
    TableStatus,
};
use crate::datasource::{BucketBy, SourceInfo};

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct TableHandle {
    pub catalog: Option<String>,
    pub database: Vec<String>,
    pub name: String,
    pub columns: Vec<TableColumnStatus>,
    pub comment: Option<String>,
    pub constraints: Vec<CatalogTableConstraint>,
    pub location: Option<String>,
    pub format: String,
    pub partition_by: Vec<String>,
    pub sort_by: Vec<CatalogTableSort>,
    pub bucket_by: Option<CatalogTableBucketBy>,
    pub options: Vec<(String, String)>,
    pub properties: Vec<(String, String)>,
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
            kind => Err(TableStatus {
                catalog,
                database,
                name,
                kind,
            }),
        }
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
            self.columns
                .iter()
                .map(|column| column.field())
                .collect::<Vec<_>>(),
        )
    }

    pub fn with_columns(mut self, columns: Vec<TableColumnStatus>) -> Self {
        self.columns = columns;
        self
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
        if !format.is_empty() && !format.eq_ignore_ascii_case(&self.format) {
            return Err(format!(
                "the format '{}' does not match the table format '{}'",
                format, self.format
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
        options.push(self.options.iter().cloned().collect());
        options.extend(additional_options);
        SourceInfo {
            paths: self.location.iter().cloned().collect(),
            schema,
            constraints,
            partition_by: self.partition_by.clone(),
            bucket_by: self.bucket_by.clone().map(Into::into),
            sort_order: self.sort_by.clone().into_iter().map(Into::into).collect(),
            options,
        }
    }

    fn is_empty_or_equivalent_partitioning(&self, partition_by: &[String]) -> bool {
        partition_by.is_empty()
            || (partition_by.len() == self.partition_by.len()
                && partition_by
                    .iter()
                    .zip(self.partition_by.iter())
                    .all(|(left, right)| left.eq_ignore_ascii_case(right)))
    }

    fn is_empty_or_equivalent_bucketing(
        &self,
        bucket_by: &Option<BucketBy>,
        sort_by: &[Sort],
    ) -> bool {
        let bucket_by_match = match (bucket_by, &self.bucket_by) {
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
        let sort_by_match = match (sort_by, self.sort_by.as_slice()) {
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
