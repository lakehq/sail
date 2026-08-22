//! Python wrappers for catalog status types.

use pyo3::prelude::*;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort,
    DatabaseStatus, PartitionTransform, TableColumnStatus, TableKind, TableStatus,
};

type PyTableConstraintStatus = (String, Option<String>, Vec<String>);
type PyPartitionFieldStatus = (String, Option<String>);
type PyTableSortStatus = (String, bool);
type PyTableBucketStatus = (Vec<String>, usize);

#[derive(Clone)]
#[pyclass(name = "ColumnStatus", frozen, get_all, skip_from_py_object)]
pub(super) struct PyColumnStatus {
    name: String,
    data_type: String,
    nullable: bool,
    comment: Option<String>,
    default: Option<String>,
    generated_always_as: Option<String>,
    is_partition: bool,
    is_bucket: bool,
    is_cluster: bool,
}

impl From<TableColumnStatus> for PyColumnStatus {
    fn from(status: TableColumnStatus) -> Self {
        Self {
            name: status.name,
            data_type: status.data_type.to_string(),
            nullable: status.nullable,
            comment: status.comment,
            default: status.default,
            generated_always_as: status.generated_always_as,
            is_partition: status.is_partition,
            is_bucket: status.is_bucket,
            is_cluster: status.is_cluster,
        }
    }
}

#[pyclass(name = "DatabaseStatus", frozen, get_all)]
pub(super) struct PyDatabaseStatus {
    catalog: String,
    database: Vec<String>,
    comment: Option<String>,
    location: Option<String>,
    properties: Vec<(String, String)>,
}

impl From<DatabaseStatus> for PyDatabaseStatus {
    fn from(status: DatabaseStatus) -> Self {
        Self {
            catalog: status.catalog,
            database: status.database,
            comment: status.comment,
            location: status.location,
            properties: status.properties,
        }
    }
}

#[pyclass(name = "TableStatus", frozen, get_all)]
pub(super) struct PyTableStatus {
    catalog: Option<String>,
    database: Vec<String>,
    name: String,
    kind: String,
    format: Option<String>,
    columns: Vec<PyColumnStatus>,
    view_definition: Option<String>,
    comment: Option<String>,
    constraints: Vec<PyTableConstraintStatus>,
    location: Option<String>,
    partition_by: Vec<PyPartitionFieldStatus>,
    sort_by: Vec<PyTableSortStatus>,
    bucket_by: Option<PyTableBucketStatus>,
    properties: Vec<(String, String)>,
    is_external: Option<bool>,
}

impl From<TableStatus> for PyTableStatus {
    fn from(status: TableStatus) -> Self {
        let (
            kind,
            format,
            columns,
            view_definition,
            comment,
            constraints,
            location,
            partition_by,
            sort_by,
            bucket_by,
            properties,
            is_external,
        ) = match status.kind {
            TableKind::Table {
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                properties,
                is_external,
            } => (
                "table".to_string(),
                Some(format),
                columns,
                None,
                comment,
                constraints
                    .into_iter()
                    .map(table_constraint_status)
                    .collect(),
                location,
                partition_by
                    .into_iter()
                    .map(partition_field_status)
                    .collect(),
                sort_by.into_iter().map(table_sort_status).collect(),
                bucket_by.map(table_bucket_status),
                properties,
                Some(is_external),
            ),
            TableKind::View {
                definition,
                columns,
                comment,
                properties,
            } => (
                "view".to_string(),
                None,
                columns,
                Some(definition),
                comment,
                vec![],
                None,
                vec![],
                vec![],
                None,
                properties,
                None,
            ),
            TableKind::TemporaryView {
                columns,
                comment,
                properties,
                ..
            } => (
                "temporary_view".to_string(),
                None,
                columns,
                None,
                comment,
                vec![],
                None,
                vec![],
                vec![],
                None,
                properties,
                None,
            ),
            TableKind::GlobalTemporaryView {
                columns,
                comment,
                properties,
                ..
            } => (
                "global_temporary_view".to_string(),
                None,
                columns,
                None,
                comment,
                vec![],
                None,
                vec![],
                vec![],
                None,
                properties,
                None,
            ),
        };

        Self {
            catalog: status.catalog,
            database: status.database,
            name: status.name,
            kind,
            format,
            columns: columns.into_iter().map(Into::into).collect(),
            view_definition,
            comment,
            constraints,
            location,
            partition_by,
            sort_by,
            bucket_by,
            properties,
            is_external,
        }
    }
}

fn table_constraint_status(constraint: CatalogTableConstraint) -> PyTableConstraintStatus {
    match constraint {
        CatalogTableConstraint::Unique { name, columns } => ("unique".to_string(), name, columns),
        CatalogTableConstraint::PrimaryKey { name, columns } => {
            ("primary_key".to_string(), name, columns)
        }
    }
}

fn partition_field_status(field: CatalogPartitionField) -> PyPartitionFieldStatus {
    (
        field.column,
        field.transform.map(|transform| match transform {
            PartitionTransform::Identity => "identity".to_string(),
            PartitionTransform::Year => "year".to_string(),
            PartitionTransform::Month => "month".to_string(),
            PartitionTransform::Day => "day".to_string(),
            PartitionTransform::Hour => "hour".to_string(),
            PartitionTransform::Bucket(count) => format!("bucket({count})"),
            PartitionTransform::Truncate(width) => format!("truncate({width})"),
        }),
    )
}

fn table_sort_status(sort: CatalogTableSort) -> PyTableSortStatus {
    (sort.column, sort.ascending)
}

fn table_bucket_status(bucket: CatalogTableBucketBy) -> PyTableBucketStatus {
    (bucket.columns, bucket.num_buckets)
}
