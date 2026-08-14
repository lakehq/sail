//! Python wrappers for catalog status types.

use pyo3::prelude::*;
use sail_common_datafusion::catalog::{DatabaseStatus, TableColumnStatus, TableKind, TableStatus};

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
    location: Option<String>,
    properties: Vec<(String, String)>,
    is_external: Option<bool>,
}

impl From<TableStatus> for PyTableStatus {
    fn from(status: TableStatus) -> Self {
        let (kind, format, columns, view_definition, comment, location, properties, is_external) =
            match status.kind {
                TableKind::Table {
                    columns,
                    comment,
                    location,
                    format,
                    properties,
                    is_external,
                    ..
                } => (
                    "table".to_string(),
                    Some(format),
                    columns,
                    None,
                    comment,
                    location,
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
            location,
            properties,
            is_external,
        }
    }
}
