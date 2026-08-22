use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableMode,
    CreateTableOptions, CreateViewColumnOptions, CreateViewOptions, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, Namespace,
};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort,
    PartitionTransform,
};

use crate::catalog::status::{PyDatabaseStatus, PyTableStatus};
use crate::catalog::to_py_error;

type PyColumnDefinition = (String, String, bool, Option<String>);
type PyTableConstraint = (String, Option<String>, Vec<String>);
type PyPartitionField = (String, Option<String>);
type PyTableSort = (String, bool);
type PyTableBucket = (Vec<String>, usize);

#[pyclass(name = "CatalogProvider", subclass)]
pub(super) struct PyCatalogProvider {
    catalog: Arc<dyn CatalogProvider>,
    runtime: RuntimeHandle,
}

impl PyCatalogProvider {
    pub(super) fn new(catalog: Arc<dyn CatalogProvider>, runtime: RuntimeHandle) -> Self {
        Self { catalog, runtime }
    }
}

#[pymethods]
impl PyCatalogProvider {
    #[getter]
    fn name(&self) -> String {
        self.catalog.get_name().to_string()
    }

    #[pyo3(signature = (
        database,
        *,
        comment=None,
        location=None,
        if_not_exists=false,
        properties=None,
    ))]
    fn create_database(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        comment: Option<String>,
        location: Option<String>,
        if_not_exists: bool,
        properties: Option<Vec<(String, String)>>,
    ) -> PyResult<PyDatabaseStatus> {
        let database = namespace(database)?;
        let options = CreateDatabaseOptions {
            comment,
            location,
            if_not_exists,
            properties: properties.unwrap_or_default(),
        };
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(catalog.create_database(&database, options))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn get_database(&self, py: Python<'_>, database: Vec<String>) -> PyResult<PyDatabaseStatus> {
        let database = namespace(database)?;
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || runtime.primary().block_on(catalog.get_database(&database)))
            .map(Into::into)
            .map_err(to_py_error)
    }

    #[pyo3(signature = (prefix=None))]
    fn list_databases(
        &self,
        py: Python<'_>,
        prefix: Option<Vec<String>>,
    ) -> PyResult<Vec<PyDatabaseStatus>> {
        let prefix = prefix.map(namespace).transpose()?;
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(catalog.list_databases(prefix.as_ref()))
        })
        .map(|statuses| statuses.into_iter().map(Into::into).collect())
        .map_err(to_py_error)
    }

    #[pyo3(signature = (database, *, if_exists=false, cascade=false))]
    fn drop_database(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        if_exists: bool,
        cascade: bool,
    ) -> PyResult<()> {
        let database = namespace(database)?;
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(
                catalog.drop_database(&database, DropDatabaseOptions { if_exists, cascade }),
            )
        })
        .map_err(to_py_error)
    }

    #[pyo3(signature = (
        database,
        table,
        columns,
        *,
        format="parquet",
        location=None,
        comment=None,
        constraints=None,
        partition_by=None,
        sort_by=None,
        bucket_by=None,
        properties=None,
        if_not_exists=false,
        is_external=true,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn create_table(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        table: String,
        columns: Vec<PyColumnDefinition>,
        format: &str,
        location: Option<String>,
        comment: Option<String>,
        constraints: Option<Vec<PyTableConstraint>>,
        partition_by: Option<Vec<PyPartitionField>>,
        sort_by: Option<Vec<PyTableSort>>,
        bucket_by: Option<PyTableBucket>,
        properties: Option<Vec<(String, String)>>,
        if_not_exists: bool,
        is_external: bool,
    ) -> PyResult<PyTableStatus> {
        let database = namespace(database)?;
        let options = CreateTableOptions {
            columns: table_columns(columns)?,
            comment,
            constraints: table_constraints(constraints.unwrap_or_default())?,
            location,
            format: format.to_string(),
            partition_by: partition_fields(partition_by.unwrap_or_default())?,
            sort_by: sort_by
                .unwrap_or_default()
                .into_iter()
                .map(|(column, ascending)| CatalogTableSort { column, ascending })
                .collect(),
            bucket_by: bucket_by.map(|(columns, num_buckets)| CatalogTableBucketBy {
                columns,
                num_buckets,
            }),
            mode: if if_not_exists {
                CreateTableMode::CreateIfNotExists
            } else {
                CreateTableMode::Create
            },
            properties: properties.unwrap_or_default(),
            is_external,
            is_write_precondition: false,
        };
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(catalog.create_table(&database, &table, options))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn get_table(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        table: &str,
    ) -> PyResult<PyTableStatus> {
        let database = namespace(database)?;
        let table = table.to_string();
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(catalog.get_table(&database, &table))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn list_tables(&self, py: Python<'_>, database: Vec<String>) -> PyResult<Vec<PyTableStatus>> {
        let database = namespace(database)?;
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || runtime.primary().block_on(catalog.list_tables(&database)))
            .map(|statuses| statuses.into_iter().map(Into::into).collect())
            .map_err(to_py_error)
    }

    #[pyo3(signature = (database, table, *, if_exists=false, purge=false))]
    fn drop_table(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        table: &str,
        if_exists: bool,
        purge: bool,
    ) -> PyResult<()> {
        let database = namespace(database)?;
        let table = table.to_string();
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(catalog.drop_table(
                &database,
                &table,
                DropTableOptions { if_exists, purge },
            ))
        })
        .map_err(to_py_error)
    }

    #[pyo3(signature = (
        database,
        view,
        columns,
        definition,
        *,
        if_not_exists=false,
        replace=false,
        comment=None,
        properties=None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn create_view(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        view: String,
        columns: Vec<PyColumnDefinition>,
        definition: String,
        if_not_exists: bool,
        replace: bool,
        comment: Option<String>,
        properties: Option<Vec<(String, String)>>,
    ) -> PyResult<PyTableStatus> {
        let database = namespace(database)?;
        let options = CreateViewOptions {
            columns: view_columns(columns)?,
            definition,
            if_not_exists,
            replace,
            comment,
            properties: properties.unwrap_or_default(),
        };
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(catalog.create_view(&database, &view, options))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn get_view(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        view: &str,
    ) -> PyResult<PyTableStatus> {
        let database = namespace(database)?;
        let view = view.to_string();
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(catalog.get_view(&database, &view))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn list_views(&self, py: Python<'_>, database: Vec<String>) -> PyResult<Vec<PyTableStatus>> {
        let database = namespace(database)?;
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || runtime.primary().block_on(catalog.list_views(&database)))
            .map(|statuses| statuses.into_iter().map(Into::into).collect())
            .map_err(to_py_error)
    }

    #[pyo3(signature = (database, view, *, if_exists=false))]
    fn drop_view(
        &self,
        py: Python<'_>,
        database: Vec<String>,
        view: &str,
        if_exists: bool,
    ) -> PyResult<()> {
        let database = namespace(database)?;
        let view = view.to_string();
        let catalog = self.catalog.clone();
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(catalog.drop_view(
                &database,
                &view,
                DropViewOptions { if_exists },
            ))
        })
        .map_err(to_py_error)
    }
}

fn namespace(parts: Vec<String>) -> PyResult<Namespace> {
    Namespace::try_from(parts).map_err(to_py_error)
}

fn table_columns(columns: Vec<PyColumnDefinition>) -> PyResult<Vec<CreateTableColumnOptions>> {
    columns
        .into_iter()
        .map(|(name, data_type, nullable, comment)| {
            Ok(CreateTableColumnOptions {
                name,
                data_type: parse_data_type(&data_type)?,
                nullable,
                comment,
                default: None,
                generated_always_as: None,
                identity: None,
            })
        })
        .collect()
}

fn view_columns(columns: Vec<PyColumnDefinition>) -> PyResult<Vec<CreateViewColumnOptions>> {
    columns
        .into_iter()
        .map(|(name, data_type, nullable, comment)| {
            Ok(CreateViewColumnOptions {
                name,
                data_type: parse_data_type(&data_type)?,
                nullable,
                comment,
            })
        })
        .collect()
}

fn table_constraints(constraints: Vec<PyTableConstraint>) -> PyResult<Vec<CatalogTableConstraint>> {
    constraints
        .into_iter()
        .map(|(kind, name, columns)| {
            match kind
                .trim()
                .to_ascii_lowercase()
                .replace(['-', ' '], "_")
                .as_str()
            {
                "unique" => Ok(CatalogTableConstraint::Unique { name, columns }),
                "primary_key" | "primarykey" => {
                    Ok(CatalogTableConstraint::PrimaryKey { name, columns })
                }
                other => Err(PyValueError::new_err(format!(
                    "unsupported catalog table constraint: {other}"
                ))),
            }
        })
        .collect()
}

fn partition_fields(fields: Vec<PyPartitionField>) -> PyResult<Vec<CatalogPartitionField>> {
    fields
        .into_iter()
        .map(|(column, transform)| {
            Ok(CatalogPartitionField {
                column,
                transform: transform
                    .as_deref()
                    .map(parse_partition_transform)
                    .transpose()?,
            })
        })
        .collect()
}

fn parse_partition_transform(value: &str) -> PyResult<PartitionTransform> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "identity" => Ok(PartitionTransform::Identity),
        "year" => Ok(PartitionTransform::Year),
        "month" => Ok(PartitionTransform::Month),
        "day" => Ok(PartitionTransform::Day),
        "hour" => Ok(PartitionTransform::Hour),
        _ => {
            if let Some(argument) = value
                .strip_prefix("bucket(")
                .and_then(|value| value.strip_suffix(')'))
            {
                return argument
                    .parse()
                    .map(PartitionTransform::Bucket)
                    .map_err(|_| {
                        PyValueError::new_err(format!(
                            "invalid bucket partition transform: {value}"
                        ))
                    });
            }
            if let Some(argument) = value
                .strip_prefix("truncate(")
                .and_then(|value| value.strip_suffix(')'))
            {
                return argument
                    .parse()
                    .map(PartitionTransform::Truncate)
                    .map_err(|_| {
                        PyValueError::new_err(format!(
                            "invalid truncate partition transform: {value}"
                        ))
                    });
            }
            Err(PyValueError::new_err(format!(
                "unsupported catalog partition transform: {value}"
            )))
        }
    }
}

fn parse_data_type(value: &str) -> PyResult<DataType> {
    let value = value.trim();
    match value.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => Ok(DataType::Boolean),
        "int8" | "tinyint" => Ok(DataType::Int8),
        "int16" | "smallint" => Ok(DataType::Int16),
        "int32" | "int" | "integer" => Ok(DataType::Int32),
        "int64" | "bigint" | "long" => Ok(DataType::Int64),
        "float32" | "float" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),
        "utf8" | "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date32" | "date" => Ok(DataType::Date32),
        "timestamp" | "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        _ => value.parse().map_err(|_| {
            PyValueError::new_err(format!("unsupported catalog column data type: {value}"))
        }),
    }
}
