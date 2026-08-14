use arrow_schema::DataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableMode,
    CreateTableOptions, CreateViewColumnOptions, CreateViewOptions, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, Namespace,
};
use sail_catalog_hms::{HmsCatalogConfig, HmsCatalogProvider};
use sail_common::runtime::RuntimeHandle;

use crate::catalog::status::{PyDatabaseStatus, PyTableStatus};
use crate::catalog::to_py_error;
use crate::globals::GlobalState;

type PyColumnDefinition = (String, String, bool);

#[pyclass(name = "HmsCatalog")]
pub(super) struct PyHmsCatalog {
    provider: HmsCatalogProvider,
    runtime: RuntimeHandle,
}

#[pymethods]
impl PyHmsCatalog {
    #[new]
    #[pyo3(signature = (
        name,
        uris,
        *,
        thrift_transport=None,
        auth=None,
        kerberos_service_principal=None,
        min_sasl_qop=None,
        connect_timeout_secs=None,
    ))]
    fn new(
        py: Python<'_>,
        name: String,
        uris: Vec<String>,
        thrift_transport: Option<String>,
        auth: Option<String>,
        kerberos_service_principal: Option<String>,
        min_sasl_qop: Option<String>,
        connect_timeout_secs: Option<u64>,
    ) -> PyResult<Self> {
        let runtime = GlobalState::instance(py)?.runtime.handle();
        let provider = HmsCatalogProvider::new(
            name,
            HmsCatalogConfig {
                uris,
                thrift_transport,
                auth,
                kerberos_service_principal,
                min_sasl_qop,
                connect_timeout_secs,
            },
            runtime.clone(),
        )
        .map_err(to_py_error)?;
        Ok(Self { provider, runtime })
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.create_database(&database, options))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn get_database(&self, py: Python<'_>, database: Vec<String>) -> PyResult<PyDatabaseStatus> {
        let database = namespace(database)?;
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.get_database(&database))
        })
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.list_databases(prefix.as_ref()))
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.primary().block_on(
                self.provider
                    .drop_database(&database, DropDatabaseOptions { if_exists, cascade }),
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
        properties: Option<Vec<(String, String)>>,
        if_not_exists: bool,
        is_external: bool,
    ) -> PyResult<PyTableStatus> {
        let database = namespace(database)?;
        let columns = table_columns(columns)?;
        let options = CreateTableOptions {
            columns,
            comment,
            constraints: vec![],
            location,
            format: format.to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            mode: if if_not_exists {
                CreateTableMode::CreateIfNotExists
            } else {
                CreateTableMode::Create
            },
            properties: properties.unwrap_or_default(),
            is_external,
            is_write_precondition: false,
        };
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.create_table(&database, &table, options))
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.get_table(&database, table))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn list_tables(&self, py: Python<'_>, database: Vec<String>) -> PyResult<Vec<PyTableStatus>> {
        let database = namespace(database)?;
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.list_tables(&database))
        })
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.primary().block_on(self.provider.drop_table(
                &database,
                table,
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
        let columns = view_columns(columns)?;
        let options = CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace,
            comment,
            properties: properties.unwrap_or_default(),
        };
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.create_view(&database, &view, options))
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.get_view(&database, view))
        })
        .map(Into::into)
        .map_err(to_py_error)
    }

    fn list_views(&self, py: Python<'_>, database: Vec<String>) -> PyResult<Vec<PyTableStatus>> {
        let database = namespace(database)?;
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime
                .primary()
                .block_on(self.provider.list_views(&database))
        })
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
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.primary().block_on(self.provider.drop_view(
                &database,
                view,
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
        .map(|(name, data_type, nullable)| {
            Ok(CreateTableColumnOptions {
                name,
                data_type: parse_data_type(&data_type)?,
                nullable,
                comment: None,
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
        .map(|(name, data_type, nullable)| {
            Ok(CreateViewColumnOptions {
                name,
                data_type: parse_data_type(&data_type)?,
                nullable,
                comment: None,
            })
        })
        .collect()
}

fn parse_data_type(value: &str) -> PyResult<DataType> {
    match value.trim().to_ascii_lowercase().as_str() {
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
        other => Err(PyValueError::new_err(format!(
            "unsupported HMS test column data type: {other}"
        ))),
    }
}
