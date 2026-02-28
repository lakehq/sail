use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use datafusion_common::Result;
/// Core Python data source implementation using PyO3.
///
/// This module provides the bridge between Rust and Python data sources, managing
/// the Python interpreter lifecycle and invoking Python data source methods.
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::arrow_utils::py_schema_to_rust;
use super::error::{import_cloudpickle, PythonDataSourceContext, PythonDataSourceError};

/// Represents a Python data source.
///
/// This struct holds the serialized Python data source and provides methods
/// to interact with it via PyO3.
///
/// # Caching Strategy
///
/// To reduce cloudpickle deserialization overhead, the datasource object is
/// cached after first deserialization. The cache uses `Py<PyAny>` which is
/// GIL-independent storage - the object stays on the Python heap and can be
/// accessed when the GIL is acquired.
pub struct PythonDataSource {
    /// Pickled Python data source command (serialized DataSource instance)
    command: Vec<u8>,
    /// DataSource name (cached for efficiency)
    name: String,
    /// Cached schema (lazily populated on first schema() call)
    schema: OnceCell<SchemaRef>,
    /// Cached deserialized Python datasource object.
    /// Uses Mutex<Option<Py<PyAny>>> for thread-safe lazy initialization.
    /// Py<PyAny> is Send+Sync when GIL is not held.
    cached_datasource: Arc<Mutex<Option<Py<PyAny>>>>,
}

// Manual Clone implementation because Py<PyAny> clone requires GIL
impl Clone for PythonDataSource {
    fn clone(&self) -> Self {
        Self {
            command: self.command.clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            cached_datasource: Arc::new(Mutex::new(None)), // Don't clone the cache, will re-deserialize
        }
    }
}

impl PythonDataSource {
    /// Create a new PythonDataSource from serialized command.
    ///
    /// # Arguments
    /// * `command` - Pickled Python data source instance
    /// * `python_ver` - Python version string (e.g., "3.11")
    ///
    /// # Returns
    /// * `Result<Self>` - New PythonDataSource instance
    ///
    /// # Errors
    /// Returns an error if:
    /// - Python version is incompatible
    /// - Command deserialization fails
    /// - DataSource name() method fails
    pub fn new(command: Vec<u8>, python_ver: String) -> Result<Self> {
        {
            // Validate Python version compatibility
            Self::validate_python_version(&python_ver)?;

            // Get DataSource name via Python call
            let name = Python::attach(|py| {
                let ds = Self::deserialize_datasource(py, &command)?;
                let name_obj = ds.call_method0("name").map_err(|e| {
                    PythonDataSourceError::PythonError(format!("Failed to call name(): {}", e))
                })?;
                let name_str: String = name_obj.extract().map_err(|e| {
                    PythonDataSourceError::PythonError(format!(
                        "name() must return a string: {}",
                        e
                    ))
                })?;
                Ok::<String, PythonDataSourceError>(name_str)
            })?;

            Ok(Self {
                command,
                name,
                schema: OnceCell::new(),
                cached_datasource: Arc::new(Mutex::new(None)),
            })
        }
    }

    /// Get or create the cached deserialized datasource.
    ///
    /// This method reduces cloudpickle overhead by caching the deserialized
    /// Python object. The first call deserializes via cloudpickle, subsequent
    /// calls return the cached object.
    ///
    /// # Arguments
    /// * `py` - Python GIL token
    ///
    /// # Returns
    /// * `Result<Bound<'py, PyAny>>` - Bound reference to the cached datasource
    fn get_cached_datasource<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let mut cache = self.cached_datasource.lock().map_err(|e| {
            PythonDataSourceError::PythonError(format!("Failed to acquire cache lock: {}", e))
        })?;

        if let Some(ref cached) = *cache {
            // Return cached object bound to current GIL scope
            return Ok(cached.bind(py).clone());
        }

        // Cache miss - deserialize and store
        let ds = Self::deserialize_datasource(py, &self.command)?;

        // Store unbound reference in cache
        *cache = Some(ds.clone().unbind());

        Ok(ds)
    }

    /// Get the DataSource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the schema of the DataSource.
    ///
    /// This calls the Python data source's `schema()` method and caches the result.
    ///
    /// # Returns
    /// * `Result<SchemaRef>` - Arrow schema
    ///
    /// # Errors
    /// Returns an error if:
    /// - Python schema() method fails
    /// - Schema string parsing fails
    /// - Schema conversion fails
    pub fn schema(&self) -> Result<SchemaRef> {
        {
            // Use OnceLock for thread-safe lazy initialization
            self.schema
                .get_or_try_init(|| {
                    let ctx = PythonDataSourceContext::new(&self.name, "schema");

                    // Call Python schema() method using cached datasource
                    Python::attach(|py| {
                        let ds = self.get_cached_datasource(py)?;

                        // Call schema() method
                        let schema_obj = ds
                            .call_method0("schema")
                            .map_err(|e| ctx.wrap_py_error(e))?;

                        // Schema should be a PyArrow Schema, PySpark DataType, or DDL string
                        // Try PyArrow Schema first
                        if let Ok(schema) = py_schema_to_rust(py, &schema_obj) {
                            return Ok(schema);
                        }

                        // Try PySpark DataType (StructType)
                        if let Some(schema) = self.try_pyspark_data_type_schema(py, &schema_obj, &ctx)? {
                            return Ok(schema);
                        }

                        // Try DDL string
                        let schema_str: String = schema_obj.extract().map_err(|e| {
                            PythonDataSourceError::SchemaError(format!(
                                "[{}::schema] schema() must return PyArrow Schema, PySpark DataType, or DDL string: {}",
                                self.name, e
                            ))
                        })?;

                        // Parse DDL string to Arrow schema
                        Self::parse_ddl_schema(&schema_str)
                    })
                })
                .cloned()
        }
    }

    fn try_pyspark_data_type_schema(
        &self,
        py: Python<'_>,
        schema_obj: &Bound<'_, PyAny>,
        ctx: &PythonDataSourceContext,
    ) -> Result<Option<SchemaRef>> {
        let types_module = match py.import("pyspark.sql.types") {
            Ok(module) => module,
            Err(_) => return Ok(None),
        };
        let data_type_class = types_module
            .getattr("DataType")
            .map_err(|e| ctx.wrap_py_error(e))?;

        let is_data_type = schema_obj
            .is_instance(&data_type_class)
            .map_err(|e| ctx.wrap_py_error(e))?;
        if !is_data_type {
            return Ok(None);
        }

        let pandas_types = py
            .import("pyspark.sql.pandas.types")
            .map_err(|e| ctx.wrap_py_error(e))?;
        let to_arrow_type = pandas_types
            .getattr("to_arrow_type")
            .map_err(|e| ctx.wrap_py_error(e))?;
        // TODO: pass options such as `prefers_large_types` as specified for the session
        let arrow_type = to_arrow_type
            .call1((schema_obj,))
            .map_err(|e| ctx.wrap_py_error(e))?;

        let pa_types = py
            .import("pyarrow.types")
            .map_err(|e| ctx.wrap_py_error(e))?;
        let is_struct: bool = pa_types
            .getattr("is_struct")
            .map_err(|e| ctx.wrap_py_error(e))?
            .call1((arrow_type.clone(),))
            .map_err(|e| ctx.wrap_py_error(e))?
            .extract()
            .map_err(|e| ctx.wrap_py_error(e))?;

        if !is_struct {
            return Err(PythonDataSourceError::SchemaError(format!(
                "[{}::schema] schema() DataType must be StructType to be used as a schema",
                self.name
            ))
            .into());
        }

        let builtins = py.import("builtins").map_err(|e| ctx.wrap_py_error(e))?;
        let list_fn = builtins.getattr("list").map_err(|e| ctx.wrap_py_error(e))?;
        let fields_list = list_fn
            .call1((arrow_type,))
            .map_err(|e| ctx.wrap_py_error(e))?;
        let pa = py.import("pyarrow").map_err(|e| ctx.wrap_py_error(e))?;
        let pa_schema = pa
            .getattr("schema")
            .map_err(|e| ctx.wrap_py_error(e))?
            .call1((fields_list,))
            .map_err(|e| ctx.wrap_py_error(e))?;

        Ok(Some(py_schema_to_rust(py, &pa_schema)?))
    }

    /// Get the number of partitions for parallel reading.
    ///
    /// Calls the Python data source's `partitioning()` method.
    ///
    /// # Returns
    /// * `Result<usize>` - Number of partitions
    pub fn partition_count(&self) -> Result<usize> {
        {
            let ctx = PythonDataSourceContext::new(&self.name, "partitioning");

            Python::attach(|py| {
                let ds = self.get_cached_datasource(py)?;

                // Call partitioning() method
                let partitions = ds
                    .call_method0("partitioning")
                    .map_err(|e| ctx.wrap_py_error(e))?;

                // Convert to list
                let partitions_list = partitions.cast::<pyo3::types::PyList>().map_err(|e| {
                    ctx.wrap_error(format!("partitioning() must return a list: {}", e))
                })?;

                Ok(partitions_list.len())
            })
        }
    }

    /// Get the raw command bytes (pickled DataSource).
    ///
    /// This is used by PythonDataSourceExec to pass the datasource
    /// to the stream for execution.
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    /// Validate Python version compatibility.
    ///
    /// Requires Python 3.10+.
    fn validate_python_version(version: &str) -> Result<()> {
        // Parse version string (e.g., "3.11" -> major=3, minor=11)
        let parts: Vec<&str> = version.split('.').collect();
        if parts.len() < 2 {
            return Err(PythonDataSourceError::VersionError(format!(
                "Invalid Python version string: {}",
                version
            ))
            .into());
        }

        let major: u32 = parts[0].parse().map_err(|_| {
            PythonDataSourceError::VersionError(format!("Invalid major version: {}", parts[0]))
        })?;

        let minor: u32 = parts[1].parse().map_err(|_| {
            PythonDataSourceError::VersionError(format!("Invalid minor version: {}", parts[1]))
        })?;

        // Require Python 3.10+
        if major < 3 || (major == 3 && minor < 10) {
            return Err(PythonDataSourceError::VersionError(format!(
                "Python {} is not supported. Require Python 3.10+",
                version
            ))
            .into());
        }

        Ok(())
    }

    /// Deserialize the pickled DataSource from bytes.
    fn deserialize_datasource<'py>(py: Python<'py>, command: &[u8]) -> Result<Bound<'py, PyAny>> {
        // Import cloudpickle with helpful error message
        let cloudpickle = import_cloudpickle(py)?;

        // Deserialize
        let py_bytes = PyBytes::new(py, command);
        let datasource = cloudpickle
            .call_method1("loads", (py_bytes,))
            .map_err(|e| {
                PythonDataSourceError::PythonError(format!(
                    "Failed to deserialize DataSource: {}",
                    e
                ))
            })?;

        Ok(datasource)
    }

    /// Parse DDL schema string to Arrow Schema.
    ///
    /// DDL format: "id INT, name STRING, age INT"
    fn parse_ddl_schema(ddl: &str) -> Result<SchemaRef> {
        use sail_common::spec;
        use sail_sql_analyzer::data_type::from_ast_data_type;
        use sail_sql_analyzer::parser as sail_parser;

        let ddl = ddl.trim();
        let type_str = if ddl
            .get(..6)
            .is_some_and(|p| p.eq_ignore_ascii_case("struct"))
            && ddl.get(6..).is_some_and(|p| p.starts_with('<'))
            && ddl.ends_with('>')
        {
            ddl.to_string()
        } else {
            format!("STRUCT<{ddl}>")
        };

        let ast = sail_parser::parse_data_type(&type_str).map_err(|e| {
            PythonDataSourceError::SchemaError(format!("Failed to parse DDL schema '{ddl}': {e}"))
        })?;
        let spec_dt = from_ast_data_type(ast).map_err(|e| {
            PythonDataSourceError::SchemaError(format!("Failed to analyze DDL schema '{ddl}': {e}"))
        })?;
        let spec::DataType::Struct { fields } = spec_dt else {
            return Err(PythonDataSourceError::SchemaError(format!(
                "Expected STRUCT schema, got: {spec_dt:?}"
            ))
            .into());
        };

        if fields.is_empty() {
            return Err(
                PythonDataSourceError::SchemaError("No columns in DDL schema".to_string()).into(),
            );
        }

        let arrow_fields: Vec<arrow_schema::Field> = fields
            .iter()
            .map(|f| {
                let dt = spec_data_type_to_arrow(&f.data_type)?;
                Ok(arrow_schema::Field::new(f.name.clone(), dt, f.nullable))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(std::sync::Arc::new(arrow_schema::Schema::new(arrow_fields)))
    }
}

/// Convert a `spec::DataType` to an Arrow `DataType` for DDL schema parsing.
fn spec_data_type_to_arrow(dt: &sail_common::spec::DataType) -> Result<arrow_schema::DataType> {
    use arrow_schema::{DataType, TimeUnit};
    use sail_common::spec::DataType as SDT;

    match dt {
        SDT::Null => Ok(DataType::Null),
        SDT::Boolean => Ok(DataType::Boolean),
        SDT::Int8 => Ok(DataType::Int8),
        SDT::Int16 => Ok(DataType::Int16),
        SDT::Int32 => Ok(DataType::Int32),
        SDT::Int64 => Ok(DataType::Int64),
        SDT::Float32 => Ok(DataType::Float32),
        SDT::Float64 => Ok(DataType::Float64),
        SDT::Binary | SDT::ConfiguredBinary => Ok(DataType::Binary),
        SDT::Utf8 | SDT::ConfiguredUtf8 { .. } => Ok(DataType::Utf8),
        SDT::Date32 => Ok(DataType::Date32),
        SDT::Timestamp { .. } => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        SDT::Decimal128 { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        other => Err(PythonDataSourceError::SchemaError(format!(
            "Unsupported type in DDL schema: {other:?}. Use PyArrow Schema for complex types.",
        ))
        .into()),
    }
}

impl std::fmt::Debug for PythonDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonDataSource")
            .field("name", &self.name)
            .field("command_len", &self.command.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_python_version() {
        // Valid versions (3.10+)
        assert!(PythonDataSource::validate_python_version("3.10").is_ok());
        assert!(PythonDataSource::validate_python_version("3.11").is_ok());
        assert!(PythonDataSource::validate_python_version("3.12").is_ok());

        // Invalid versions
        assert!(PythonDataSource::validate_python_version("2.7").is_err());
        assert!(PythonDataSource::validate_python_version("3.6").is_err());
        assert!(PythonDataSource::validate_python_version("3.8").is_err());
        assert!(PythonDataSource::validate_python_version("3.9").is_err());
        assert!(PythonDataSource::validate_python_version("invalid").is_err());
    }

    #[test]
    fn test_parse_ddl_schema() -> Result<()> {
        // Basic types
        let schema = PythonDataSource::parse_ddl_schema("id INT, name STRING")?;
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Utf8);

        // More types
        let schema = PythonDataSource::parse_ddl_schema("a BIGINT, b DOUBLE, c BOOLEAN, d DATE")?;
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
        assert_eq!(
            schema.field(1).data_type(),
            &arrow_schema::DataType::Float64
        );
        assert_eq!(
            schema.field(2).data_type(),
            &arrow_schema::DataType::Boolean
        );
        assert_eq!(schema.field(3).data_type(), &arrow_schema::DataType::Date32);
        Ok(())
    }
}
