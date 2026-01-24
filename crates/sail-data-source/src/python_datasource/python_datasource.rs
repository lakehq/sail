use std::sync::Arc;
#[cfg(feature = "python")]
use std::sync::Mutex;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
/// Core Python DataSource implementation using PyO3.
///
/// This module provides the bridge between Rust and Python DataSources, managing
/// the Python interpreter lifecycle and invoking Python DataSource methods.
use once_cell::sync::OnceCell;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyBytes;

#[cfg(feature = "python")]
use super::arrow_utils::py_schema_to_rust;
#[cfg(feature = "python")]
use super::error::{PythonDataSourceContext, PythonDataSourceError};
#[cfg(feature = "python")]
use super::executor::InputPartition;

/// Represents a Python-defined DataSource.
///
/// This struct holds the serialized Python DataSource and provides methods
/// to interact with it via PyO3.
///
/// # Caching Strategy
///
/// To reduce cloudpickle deserialization overhead, the datasource object is
/// cached after first deserialization. The cache uses `Py<PyAny>` which is
/// GIL-independent storage - the object stays on the Python heap and can be
/// accessed when the GIL is acquired.
pub struct PythonDataSource {
    /// Pickled Python DataSource command (serialized DataSource instance)
    command: Vec<u8>,
    /// DataSource name (cached for efficiency)
    name: String,
    /// Cached schema (lazily populated on first schema() call)
    schema: OnceCell<SchemaRef>,
    /// Cached deserialized Python datasource object.
    /// Uses Mutex<Option<Py<PyAny>>> for thread-safe lazy initialization.
    /// Py<PyAny> is Send+Sync when GIL is not held.
    #[cfg(feature = "python")]
    cached_datasource: Arc<Mutex<Option<Py<PyAny>>>>,
}

// Manual Clone implementation because Py<PyAny> clone requires GIL
impl Clone for PythonDataSource {
    fn clone(&self) -> Self {
        Self {
            command: self.command.clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            #[cfg(feature = "python")]
            cached_datasource: Arc::new(Mutex::new(None)), // Don't clone the cache, will re-deserialize
        }
    }
}

impl PythonDataSource {
    /// Create a new PythonDataSource from serialized command.
    ///
    /// # Arguments
    /// * `command` - Pickled Python DataSource instance
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
        #[cfg(not(feature = "python"))]
        {
            let _ = (command, python_ver);
            datafusion_common::exec_err!("Python support not enabled in this build")
        }

        #[cfg(feature = "python")]
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
                #[cfg(feature = "python")]
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
    #[cfg(feature = "python")]
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
    /// This calls the Python DataSource's `schema()` method and caches the result.
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
        #[cfg(not(feature = "python"))]
        {
            datafusion_common::exec_err!("Python support not enabled in this build")
        }

        #[cfg(feature = "python")]
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

                        // Schema should be a PyArrow Schema or DDL string
                        // Try PyArrow Schema first
                        if let Ok(schema) = py_schema_to_rust(py, &schema_obj) {
                            return Ok(schema);
                        }

                        // Try DDL string
                        let schema_str: String = schema_obj.extract().map_err(|e| {
                            PythonDataSourceError::SchemaError(format!(
                                "[{}::schema] schema() must return PyArrow Schema or DDL string: {}",
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

    /// Get the number of partitions for parallel reading.
    ///
    /// Calls the Python DataSource's `partitioning()` method.
    ///
    /// # Returns
    /// * `Result<usize>` - Number of partitions
    pub fn partition_count(&self) -> Result<usize> {
        #[cfg(not(feature = "python"))]
        {
            datafusion_common::exec_err!("Python support not enabled in this build")
        }

        #[cfg(feature = "python")]
        {
            let ctx = PythonDataSourceContext::new(&self.name, "partitioning");

            Python::attach(|py| {
                let ds = self.get_cached_datasource(py)?;

                // Call partitioning() method
                let partitions = ds
                    .call_method0("partitioning")
                    .map_err(|e| ctx.wrap_py_error(e))?;

                // Convert to list
                let partitions_list =
                    partitions.downcast::<pyo3::types::PyList>().map_err(|e| {
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

    /// Get input partitions for parallel reading.
    ///
    /// Calls the Python DataSource's reader().partitions() method
    /// and pickles each partition for distribution.
    ///
    /// # Returns
    /// * `Result<Vec<InputPartition>>` - List of partitions
    #[cfg(feature = "python")]
    pub fn get_partitions(&self, schema: &SchemaRef) -> Result<Vec<InputPartition>> {
        let ctx = PythonDataSourceContext::new(&self.name, "get_partitions");

        Python::attach(|py| {
            let ds = self.get_cached_datasource(py)?;

            // Get reader with schema
            let schema_obj = super::arrow_utils::rust_schema_to_py(py, schema)?;
            let reader = ds
                .call_method1("reader", (schema_obj,))
                .map_err(|e| ctx.wrap_py_error(e))?;

            // Get partitions
            let partitions = reader
                .call_method0("partitions")
                .map_err(|e| ctx.wrap_py_error(e))?;

            // Convert to list
            let partitions_list = partitions
                .downcast::<pyo3::types::PyList>()
                .map_err(|e| ctx.wrap_error(format!("partitions() must return a list: {}", e)))?;

            // Pickle each partition
            let cloudpickle = py.import("cloudpickle").map_err(|e| ctx.wrap_py_error(e))?;

            let mut result = Vec::with_capacity(partitions_list.len());
            for (i, partition) in partitions_list.iter().enumerate() {
                let pickled = cloudpickle
                    .call_method1("dumps", (&partition,))
                    .map_err(|e| {
                        ctx.wrap_error(format!("Failed to pickle partition {}: {}", i, e))
                    })?;

                let bytes: Vec<u8> = pickled.extract().map_err(|e| {
                    ctx.wrap_error(format!(
                        "Failed to extract pickled bytes for partition {}: {}",
                        i, e
                    ))
                })?;

                result.push(InputPartition {
                    partition_id: i,
                    data: bytes,
                });
            }

            log::debug!(
                "[{}::get_partitions] Created {} partitions",
                self.name,
                result.len()
            );

            Ok(result)
        })
    }

    /// Get input partitions (non-Python fallback).
    #[cfg(not(feature = "python"))]
    pub fn get_partitions(
        &self,
        _schema: &SchemaRef,
    ) -> Result<Vec<super::executor::InputPartition>> {
        datafusion_common::exec_err!("Python support not enabled in this build")
    }

    /// Validate Python version compatibility.
    ///
    /// Requires Python 3.9+ per RFC (entry_points API changes in 3.9).
    #[cfg(feature = "python")]
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

        // Require Python 3.9+ (entry_points API changed in 3.9)
        if major < 3 || (major == 3 && minor < 9) {
            return Err(PythonDataSourceError::VersionError(format!(
                "Python {} is not supported. Require Python 3.9+",
                version
            ))
            .into());
        }

        Ok(())
    }

    /// Deserialize the pickled DataSource from bytes.
    #[cfg(feature = "python")]
    fn deserialize_datasource<'py>(py: Python<'py>, command: &[u8]) -> Result<Bound<'py, PyAny>> {
        // Import cloudpickle
        let cloudpickle = py.import("cloudpickle").map_err(|e| {
            PythonDataSourceError::PythonError(format!("Failed to import cloudpickle: {}", e))
        })?;

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
    #[cfg(feature = "python")]
    fn parse_ddl_schema(ddl: &str) -> Result<SchemaRef> {
        use arrow_schema::Field;
        use datafusion::sql::sqlparser::ast::Statement;
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        // Wrap DDL in CREATE TABLE to parse
        let sql = format!("CREATE TABLE t ({})", ddl);
        let dialect = GenericDialect {};

        let statements = Parser::parse_sql(&dialect, &sql).map_err(|e| {
            PythonDataSourceError::SchemaError(format!("Failed to parse DDL schema: {}", e))
        })?;

        if statements.is_empty() {
            return Err(PythonDataSourceError::SchemaError("Empty DDL schema".to_string()).into());
        }

        // Extract column definitions from CREATE TABLE statement
        let columns = match &statements[0] {
            Statement::CreateTable(create) => &create.columns,
            _ => {
                return Err(PythonDataSourceError::SchemaError(
                    "Expected CREATE TABLE statement".to_string(),
                )
                .into())
            }
        };

        // Convert each column to an Arrow Field
        let fields: Vec<Field> = columns
            .iter()
            .map(sql_column_to_arrow_field)
            .collect::<Result<Vec<_>>>()?;

        if fields.is_empty() {
            return Err(
                PythonDataSourceError::SchemaError("No columns in DDL schema".to_string()).into(),
            );
        }

        Ok(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
    }
}

/// Convert SQL column definition to Arrow Field.
#[cfg(feature = "python")]
fn sql_column_to_arrow_field(
    col: &datafusion::sql::sqlparser::ast::ColumnDef,
) -> Result<arrow_schema::Field> {
    use arrow_schema::Field;

    let name = col.name.value.clone();
    let data_type = sql_type_to_arrow(&col.data_type)?;

    // Check for NOT NULL constraint
    let nullable = !col.options.iter().any(|opt| {
        matches!(
            opt.option,
            datafusion::sql::sqlparser::ast::ColumnOption::NotNull
        )
    });

    Ok(Field::new(name, data_type, nullable))
}

/// Convert SQL data type to Arrow DataType.
#[cfg(feature = "python")]
fn sql_type_to_arrow(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
) -> Result<arrow_schema::DataType> {
    use arrow_schema::{DataType, TimeUnit};
    use datafusion::sql::sqlparser::ast::DataType as SqlDataType;

    match sql_type {
        // Integer types
        SqlDataType::TinyInt(_) => Ok(DataType::Int8),
        SqlDataType::SmallInt(_) => Ok(DataType::Int16),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),
        SqlDataType::BigInt(_) => Ok(DataType::Int64),

        // Floating point types
        SqlDataType::Float(_) | SqlDataType::Real => Ok(DataType::Float32),
        SqlDataType::Double(_) | SqlDataType::DoublePrecision => Ok(DataType::Float64),

        // Boolean
        SqlDataType::Boolean => Ok(DataType::Boolean),

        // String types - Spark's STRING maps to Utf8
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => Ok(DataType::Utf8),

        // Binary
        SqlDataType::Binary(_) | SqlDataType::Varbinary(_) | SqlDataType::Blob(_) => {
            Ok(DataType::Binary)
        }

        // Date and time
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

        // Decimal (use default precision/scale if not specified)
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => {
            use datafusion::sql::sqlparser::ast::ExactNumberInfo;
            let (precision, scale) = match info {
                ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as i8),
                ExactNumberInfo::Precision(p) => (*p as u8, 0),
                ExactNumberInfo::None => (38, 10), // Default Spark precision/scale
            };
            Ok(DataType::Decimal128(precision, scale))
        }

        // Unsupported types - fall through to error
        other => Err(PythonDataSourceError::SchemaError(format!(
            "Unsupported SQL type in DDL schema: {:?}. Use PyArrow Schema for complex types.",
            other
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

#[cfg(all(test, feature = "python"))]
mod tests {
    use super::*;

    #[test]
    fn test_validate_python_version() {
        // Valid versions (3.9+)
        assert!(PythonDataSource::validate_python_version("3.9").is_ok());
        assert!(PythonDataSource::validate_python_version("3.10").is_ok());
        assert!(PythonDataSource::validate_python_version("3.11").is_ok());
        assert!(PythonDataSource::validate_python_version("3.12").is_ok());

        // Invalid versions
        assert!(PythonDataSource::validate_python_version("2.7").is_err());
        assert!(PythonDataSource::validate_python_version("3.6").is_err());
        assert!(PythonDataSource::validate_python_version("3.8").is_err()); // 3.8 no longer supported
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
