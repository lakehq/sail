/// Core Python DataSource implementation using PyO3.
///
/// This module provides the bridge between Rust and Python DataSources, managing
/// the Python interpreter lifecycle and invoking Python DataSource methods.

use arrow_schema::SchemaRef;
use datafusion_common::Result;
#[cfg(feature = "python")]
use super::arrow_utils::py_schema_to_rust;
#[cfg(feature = "python")]
use super::error::PythonDataSourceError;
#[cfg(feature = "python")]
use super::executor::InputPartition;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyBytes;

/// Represents a Python-defined DataSource.
///
/// This struct holds the serialized Python DataSource and provides methods
/// to interact with it via PyO3.
#[derive(Clone)]
pub struct PythonDataSource {
    /// Pickled Python DataSource command (serialized DataSource instance)
    command: Vec<u8>,
    /// Python version string for compatibility checking
    python_ver: String,
    /// DataSource name (cached for efficiency)
    name: String,
    /// Cached schema (populated on first schema() call)
    schema: Option<SchemaRef>,
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
                python_ver,
                name,
                schema: None,
            })
        }
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
    pub fn schema(&mut self) -> Result<SchemaRef> {
        #[cfg(not(feature = "python"))]
        {
            datafusion_common::exec_err!("Python support not enabled in this build")
        }

        #[cfg(feature = "python")]
        {
            // Return cached schema if available
            if let Some(ref schema) = self.schema {
                return Ok(schema.clone());
            }

            // Call Python schema() method
            let schema = Python::attach(|py| {
                let ds = Self::deserialize_datasource(py, &self.command)?;

                // Call schema() method
                let schema_obj = ds.call_method0("schema").map_err(|e| {
                    PythonDataSourceError::PythonError(format!("Failed to call schema(): {}", e))
                })?;

                // Schema should be a PyArrow Schema or DDL string
                // Try PyArrow Schema first
                if let Ok(schema) = py_schema_to_rust(py, &schema_obj) {
                    return Ok(schema);
                }

                // Try DDL string
                let schema_str: String = schema_obj.extract().map_err(|e| {
                    PythonDataSourceError::SchemaError(format!(
                        "schema() must return PyArrow Schema or DDL string: {}",
                        e
                    ))
                })?;

                // Parse DDL string to Arrow schema
                Self::parse_ddl_schema(&schema_str)
            })?;

            // Cache the schema
            self.schema = Some(schema.clone());

            Ok(schema)
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
            Python::attach(|py| {
                let ds = Self::deserialize_datasource(py, &self.command)?;

                // Call partitioning() method
                let partitions = ds.call_method0("partitioning").map_err(|e| {
                    PythonDataSourceError::PythonError(format!(
                        "Failed to call partitioning(): {}",
                        e
                    ))
                })?;

                // Convert to list
                let partitions_list = partitions.downcast::<pyo3::types::PyList>().map_err(|e| {
                    PythonDataSourceError::PythonError(format!(
                        "partitioning() must return a list: {}",
                        e
                    ))
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
        Python::attach(|py| {
            let ds = Self::deserialize_datasource(py, &self.command)?;

            // Get reader with schema
            let schema_obj = super::arrow_utils::rust_schema_to_py(py, schema)?;
            let reader = ds.call_method1("reader", (schema_obj,)).map_err(|e| {
                PythonDataSourceError::PythonError(format!("Failed to call reader(): {}", e))
            })?;

            // Get partitions
            let partitions = reader.call_method0("partitions").map_err(|e| {
                PythonDataSourceError::PythonError(format!(
                    "Failed to call partitions(): {}",
                    e
                ))
            })?;

            // Convert to list
            let partitions_list = partitions.downcast::<pyo3::types::PyList>().map_err(|e| {
                PythonDataSourceError::PythonError(format!(
                    "partitions() must return a list: {}",
                    e
                ))
            })?;

            // Pickle each partition
            let cloudpickle = py.import("cloudpickle").map_err(|e| {
                PythonDataSourceError::PythonError(format!("Failed to import cloudpickle: {}", e))
            })?;

            let mut result = Vec::with_capacity(partitions_list.len());
            for (i, partition) in partitions_list.iter().enumerate() {
                let pickled = cloudpickle
                    .call_method1("dumps", (&partition,))
                    .map_err(|e| {
                        PythonDataSourceError::PythonError(format!(
                            "Failed to pickle partition: {}",
                            e
                        ))
                    })?;

                let bytes: Vec<u8> = pickled.extract().map_err(|e| {
                    PythonDataSourceError::PythonError(format!(
                        "Failed to extract pickled bytes: {}",
                        e
                    ))
                })?;

                result.push(InputPartition {
                    partition_id: i,
                    data: bytes,
                });
            }

            Ok(result)
        })
    }

    /// Get input partitions (non-Python fallback).
    #[cfg(not(feature = "python"))]
    pub fn get_partitions(&self, _schema: &SchemaRef) -> Result<Vec<super::executor::InputPartition>> {
        datafusion_common::exec_err!("Python support not enabled in this build")
    }

    /// Validate Python version compatibility.
    ///
    /// Currently accepts Python 3.8+.
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

        // Require Python 3.8+
        if major < 3 || (major == 3 && minor < 8) {
            return Err(PythonDataSourceError::VersionError(format!(
                "Python {} is not supported. Require Python 3.8+",
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
        // Use DataFusion's SQL parser for DDL schema parsing
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        
        // Wrap DDL in CREATE TABLE to parse
        let sql = format!("CREATE TABLE t ({})", ddl);
        let dialect = GenericDialect {};
        
        let statements = Parser::parse_sql(&dialect, &sql).map_err(|e| {
            PythonDataSourceError::SchemaError(format!("Failed to parse DDL schema: {}", e))
        })?;

        if statements.is_empty() {
            return Err(
                PythonDataSourceError::SchemaError("Empty DDL schema".to_string()).into(),
            );
        }

        // Extract column definitions from parsed statement
        // For now, return a simple error - full DDL parsing will be implemented
        // when we integrate with DataFusion's schema parser
        Err(PythonDataSourceError::SchemaError(
            "DDL schema parsing not yet implemented. Please return PyArrow Schema from schema() method".to_string()
        ).into())
    }
}

impl std::fmt::Debug for PythonDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonDataSource")
            .field("name", &self.name)
            .field("python_ver", &self.python_ver)
            .field("command_len", &self.command.len())
            .finish()
    }
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use super::*;

    #[test]
    fn test_validate_python_version() {
        // Valid versions
        assert!(PythonDataSource::validate_python_version("3.8").is_ok());
        assert!(PythonDataSource::validate_python_version("3.11").is_ok());
        assert!(PythonDataSource::validate_python_version("3.12").is_ok());

        // Invalid versions
        assert!(PythonDataSource::validate_python_version("2.7").is_err());
        assert!(PythonDataSource::validate_python_version("3.6").is_err());
        assert!(PythonDataSource::validate_python_version("invalid").is_err());
    }
}
