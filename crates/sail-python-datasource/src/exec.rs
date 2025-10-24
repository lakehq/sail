// SPDX-License-Identifier: Apache-2.0

//! Python ExecutionPlan - calls Python code to read partitions.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_pyarrow::PyArrowType;
use datafusion::common::{Result as DFResult, Statistics};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_physical_expr::EquivalenceProperties;
use futures::{stream, Stream, StreamExt};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value as JsonValue;

use crate::error::{PythonDataSourceError, Result};

/// Python ExecutionPlan - executes Python code to read data
#[derive(Debug, Clone)]
pub struct PythonExec {
    /// Python module name
    module: String,
    /// Python class name
    class: String,
    /// Arrow schema
    schema: SchemaRef,
    /// Partition specifications (JSON-serializable)
    partitions: Vec<JsonValue>,
    /// Data source options
    options: JsonValue,
    /// Execution properties
    properties: PlanProperties,
}

impl PythonExec {
    pub fn new(
        module: String,
        class: String,
        schema: SchemaRef,
        partitions: Vec<JsonValue>,
        options: JsonValue,
    ) -> Self {
        let num_partitions = partitions.len();

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            module,
            class,
            schema,
            partitions,
            options,
            properties,
        }
    }

    /// Return the fully qualified Python module name.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Return the Python class name.
    pub fn class(&self) -> &str {
        &self.class
    }

    /// Return the partition specifications.
    pub fn partitions(&self) -> &[JsonValue] {
        &self.partitions
    }

    /// Return the datasource options.
    pub fn options(&self) -> &JsonValue {
        &self.options
    }
}

impl DisplayAs for PythonExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PythonExec: module={}, class={}, partitions={}",
            self.module,
            self.class,
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for PythonExec {
    fn name(&self) -> &str {
        "PythonExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::common::DataFusionError::Internal(
                "PythonExec should have no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partition_spec = self.partitions.get(partition).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "Invalid partition index: {}",
                partition
            ))
        })?;

        // Read partition from Python
        let batches = read_partition_from_python(
            &self.module,
            &self.class,
            partition_spec,
            &self.options,
            &self.schema,
        )
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        // Create stream from batches
        let schema = self.schema.clone();
        let stream = stream::iter(batches.into_iter().map(Ok));

        Ok(Box::pin(PythonRecordBatchStream {
            schema,
            inner: Box::pin(stream),
        }))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Read a partition by calling Python code
fn read_partition_from_python(
    module: &str,
    class: &str,
    partition_spec: &JsonValue,
    options: &JsonValue,
    _schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    Python::with_gil(|py| {
        // Import Python module
        let py_module = py.import(module).map_err(|e| {
            PythonDataSourceError::ImportError(format!("Failed to import {}: {}", module, e))
        })?;

        // Get class
        let py_class = py_module.getattr(class).map_err(|e| {
            PythonDataSourceError::ImportError(format!("Failed to get class {}: {}", class, e))
        })?;

        // Instantiate datasource
        let datasource = py_class.call0().map_err(|e| {
            PythonDataSourceError::ExecutionError(format!("Failed to instantiate {}: {}", class, e))
        })?;

        // Convert partition_spec to Python dict
        let partition_dict = json_to_py_dict(py, partition_spec)?;

        // Convert options to Python dict
        let options_dict = json_to_py_dict(py, options)?;

        // Call read_partition() - returns iterator of PyArrow RecordBatches
        let py_batches = datasource
            .call_method1("read_partition", (partition_dict, options_dict))
            .map_err(|e| {
                PythonDataSourceError::ExecutionError(format!("read_partition() failed: {}", e))
            })?;

        // Iterate over PyArrow batches and convert to Rust Arrow batches
        let mut batches = Vec::new();
        for py_batch_result in py_batches
            .try_iter()
            .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?
        {
            let py_batch = py_batch_result
                .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?;

            // Convert PyArrow RecordBatch to Rust Arrow RecordBatch (zero-copy via FFI!)
            let arrow_batch: PyArrowType<RecordBatch> = py_batch.extract().map_err(|e| {
                PythonDataSourceError::ArrowError(format!("Failed to convert RecordBatch: {}", e))
            })?;
            let batch = arrow_batch.0;

            batches.push(batch);
        }

        Ok(batches)
    })
}

/// Convert JSON value to Python dict
fn json_to_py_dict<'py>(py: Python<'py>, value: &JsonValue) -> Result<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);

    if let JsonValue::Object(map) = value {
        for (key, val) in map {
            let py_val = json_to_py_object(py, val)?;
            dict.set_item(key, py_val)
                .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?;
        }
    }

    Ok(dict)
}

/// Convert JSON value to Python object
fn json_to_py_object(py: Python, value: &JsonValue) -> Result<PyObject> {
    use pyo3::IntoPyObject;

    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => {
            let bound = b
                .into_pyobject(py)
                .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?
                .to_owned();
            Ok(bound.into())
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                let bound = i
                    .into_pyobject(py)
                    .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?
                    .to_owned();
                Ok(bound.into())
            } else if let Some(f) = n.as_f64() {
                let bound = f
                    .into_pyobject(py)
                    .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?
                    .to_owned();
                Ok(bound.into())
            } else {
                let bound = n
                    .to_string()
                    .into_pyobject(py)
                    .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?
                    .to_owned();
                Ok(bound.into())
            }
        }
        JsonValue::String(s) => {
            let bound = s
                .into_pyobject(py)
                .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?
                .to_owned();
            Ok(bound.into())
        }
        JsonValue::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                let py_item = json_to_py_object(py, item)?;
                py_list
                    .append(py_item)
                    .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?;
            }
            Ok(py_list.unbind().into())
        }
        JsonValue::Object(map) => {
            let py_dict = PyDict::new(py);
            for (key, val) in map {
                let py_val = json_to_py_object(py, val)?;
                py_dict
                    .set_item(key, py_val)
                    .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))?;
            }
            Ok(py_dict.unbind().into())
        }
    }
}

/// RecordBatchStream implementation for Python datasource
struct PythonRecordBatchStream {
    schema: SchemaRef,
    inner: std::pin::Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl Stream for PythonRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for PythonRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
