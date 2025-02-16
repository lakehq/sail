use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{exec_err, plan_err};
use datafusion_expr::Expr;
use pyo3::Python;
use sail_common_datafusion::udf::StreamUDF;

use crate::cereal::pyspark_udtf::PySparkUdtfPayload;
use crate::config::PySparkUdfConfig;
use crate::error::PyUdfResult;
use crate::python::spark::PySpark;
use crate::stream::PyMapStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum PySparkUdtfKind {
    Table,
    ArrowTable,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PySparkUDTF {
    kind: PySparkUdtfKind,
    name: String,
    payload: Vec<u8>,
    /// The names of columns in the input batch.
    input_names: Vec<String>,
    /// The data types of columns in the input batch.
    input_types: Vec<DataType>,
    /// The number of passthrough columns in the input batch.
    /// This is also the start position of the UDTF arguments in the input batch.
    /// The input batch contains the passthrough columns, followed by the UDTF arguments.
    passthrough_columns: usize,
    /// The UDTF return type. This can only be a struct type.
    function_return_type: DataType,
    /// The optional names to override the field names in the UDTF return type.
    function_output_names: Option<Vec<String>>,
    deterministic: bool,
    config: Arc<PySparkUdfConfig>,
    /// The output schema of the UDTF output stream.
    /// The output batch contains the passthrough non-argument columns from the input batch,
    /// followed by the columns of the UDTF output.
    output_schema: SchemaRef,
}

#[derive(PartialEq, PartialOrd)]
struct PySparkUDTFOrd<'a> {
    kind: PySparkUdtfKind,
    name: &'a str,
    payload: &'a [u8],
    input_names: &'a [String],
    input_types: &'a [DataType],
    passthrough_columns: usize,
    function_return_type: &'a DataType,
    function_output_names: &'a Option<Vec<String>>,
    deterministic: &'a bool,
    config: &'a PySparkUdfConfig,
}

impl<'a> From<&'a PySparkUDTF> for PySparkUDTFOrd<'a> {
    fn from(udtf: &'a PySparkUDTF) -> Self {
        Self {
            kind: udtf.kind,
            name: &udtf.name,
            payload: &udtf.payload,
            input_names: &udtf.input_names,
            input_types: &udtf.input_types,
            passthrough_columns: udtf.passthrough_columns,
            function_return_type: &udtf.function_return_type,
            function_output_names: &udtf.function_output_names,
            deterministic: &udtf.deterministic,
            config: &udtf.config,
        }
    }
}

impl PartialOrd for PySparkUDTF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PySparkUDTFOrd::from(self).partial_cmp(&other.into())
    }
}

impl PySparkUDTF {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        kind: PySparkUdtfKind,
        name: String,
        payload: Vec<u8>,
        input_names: Vec<String>,
        input_types: Vec<DataType>,
        passthrough_columns: usize,
        function_return_type: DataType,
        function_output_names: Option<Vec<String>>,
        deterministic: bool,
        config: Arc<PySparkUdfConfig>,
    ) -> Result<Self> {
        if input_names.len() != input_types.len() {
            return plan_err!(
                "The number of input names ({}) does not match the number of input types ({}).",
                input_names.len(),
                input_types.len()
            );
        }
        if input_names.len() < passthrough_columns {
            return plan_err!(
                "The number of input columns ({}) is less than the number of passthrough columns ({}).",
                input_names.len(),
                passthrough_columns
            );
        }
        let function_return_fields = match &function_return_type {
            DataType::Struct(fields) => fields.to_vec(),
            _ => {
                // The PySpark unit test expects the exact error message here.
                return plan_err!(
                    "Invalid Python user-defined table function return type. Expect a struct type, but got {}.",
                    &function_return_type
                );
            }
        };
        let function_return_fields = if let Some(names) = &function_output_names {
            if names.len() != function_return_fields.len() {
                return plan_err!(
                    "The number of function output names ({}) does not match the number of function return fields ({}).",
                    names.len(),
                    function_return_fields.len()
                );
            }
            function_return_fields
                .into_iter()
                .zip(names.iter())
                .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                .collect()
        } else {
            function_return_fields
        };
        let output_fields = (0..passthrough_columns)
            .map(|i| {
                Arc::new(Field::new(
                    input_names[i].clone(),
                    input_types[i].clone(),
                    true,
                ))
            })
            .chain(function_return_fields)
            .collect::<Vec<_>>();
        Ok(Self {
            kind,
            name,
            payload,
            input_names,
            input_types,
            passthrough_columns,
            function_return_type,
            function_output_names,
            output_schema: Arc::new(Schema::new(output_fields)),
            deterministic,
            config,
        })
    }

    pub fn kind(&self) -> PySparkUdtfKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn input_names(&self) -> &[String] {
        &self.input_names
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn passthrough_columns(&self) -> usize {
        self.passthrough_columns
    }

    pub fn function_return_type(&self) -> &DataType {
        &self.function_return_type
    }

    pub fn function_output_names(&self) -> Option<&[String]> {
        self.function_output_names.as_deref()
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }
}

impl TableFunctionImpl for PySparkUDTF {
    fn call(&self, _: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        exec_err!("Python UDTF should not be called as a table function")
    }
}

impl StreamUDF for PySparkUDTF {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn invoke(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let function = Python::with_gil(|py| -> PyUdfResult<_> {
            let udtf = PySparkUdtfPayload::load(py, &self.payload)?;
            let udtf = match self.kind {
                PySparkUdtfKind::Table => PySpark::table_udf(
                    py,
                    udtf,
                    &self.input_types,
                    self.passthrough_columns,
                    &self.output_schema,
                    &self.config,
                )?,
                PySparkUdtfKind::ArrowTable => PySpark::arrow_table_udf(
                    py,
                    udtf,
                    &self.input_names,
                    self.passthrough_columns,
                    &self.output_schema,
                    &self.config,
                )?,
            };
            Ok(udtf.unbind())
        })?;
        Ok(Box::pin(PyMapStream::new(
            input,
            function,
            self.output_schema.clone(),
        )))
    }
}
