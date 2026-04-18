use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::AggregateUDFImpl;
use pyo3::{Py, PyAny, Python};

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::python::spark::PySpark;

// Distinguishes Pandas (202) vs Arrow-native (252) grouped aggregate UDFs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PySparkGroupAggKind {
    Pandas,
    Arrow,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkGroupAggregateUDF {
    signature: Signature,
    kind: PySparkGroupAggKind,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    input_names: Vec<String>,
    input_types: Vec<DataType>,
    output_type: DataType,
    config: Arc<PySparkUdfConfig>,
    /// Number of arguments the Python function actually accepts. When a dummy
    /// argument has been injected (0-arg UDF), this is 0 while `input_types`
    /// contains the dummy Int64 type.
    actual_arg_count: usize,
    udf: LazyPyObject,
}

impl PySparkGroupAggregateUDF {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        kind: PySparkGroupAggKind,
        name: String,
        payload: Vec<u8>,
        deterministic: bool,
        input_names: Vec<String>,
        input_types: Vec<DataType>,
        output_type: DataType,
        config: Arc<PySparkUdfConfig>,
        actual_arg_count: usize,
    ) -> Self {
        let signature = Signature::exact(
            input_types.clone(),
            match deterministic {
                true => Volatility::Immutable,
                false => Volatility::Volatile,
            },
        );
        Self {
            signature,
            kind,
            name,
            payload,
            deterministic,
            input_names,
            input_types,
            output_type,
            config,
            actual_arg_count,
            udf: LazyPyObject::new(),
        }
    }

    pub fn kind(&self) -> PySparkGroupAggKind {
        self.kind
    }

    pub fn actual_arg_count(&self) -> usize {
        self.actual_arg_count
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }

    pub fn input_names(&self) -> &[String] {
        &self.input_names
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }

    fn udf(&self, py: Python) -> Result<Py<PyAny>> {
        let udf = self.udf.get_or_try_init(py, || {
            let loaded = PySparkUdfPayload::load(py, &self.payload)?;
            let wrapped = match self.kind {
                // Pandas path: wraps Arrow → named Pandas Series → user func → Arrow
                PySparkGroupAggKind::Pandas => {
                    PySpark::group_agg_udf(py, loaded, self.input_names.clone(), &self.config)?
                }
                // Arrow path: passes Arrow arrays directly to user func
                PySparkGroupAggKind::Arrow => {
                    PySpark::group_agg_arrow_udf(py, loaded, &self.config)?
                }
            };
            Ok(wrapped.unbind())
        })?;
        Ok(udf.clone_ref(py))
    }
}

impl AggregateUDFImpl for PySparkGroupAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let udf = Python::attach(|py| self.udf(py))?;
        let aggregator = Box::new(PySparkGroupAggregator {
            udf,
            output_type: self.output_type.clone(),
        });
        Ok(Box::new(BatchAggregateAccumulator::new(
            self.input_types.clone(),
            self.output_type.clone(),
            aggregator,
            self.actual_arg_count,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        BatchAggregateAccumulator::state_fields(args)
    }
}

struct PySparkGroupAggregator {
    udf: Py<PyAny>,
    output_type: DataType,
}

impl BatchAggregator for PySparkGroupAggregator {
    fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let data = Python::attach(|py| -> PyUdfResult<_> {
            let output = self.udf.call1(py, (args.try_to_py(py)?,))?;
            Ok(ArrayData::try_from_py(py, &output)?)
        })?;
        let array = cast(&make_array(data), &self.output_type)?;
        Ok(array)
    }
}
