use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::AggregateUDFImpl;
use pyo3::{PyObject, Python};

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::python::spark::PySpark;

#[derive(Debug)]
pub struct PySparkGroupAggregateUDF {
    signature: Signature,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    input_names: Vec<String>,
    input_types: Vec<DataType>,
    output_type: DataType,
    config: Arc<PySparkUdfConfig>,
    udf: LazyPyObject,
}

impl PySparkGroupAggregateUDF {
    pub fn new(
        name: String,
        payload: Vec<u8>,
        deterministic: bool,
        input_names: Vec<String>,
        input_types: Vec<DataType>,
        output_type: DataType,
        config: Arc<PySparkUdfConfig>,
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
            name,
            payload,
            deterministic,
            input_names,
            input_types,
            output_type,
            config,
            udf: LazyPyObject::new(),
        }
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

    fn udf(&self, py: Python) -> Result<PyObject> {
        let udf = self.udf.get_or_try_init(py, || {
            Ok(PySpark::group_agg_udf(
                py,
                PySparkUdfPayload::load(py, &self.payload)?,
                self.input_names.clone(),
                &self.config,
            )?
            .unbind())
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
        let udf = Python::with_gil(|py| self.udf(py))?;
        let aggregator = Box::new(PySparkGroupAggregator {
            udf,
            output_type: self.output_type.clone(),
        });
        Ok(Box::new(BatchAggregateAccumulator::new(
            self.input_types.clone(),
            self.output_type.clone(),
            aggregator,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        BatchAggregateAccumulator::state_fields(args)
    }
}

struct PySparkGroupAggregator {
    udf: PyObject,
    output_type: DataType,
}

impl BatchAggregator for PySparkGroupAggregator {
    fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let data = Python::with_gil(|py| -> PyUdfResult<_> {
            let output = self.udf.call1(py, (args.try_to_py(py)?,))?;
            Ok(ArrayData::try_from_py(py, &output)?)
        })?;
        let array = cast(&make_array(data), &self.output_type)?;
        Ok(array)
    }
}
