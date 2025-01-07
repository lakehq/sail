use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::AggregateUDFImpl;
use pyo3::{PyObject, Python};

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::{PyUdfError, PyUdfResult};
use crate::lazy::LazyPyObject;
use crate::utils::spark::PySpark;

#[derive(Debug)]
pub struct PySparkGroupMapUDF {
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

impl PySparkGroupMapUDF {
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

    fn udf(&self, py: Python) -> PyUdfResult<PyObject> {
        let field = match &self.output_type {
            DataType::List(field) => field,
            _ => return Err(PyUdfError::invalid("group map output type")),
        };
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            Ok(PySpark::group_map_udf(
                py,
                udf,
                self.input_names.clone(),
                field.data_type(),
                &self.config,
            )?
            .unbind())
        })?;
        Ok(udf.clone_ref(py))
    }
}

impl AggregateUDFImpl for PySparkGroupMapUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        let udf = Python::with_gil(|py| self.udf(py))?;
        let aggregator = Box::new(PySparkGroupMapper { udf });
        Ok(Box::new(BatchAggregateAccumulator::new(
            self.input_types.clone(),
            self.output_type.clone(),
            aggregator,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> datafusion_common::Result<Vec<Field>> {
        BatchAggregateAccumulator::state_fields(args)
    }
}

struct PySparkGroupMapper {
    udf: PyObject,
}

impl BatchAggregator for PySparkGroupMapper {
    fn call(&self, args: &[ArrayRef]) -> PyUdfResult<ArrayRef> {
        let output = Python::with_gil(|py| {
            let output = self.udf.call1(py, (args.try_to_py(py)?,))?;
            ArrayData::try_from_py(py, &output)
        })?;
        let array = make_array(output);
        let array = ListArray::new(
            Arc::new(Field::new_list_field(array.data_type().clone(), false)),
            OffsetBuffer::from_lengths(vec![array.len()]),
            Arc::new(array),
            None,
        );
        Ok(Arc::new(array))
    }
}
