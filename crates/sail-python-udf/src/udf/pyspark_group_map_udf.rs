use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::AggregateUDFImpl;
use pyo3::{PyObject, Python};

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::{PyUdfError, PyUdfResult};
use crate::lazy::LazyPyObject;
use crate::udf::ColumnMatch;
use crate::utils::spark::PySpark;

#[derive(Debug)]
pub struct PySparkGroupMapUDF {
    signature: Signature,
    function_name: String,
    function: Vec<u8>,
    input_names: Vec<String>,
    input_types: Vec<DataType>,
    output_type: DataType,
    column_match: ColumnMatch,
    udf: LazyPyObject,
}

impl PySparkGroupMapUDF {
    pub fn new(
        function_name: String,
        function: Vec<u8>,
        deterministic: bool,
        input_names: Vec<String>,
        input_types: Vec<DataType>,
        output_type: DataType,
        column_match: ColumnMatch,
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
            function_name,
            function,
            input_names,
            input_types,
            output_type,
            column_match,
            udf: LazyPyObject::new(),
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn function(&self) -> &[u8] {
        &self.function
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

    pub fn column_match(&self) -> ColumnMatch {
        self.column_match
    }

    fn output_schema(&self) -> PyUdfResult<SchemaRef> {
        let schema = match &self.output_type {
            DataType::List(field) => match field.data_type() {
                DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
                _ => return Err(PyUdfError::invalid("group map output type")),
            },
            _ => return Err(PyUdfError::invalid("group map output type")),
        };
        Ok(schema)
    }

    fn udf(&self, py: Python) -> PyUdfResult<PyObject> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.function)?;
            Ok(PySpark::group_map_udf(
                py,
                udf,
                self.input_names.clone(),
                self.output_schema()?,
                self.column_match,
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
        &self.function_name
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
        let batch = Python::with_gil(|py| {
            let output = self.udf.call1(py, (args.try_to_py(py)?,))?;
            RecordBatch::try_from_py(py, &output)
        })?;
        let array = StructArray::from(batch);
        let array = ListArray::new(
            Arc::new(Field::new_list_field(array.data_type().clone(), false)),
            OffsetBuffer::from_lengths(vec![array.len()]),
            Arc::new(array),
            None,
        );
        Ok(Arc::new(array))
    }
}
