use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef, AsArray};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::Result;
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::AggregateUDFImpl;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyTuple;
use pyo3::Python;

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::cereal::PythonFunction;
use crate::error::PyUdfResult;
use crate::udf::get_udf_name;
use crate::utils::builtins::PyBuiltins;
use crate::utils::pyarrow::{
    to_pyarrow_data_type, PyArrow, PyArrowArray, PyArrowArrayOptions, PyArrowToPandasOptions,
};

#[derive(Debug)]
pub struct PySparkAggregateUDF {
    signature: Signature,
    function_name: String,
    input_types: Vec<DataType>,
    output_type: DataType,
    python_bytes: Vec<u8>,
    python_function: OnceLock<Result<PySparkUdfObject>>,
}

impl PySparkAggregateUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        output_type: DataType,
        python_bytes: Vec<u8>,
        construct_udf_name: bool,
    ) -> Self {
        let function_name = if construct_udf_name {
            get_udf_name(&function_name, &python_bytes)
        } else {
            function_name
        };
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
            input_types,
            output_type,
            python_bytes,
            python_function: OnceLock::new(),
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn python_bytes(&self) -> &[u8] {
        &self.python_bytes
    }

    pub fn python_function(&self) -> Result<&PySparkUdfObject> {
        self.python_function
            .get_or_init(|| -> Result<PySparkUdfObject> {
                Ok(PySparkUdfObject::load(&self.python_bytes)?)
            })
            .as_ref()
            .map_err(|e| DataFusionError::Internal(format!("Aggregate Python function error: {e}")))
    }
}

impl AggregateUDFImpl for PySparkAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.function_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // We intentionally call self.python_function() in accumulator() instead of in the constructor.
        // This is because the Sail Driver may serialize the UDAF in `try_encode_udaf`.
        let python_function = self.python_function()?;
        if acc_args.is_distinct {
            return exec_err!("distinct is not supported for aggregate UDFs");
        }
        Ok(Box::new(PySparkAggregateUDFAccumulator::new(
            python_function,
            &self.input_types,
            &self.output_type,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        // We accumulate the inputs in the state.
        // Each state field corresponds to an input argument.
        let fields = self
            .input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| {
                let name = format_state_name(args.name, &i.to_string());
                let field = Field::new_list_field(dt.clone(), true);
                Field::new(name, DataType::List(Arc::new(field)), true)
            })
            .collect();
        Ok(fields)
    }
}

#[derive(Debug)]
struct PySparkAggregateUDFAccumulator {
    python_function: PySparkUdfObject,
    inputs: Vec<Vec<ArrayRef>>,
    output_type: DataType,
}

impl PySparkAggregateUDFAccumulator {
    fn new(
        python_function: &PySparkUdfObject,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> Self {
        Self {
            python_function: python_function.clone(),
            inputs: vec![vec![]; input_types.len()],
            output_type: output_type.clone(),
        }
    }
}

fn call_pandas_udaf(
    py: Python,
    function: &PySparkUdfObject,
    args: Vec<ArrayRef>,
    output_type: &DataType,
) -> PyUdfResult<ArrayData> {
    let udaf = function.function(py)?;
    let pyarrow_array = PyArrow::array(
        py,
        PyArrowArrayOptions {
            r#type: Some(to_pyarrow_data_type(py, output_type)?),
            from_pandas: Some(true),
        },
    )?;
    let pyarrow_array_to_pandas = PyArrowArray::to_pandas(
        py,
        PyArrowToPandasOptions {
            use_pandas_nullable_types: true,
        },
    )?;

    let py_args = args
        .iter()
        .map(|arg| {
            let arg = arg.into_data().to_pyarrow(py)?.clone_ref(py).into_bound(py);
            let arg = pyarrow_array_to_pandas.call1((arg,))?;
            Ok(arg)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    let py_args = PyTuple::new_bound(py, &py_args);

    let result = udaf.call1((py.None(), (py_args,)))?;
    let result = PyBuiltins::list(py)?.call1((result,))?.get_item(0)?;

    let data = result.get_item(0)?;
    let _data_type = result.get_item(1)?;
    let data = pyarrow_array.call1((data,))?;

    Ok(ArrayData::from_pyarrow_bound(&data)?)
}

impl Accumulator for PySparkAggregateUDFAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != self.inputs.len() {
            return exec_err!(
                "expected {} arguments, got {}",
                self.inputs.len(),
                values.len()
            );
        }
        for (input, value) in self.inputs.iter_mut().zip(values.iter()) {
            input.push(value.clone());
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| {
                let input = input.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                let input = concat(&input)?;
                Ok(input)
            })
            .collect::<Result<Vec<_>>>()?;
        let array_data = Python::with_gil(|py| {
            call_pandas_udaf(py, &self.python_function, inputs, &self.output_type)
        })?;
        let array = make_array(array_data);
        if array.len() != 1 {
            return exec_err!("expected a single value, got {}", array.len());
        }
        let scalar = ScalarValue::try_from_array(&array, 0)?;
        Ok(scalar)
    }

    fn size(&self) -> usize {
        let mut size = size_of_val(self);
        size += size_of::<Vec<ArrayRef>>() * self.inputs.capacity();
        for input in &self.inputs {
            size += size_of::<ArrayRef>() * input.capacity();
            for array in input {
                size += array.get_array_memory_size();
            }
        }
        size += self.output_type.size();
        size -= size_of_val(&self.output_type);
        size
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = self
            .inputs
            .iter()
            .map(|input| {
                let input = input.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                let input = array_into_list_array(concat(&input)?, true);
                Ok(ScalarValue::List(Arc::new(input)))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != self.inputs.len() {
            return exec_err!(
                "expected {} arguments in the states, got {}",
                self.inputs.len(),
                states.len()
            );
        }
        for (input, state) in self.inputs.iter_mut().zip(states.iter()) {
            let state = state.as_list::<i32>();
            for v in state.iter().flatten() {
                input.push(v);
            }
        }
        Ok(())
    }
}
