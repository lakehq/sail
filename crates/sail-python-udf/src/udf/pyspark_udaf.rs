use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    make_array, new_empty_array, Array, ArrayData, ArrayRef, AsArray, ListArray, RecordBatch,
    StructArray,
};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::Result;
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::arrow::buffer::OffsetBuffer;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::AggregateUDFImpl;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyTuple;
use pyo3::{intern, Bound, PyObject, Python};

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::error::{PyUdfError, PyUdfResult};
use crate::udf::ColumnMatch;
use crate::utils::builtins::PyBuiltins;
use crate::utils::pandas::PandasDataFrame;
use crate::utils::pyarrow::{
    PyArrow, PyArrowArray, PyArrowArrayOptions, PyArrowRecordBatch, PyArrowToPandasOptions,
};

#[derive(Debug, Copy, Clone)]
pub enum PySparkAggFormat {
    NoOp,
    GroupAgg,
    GroupMap,
    /// The legacy group map behavior that matches returned columns by position.
    GroupMapLegacy,
}

#[derive(Debug)]
pub struct PySparkAggregateUDF {
    signature: Signature,
    format: PySparkAggFormat,
    function_name: String,
    input_names: Vec<String>,
    input_types: Vec<DataType>,
    output_type: DataType,
    python_function: PySparkUdfObject,
}

impl PySparkAggregateUDF {
    pub fn new(
        format: PySparkAggFormat,
        function_name: String,
        deterministic: bool,
        input_names: Vec<String>,
        input_types: Vec<DataType>,
        output_type: DataType,
        function: Vec<u8>,
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
            format,
            function_name,
            input_names,
            input_types,
            output_type,
            python_function: PySparkUdfObject::new(function),
        }
    }

    pub fn format(&self) -> PySparkAggFormat {
        self.format
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
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

    pub fn function(&self) -> &[u8] {
        self.python_function.data()
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
        let udf = Python::with_gil(|py| self.python_function.get(py))?;
        if acc_args.is_distinct {
            return exec_err!("distinct is not supported for aggregate UDFs");
        }
        Ok(Box::new(PySparkAggregateUDFAccumulator::new(
            self.format,
            udf,
            self.input_names.clone(),
            self.input_types.clone(),
            self.output_type.clone(),
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
    format: PySparkAggFormat,
    udf: PyObject,
    input_names: Vec<String>,
    input_types: Vec<DataType>,
    inputs: Vec<Vec<ArrayRef>>,
    output_type: DataType,
}

impl PySparkAggregateUDFAccumulator {
    fn new(
        format: PySparkAggFormat,
        udf: PyObject,
        input_names: Vec<String>,
        input_types: Vec<DataType>,
        output_type: DataType,
    ) -> Self {
        let num_inputs = input_names.len();
        Self {
            format,
            udf,
            input_names,
            input_types,
            inputs: vec![vec![]; num_inputs],
            output_type,
        }
    }
}

fn call_pandas_group_agg_udf(
    py: Python,
    udf: PyObject,
    input_names: &[String],
    args: Vec<ArrayRef>,
    output_type: &DataType,
) -> PyUdfResult<ArrayRef> {
    let udf = udf.into_bound(py);
    let pyarrow_array = PyArrow::array(
        py,
        PyArrowArrayOptions {
            r#type: Some(output_type.to_pyarrow(py)?),
            from_pandas: Some(true),
        },
    )?;

    let args = get_pandas_udf_arguments(py, input_names, &args)?;
    let result = udf.call1((py.None(), (args,)))?;
    let result = PyBuiltins::list(py)?.call1((result,))?;
    let result = result.get_item(0)?;

    let data = result.get_item(0)?;
    let _data_type = result.get_item(1)?;
    let data = pyarrow_array.call1((data,))?;

    Ok(make_array(ArrayData::from_pyarrow_bound(&data)?))
}

fn call_pandas_group_map_udf(
    py: Python,
    udf: PyObject,
    input_names: &[String],
    args: Vec<ArrayRef>,
    output_type: &DataType,
    column_match: ColumnMatch,
) -> PyUdfResult<ArrayRef> {
    let udf = udf.into_bound(py);
    let schema = match output_type {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
            _ => return Err(PyUdfError::invalid("group map UDF output type")),
        },
        _ => return Err(PyUdfError::invalid("group map UDF output type")),
    };
    let pyarrow_record_batch_from_pandas =
        PyArrowRecordBatch::from_pandas(py, Some(schema.to_pyarrow(py)?))?;

    let args = get_pandas_udf_arguments(py, input_names, &args)?;
    let result = udf.call1((py.None(), (args,)))?;
    let result = PyBuiltins::list(py)?.call1((result,))?;
    let result = result.get_item(0)?.get_item(0)?;

    let data = result.get_item(0)?;
    let _data_type = result.get_item(1)?;

    let batch = if data.is_empty()? {
        RecordBatch::new_empty(schema.clone())
    } else {
        let data = if matches!(column_match, ColumnMatch::ByName)
            && PandasDataFrame::has_string_columns(&data)?
        {
            data
        } else {
            PandasDataFrame::rename_columns_by_position(&data, &schema)?
        };
        let batch = pyarrow_record_batch_from_pandas.call1((data,))?;
        RecordBatch::from_pyarrow_bound(&batch)?
    };
    let array = StructArray::from(batch);
    let array = ListArray::new(
        Arc::new(Field::new_list_field(array.data_type().clone(), false)),
        OffsetBuffer::from_lengths(vec![array.len()]),
        Arc::new(array),
        None,
    );
    Ok(Arc::new(array))
}

fn call_no_op_udf(args: Vec<ArrayRef>, output_type: &DataType) -> PyUdfResult<ArrayRef> {
    let schema = match output_type {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
            _ => return Err(PyUdfError::invalid("no-op UDF output type")),
        },
        _ => return Err(PyUdfError::invalid("no-op UDF output type")),
    };
    let batch =
        RecordBatch::try_new(schema, args).map_err(|e| PyUdfError::invalid(e.to_string()))?;
    let array = StructArray::from(batch);
    let array = ListArray::new(
        Arc::new(Field::new_list_field(array.data_type().clone(), false)),
        OffsetBuffer::from_lengths(vec![array.len()]),
        Arc::new(array),
        None,
    );
    Ok(Arc::new(array))
}

fn get_pandas_udf_arguments<'py>(
    py: Python<'py>,
    names: &[String],
    args: &[ArrayRef],
) -> PyUdfResult<Bound<'py, PyTuple>> {
    let pyarrow_array_to_pandas = PyArrowArray::to_pandas(
        py,
        PyArrowToPandasOptions {
            use_pandas_nullable_types: true,
        },
    )?;
    let args = args
        .iter()
        .zip(names)
        .map(|(arg, name)| {
            let arg = arg.into_data().to_pyarrow(py)?;
            let arg = pyarrow_array_to_pandas.call1((arg,))?;
            arg.setattr(intern!(py, "name"), name)?;
            Ok(arg)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    Ok(PyTuple::new_bound(py, &args))
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
            .zip(&self.input_types)
            .map(|(input, data_type)| {
                let input = input.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                let input = if input.is_empty() {
                    new_empty_array(data_type)
                } else {
                    concat(&input)?
                };
                Ok(input)
            })
            .collect::<Result<Vec<_>>>()?;
        let array = Python::with_gil(|py| {
            let udf = self.udf.clone_ref(py);
            match self.format {
                PySparkAggFormat::NoOp => call_no_op_udf(inputs, &self.output_type),
                PySparkAggFormat::GroupAgg => {
                    call_pandas_group_agg_udf(py, udf, &self.input_names, inputs, &self.output_type)
                }
                PySparkAggFormat::GroupMap => call_pandas_group_map_udf(
                    py,
                    udf,
                    &self.input_names,
                    inputs,
                    &self.output_type,
                    ColumnMatch::ByName,
                ),
                PySparkAggFormat::GroupMapLegacy => call_pandas_group_map_udf(
                    py,
                    udf,
                    &self.input_names,
                    inputs,
                    &self.output_type,
                    ColumnMatch::ByPosition,
                ),
            }
        })?;
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
            .zip(&self.input_types)
            .map(|(input, data_type)| {
                let field = Arc::new(Field::new_list_field(data_type.clone(), true));
                let input = input.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                let input = if input.is_empty() {
                    ListArray::new_null(field, 0)
                } else {
                    let values = concat(&input)?;
                    let offsets = OffsetBuffer::from_lengths([values.len()]);
                    ListArray::new(field, offsets, values, None)
                };
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
