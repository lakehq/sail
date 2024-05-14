use arrow::array::types;
use std::any::Any;
use std::sync::Arc;

use crate::partial_python_udf::PartialPythonUDF;
use datafusion::arrow::array::{make_array, make_builder, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::arrow::array::builder::*;
use datafusion::common::{DataFusionError, Result};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

use crate::pyarrow::{FromPyArrow, ToPyArrow};

use crate::utils::{array_ref_to_columnar_value, downcast_array_ref, execute_python_function};

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,
    // TODO: See what we exactly need from below fields.
    function_name: String,
    output_type: DataType,
    #[allow(dead_code)]
    eval_type: i32,
    python_function: PartialPythonUDF,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        python_function: PartialPythonUDF,
        output_type: DataType,
        eval_type: i32, // TODO: Incorporate this
    ) -> Self {
        Self {
            signature: Signature::exact(
                input_types,
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            python_function,
            output_type,
            eval_type,
        }
    }
}

impl ScalarUDFImpl for PythonUDF {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{:?} should only be called with a single argument",
                self.name()
            )));
        }

        let (array_ref, is_scalar) = match &args[0] {
            ColumnarValue::Array(arr) => (arr.clone(), false),
            ColumnarValue::Scalar(scalar) => {
                let arr = scalar.to_array().map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to convert scalar to array: {:?}",
                        e
                    ))
                })?;
                (arr, true)
            }
        };

        let array_len = array_ref.len().clone();
        let array_data_type = array_ref.data_type().clone();
        let mut builder: Box<dyn ArrayBuilder> = make_builder(&self.output_type, array_len);

        let processed_array = Python::with_gil(|py| {
            let mut results: Vec<Bound<PyAny>> = vec![];
            let python_function = self
                .python_function
                .0
                .clone_ref(py)
                .into_bound(py)
                .get_item(0)
                .unwrap();

            let py_args = array_ref
                .into_data()
                .to_pyarrow(py)
                .unwrap()
                .call_method0(py, pyo3::intern!(py, "to_pylist"))
                .unwrap()
                .clone_ref(py)
                .into_bound(py);

            for i in 0..array_len {
                let py_arg: Bound<PyAny> = py_args.get_item(i).unwrap();
                let py_arg: Bound<PyTuple> = PyTuple::new_bound(py, &[py_arg]);
                let result: Bound<PyAny> = python_function
                    .call1(py_arg)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
                    .unwrap();

                results.push(result);

                // builder_append_pyany(builder.as_any_mut(), result, &self.output_type).unwrap();
            }

            let kwargs: Bound<PyDict> = PyDict::new_bound(py);
            kwargs
                .set_item("type", self.output_type.to_pyarrow(py).unwrap())
                .unwrap();

            let result: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                .and_then(|pyarrow| pyarrow.getattr(pyo3::intern!(py, "array")))
                .and_then(|array| array.call((results,), Some(&kwargs)))
                .unwrap();

            let array_data = ArrayData::from_pyarrow_bound(&result).unwrap();
            make_array(array_data)
            // Arc::new(builder.finish()) as ArrayRef
        });

        Ok(array_ref_to_columnar_value(
            processed_array,
            &self.output_type,
            is_scalar,
        )?)
    }
}

pub fn builder_append_pyany(
    builder: &mut dyn Any,
    pyany: Bound<PyAny>,
    datatype: &DataType,
) -> Result<()> {
    match datatype {
        DataType::Null => Ok(builder.downcast_mut::<NullBuilder>().unwrap().append_null()),
        DataType::Boolean => Ok(builder
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Int8 => Ok(builder
            .downcast_mut::<Int8Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Int16 => Ok(builder
            .downcast_mut::<Int16Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Int32 => Ok(builder
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Int64 => Ok(builder
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::UInt8 => Ok(builder
            .downcast_mut::<UInt8Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::UInt16 => Ok(builder
            .downcast_mut::<UInt16Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::UInt32 => Ok(builder
            .downcast_mut::<UInt32Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::UInt64 => Ok(builder
            .downcast_mut::<UInt64Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Float16 => Err(DataFusionError::Execution(
            "DataType::Float16 is not supported by Python".to_string(),
        )),
        DataType::Float32 => Ok(builder
            .downcast_mut::<Float32Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Float64 => Ok(builder
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Binary => Err(DataFusionError::Execution(
            "DataType::Binary is not supported by Python".to_string(),
        )),
        DataType::LargeBinary => Err(DataFusionError::Execution(
            "DataType::LargeBinary is not supported by Python".to_string(),
        )),
        DataType::FixedSizeBinary(_len) => Err(DataFusionError::Execution(
            "DataType::FixedSizeBinary is not supported by Python".to_string(),
        )),
        DataType::Decimal128(_p, _s) => Ok(builder
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Decimal256(_p, _s) => Err(DataFusionError::Execution(
            "DataType::Decimal256 is not supported by Python".to_string(),
        )),
        DataType::Utf8 => Err(DataFusionError::Execution(
            "DataType::Utf8 is not supported by Python".to_string(),
        )),
        DataType::LargeUtf8 => Err(DataFusionError::Execution(
            "DataType::LargeUtf8 is not supported by Python".to_string(),
        )),
        DataType::Date32 => Ok(builder
            .downcast_mut::<Date32Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Date64 => Ok(builder
            .downcast_mut::<Date64Builder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Time32(TimeUnit::Second) => Ok(builder
            .downcast_mut::<Time32SecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Time32(TimeUnit::Millisecond) => Ok(builder
            .downcast_mut::<Time32MillisecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Time64(TimeUnit::Microsecond) => Ok(builder
            .downcast_mut::<Time64MicrosecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(builder
            .downcast_mut::<Time64NanosecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Timestamp(TimeUnit::Second, _tz) => Ok(builder
            .downcast_mut::<TimestampSecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Timestamp(TimeUnit::Millisecond, _tz) => Ok(builder
            .downcast_mut::<TimestampMillisecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Timestamp(TimeUnit::Microsecond, _tz) => Ok(builder
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Timestamp(TimeUnit::Nanosecond, _tz) => Ok(builder
            .downcast_mut::<TimestampNanosecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Interval(IntervalUnit::YearMonth) => Ok(builder
            .downcast_mut::<IntervalYearMonthBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Interval(IntervalUnit::DayTime) => Ok(builder
            .downcast_mut::<IntervalDayTimeBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Interval(IntervalUnit::MonthDayNano) => Ok(builder
            .downcast_mut::<IntervalMonthDayNanoBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Duration(TimeUnit::Second) => Ok(builder
            .downcast_mut::<DurationSecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Duration(TimeUnit::Millisecond) => Ok(builder
            .downcast_mut::<DurationMillisecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Duration(TimeUnit::Microsecond) => Ok(builder
            .downcast_mut::<DurationMicrosecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::Duration(TimeUnit::Nanosecond) => Ok(builder
            .downcast_mut::<DurationNanosecondBuilder>()
            .unwrap()
            .append_value(pyany.extract().unwrap())),
        DataType::List(_field) => Err(DataFusionError::NotImplemented(
            "TODO: DataType::List".to_string(),
        )),
        DataType::LargeList(_field) => Err(DataFusionError::NotImplemented(
            "TODO: DataType::LargeList".to_string(),
        )),
        DataType::Map(_field, _) => Err(DataFusionError::NotImplemented(
            "TODO: DataType::Map".to_string(),
        )),
        DataType::Struct(_fields) => Err(DataFusionError::NotImplemented(
            "TODO: DataType::Struct".to_string(),
        )),
        t => Err(DataFusionError::NotImplemented(format!(
            "Not supported: DataType::{:?}",
            t
        ))),
    }
}
