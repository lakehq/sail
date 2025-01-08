use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::array::make_array;
use datafusion_common::exec_err;
use datafusion_expr::ScalarUDFImpl;
use pyo3::{PyObject, Python};

use crate::array::{build_list_array, get_list_field, get_struct_array_type};
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::python::spark::PySpark;

#[derive(Debug)]
pub struct PySparkCoGroupMapUDF {
    signature: Signature,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    left_types: Vec<DataType>,
    left_names: Vec<String>,
    right_types: Vec<DataType>,
    right_names: Vec<String>,
    output_type: DataType,
    config: Arc<PySparkUdfConfig>,
    udf: LazyPyObject,
}

impl PySparkCoGroupMapUDF {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        name: String,
        payload: Vec<u8>,
        deterministic: bool,
        left_types: Vec<DataType>,
        left_names: Vec<String>,
        right_types: Vec<DataType>,
        right_names: Vec<String>,
        output_type: DataType,
        config: Arc<PySparkUdfConfig>,
    ) -> Result<Self> {
        let input_types = vec![
            get_struct_array_type(&left_types, &left_names)?,
            get_struct_array_type(&right_types, &right_names)?,
        ];
        Ok(Self {
            signature: Signature::exact(
                input_types,
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            name,
            payload,
            deterministic,
            left_types,
            left_names,
            right_types,
            right_names,
            output_type,
            config,
            udf: LazyPyObject::new(),
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }

    pub fn left_types(&self) -> &[DataType] {
        &self.left_types
    }

    pub fn left_names(&self) -> &[String] {
        &self.left_names
    }

    pub fn right_types(&self) -> &[DataType] {
        &self.right_types
    }

    pub fn right_names(&self) -> &[String] {
        &self.right_names
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }

    fn udf(&self, py: Python) -> Result<PyObject> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            Ok(PySpark::cogroup_map_udf(
                py,
                udf,
                self.left_names.clone(),
                self.right_names.clone(),
                &self.config,
            )?
            .unbind())
        })?;
        Ok(udf.clone_ref(py))
    }

    fn get_group(list: &ArrayRef, i: usize) -> Result<Vec<ArrayRef>> {
        let list = list.as_list::<i32>();
        let array = if list.is_null(i) {
            make_array(ArrayData::new_null(&list.value_type(), 0))
        } else {
            list.value(i)
        };
        let value = array.as_struct();
        if value.nulls().map(|x| x.null_count()).unwrap_or_default() > 0 {
            return exec_err!("co-group map UDF input arrays must not contain nulls");
        }
        Ok(value.columns().to_vec())
    }
}

impl ScalarUDFImpl for PySparkCoGroupMapUDF {
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

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        let mut args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;
        let (Some(right), Some(left), true) = (args.pop(), args.pop(), args.is_empty()) else {
            return exec_err!("co-group map expects exactly two arguments");
        };
        if left.len() != right.len() {
            return exec_err!(
                "co-group map UDF input arrays have different lengths: left {}, right {}",
                left.len(),
                right.len()
            );
        }
        let udf = Python::with_gil(|py| self.udf(py))?;
        let field = get_list_field(self.output_type())?;
        let arrays = (0..left.len())
            .map(|i| {
                let left = Self::get_group(&left, i)?;
                let right = Self::get_group(&right, i)?;
                let data = Python::with_gil(|py| -> PyUdfResult<_> {
                    let output = udf.call1(py, (left.try_to_py(py)?, right.try_to_py(py)?))?;
                    Ok(ArrayData::try_from_py(py, &output)?)
                })?;
                let array = cast(&make_array(data), field.data_type())?;
                Ok(array)
            })
            .collect::<Result<Vec<_>>>()?;
        let array = build_list_array(&arrays, &self.output_type)?;
        Ok(ColumnarValue::Array(array))
    }
}
