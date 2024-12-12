use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, RecordBatch, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_common::exec_err;
use datafusion_expr::ScalarUDFImpl;
use pyo3::{PyObject, Python};

use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::udf::ColumnMatch;
use crate::utils::spark::PySpark;

#[derive(Debug)]
pub struct PySparkCoGroupMapUDF {
    signature: Signature,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    left_type: DataType,
    left_inner_schema: SchemaRef,
    right_type: DataType,
    right_inner_schema: SchemaRef,
    output_type: DataType,
    output_inner_schema: SchemaRef,
    column_match: ColumnMatch,
    udf: LazyPyObject,
}

impl PySparkCoGroupMapUDF {
    pub fn try_new(
        name: String,
        payload: Vec<u8>,
        deterministic: bool,
        left_type: DataType,
        right_type: DataType,
        output_type: DataType,
        column_match: ColumnMatch,
    ) -> Result<Self> {
        let input_types = vec![left_type.clone(), right_type.clone()];
        let left_inner_schema = Self::get_inner_schema(&left_type)?;
        let right_inner_schema = Self::get_inner_schema(&right_type)?;
        let output_inner_schema = Self::get_inner_schema(&output_type)?;
        Ok(Self {
            signature: Signature::exact(
                input_types,
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            name,
            payload,
            deterministic,
            left_type,
            left_inner_schema,
            right_type,
            right_inner_schema,
            output_type,
            output_inner_schema,
            column_match,
            udf: LazyPyObject::new(),
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }

    pub fn left_type(&self) -> &DataType {
        &self.left_type
    }

    pub fn right_type(&self) -> &DataType {
        &self.right_type
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn column_match(&self) -> ColumnMatch {
        self.column_match
    }

    fn udf(&self, py: Python) -> PyUdfResult<PyObject> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            Ok(PySpark::cogroup_map_udf(
                py,
                udf,
                self.output_inner_schema.clone(),
                self.column_match,
            )?
            .unbind())
        })?;
        Ok(udf.clone_ref(py))
    }

    fn validate_input(data: &ArrayRef, data_type: &DataType) -> Result<()> {
        if data.data_type() == data_type {
            Ok(())
        } else {
            exec_err!(
                "co-group map UDF input type mismatch: expected {}, got {}",
                data_type,
                data.data_type()
            )
        }
    }

    fn get_inner_schema(data_type: &DataType) -> Result<SchemaRef> {
        let error = || exec_err!("invalid co-group map UDF data type: {data_type}");
        let field = match data_type {
            DataType::List(field) => field,
            _ => return error(),
        };
        match field.data_type() {
            DataType::Struct(fields) => Ok(Arc::new(Schema::new(fields.clone()))),
            _ => error(),
        }
    }

    fn get_group(list: &ArrayRef, i: usize, schema: &SchemaRef) -> Result<RecordBatch> {
        let list = list.as_list::<i32>();
        if list.is_null(i) {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let value = list.value(i);
        let value = value.as_struct();
        if value.nulls().map(|x| x.null_count()).unwrap_or_default() > 0 {
            return exec_err!("co-group map UDF input arrays must not contain nulls");
        }
        Ok(RecordBatch::from(value.clone()))
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
        let num_args = args.len();
        let mut args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;
        let (Some(right), Some(left), true) = (args.pop(), args.pop(), args.is_empty()) else {
            return exec_err!("co-group map expects exactly two arguments, got {num_args}");
        };
        Self::validate_input(&left, &self.left_type)?;
        Self::validate_input(&right, &self.right_type)?;
        if left.len() != right.len() {
            return exec_err!(
                "co-group map UDF input arrays have different lengths: left {}, right {}",
                left.len(),
                right.len()
            );
        }
        let mut arrays: Vec<ArrayRef> = vec![];
        for i in 0..left.len() {
            let left = Self::get_group(&left, i, &self.left_inner_schema)?;
            let right = Self::get_group(&right, i, &self.right_inner_schema)?;
            let result = Python::with_gil(|py| -> PyUdfResult<_> {
                let output = self
                    .udf(py)?
                    .call1(py, (left.try_to_py(py)?, right.try_to_py(py)?))?;
                Ok(RecordBatch::try_from_py(py, &output)?)
            })?;
            let result = StructArray::from(result);
            arrays.push(Arc::new(result));
        }
        let field = Arc::new(Field::new_list_field(
            DataType::Struct(self.output_inner_schema.fields.clone()),
            false,
        ));
        let array = if arrays.is_empty() {
            ListArray::new_null(field, 0)
        } else {
            let lengths = arrays.iter().map(|x| x.len()).collect::<Vec<_>>();
            let arrays = arrays.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            ListArray::new(
                field,
                OffsetBuffer::from_lengths(lengths),
                concat(&arrays)?,
                None,
            )
        };
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}
