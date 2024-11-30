use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, RecordBatch, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_common::exec_err;
use datafusion_expr::ScalarUDFImpl;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Bound, PyAny, PyObject, Python};

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::error::PyUdfResult;
use crate::udf::ColumnMatch;
use crate::utils::builtins::PyBuiltins;
use crate::utils::pandas::PandasDataFrame;
use crate::utils::pyarrow::{PyArrowRecordBatch, PyArrowToPandasOptions};

#[derive(Debug)]
pub struct PySparkCoGroupMapUDF {
    signature: Signature,
    function_name: String,
    deterministic: bool,
    left_type: DataType,
    left_inner_schema: SchemaRef,
    right_type: DataType,
    right_inner_schema: SchemaRef,
    output_type: DataType,
    output_inner_schema: SchemaRef,
    python_function: PySparkUdfObject,
    legacy: bool,
}

impl PySparkCoGroupMapUDF {
    pub fn try_new(
        function_name: String,
        deterministic: bool,
        left_type: DataType,
        right_type: DataType,
        output_type: DataType,
        function: Vec<u8>,
        legacy: bool,
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
            function_name,
            deterministic,
            left_type,
            left_inner_schema,
            right_type,
            right_inner_schema,
            output_type,
            output_inner_schema,
            python_function: PySparkUdfObject::new(function),
            legacy,
        })
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
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

    pub fn function(&self) -> &[u8] {
        self.python_function.data()
    }

    pub fn legacy(&self) -> bool {
        self.legacy
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

    fn invoke_group(
        &self,
        py: Python,
        udf: PyObject,
        left: RecordBatch,
        right: RecordBatch,
    ) -> PyUdfResult<RecordBatch> {
        let udf = udf.into_bound(py);
        let left = Self::get_group_arg(py, left)?;
        let right = Self::get_group_arg(py, right)?;
        let args = vec![left, right];

        let result = udf.call1((py.None(), (args,)))?;
        let result = PyBuiltins::list(py)?.call1((result,))?;
        let result = result.get_item(0)?.get_item(0)?;

        let data = result.get_item(0)?;
        let _data_type = result.get_item(1)?;

        let schema = &self.output_inner_schema;
        let pyarrow_record_batch_from_pandas =
            PyArrowRecordBatch::from_pandas(py, Some(schema.to_pyarrow(py)?))?;
        let column_match = if self.legacy {
            ColumnMatch::ByPosition
        } else {
            ColumnMatch::ByName
        };
        let batch = if data.is_empty()? {
            RecordBatch::new_empty(schema.clone())
        } else {
            let data = if matches!(column_match, ColumnMatch::ByName)
                && PandasDataFrame::has_string_columns(&data)?
            {
                data
            } else {
                PandasDataFrame::rename_columns_by_position(&data, schema)?
            };
            let batch = pyarrow_record_batch_from_pandas.call1((data,))?;
            RecordBatch::from_pyarrow_bound(&batch)?
        };

        Ok(batch)
    }

    fn get_group_arg(py: Python, batch: RecordBatch) -> PyUdfResult<Bound<PyAny>> {
        let batch = batch.to_pyarrow(py)?;
        let df = PyArrowRecordBatch::to_pandas(
            py,
            PyArrowToPandasOptions {
                use_pandas_nullable_types: false,
            },
        )?
        .call1((batch,))?;
        Ok(PandasDataFrame::to_series_list(&df)?)
    }
}

impl ScalarUDFImpl for PySparkCoGroupMapUDF {
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
            let result = Python::with_gil(|py| {
                let udf = self.python_function.get(py)?;
                self.invoke_group(py, udf, left, right)
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
