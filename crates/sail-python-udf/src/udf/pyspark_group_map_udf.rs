use std::sync::Arc;

use datafusion::arrow::array::{ArrayData, ArrayRef, make_array};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::Result;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use pyo3::{Py, PyAny, Python};
use sail_common_datafusion::array::record_batch::{
    cast_array_positionally_recursively, cast_array_recursively,
};

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::array::{build_singleton_list_array, get_list_field};
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::python::spark::PySpark;

/// Mode flags describing the variant of a GroupMap UDF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PySparkGroupMapMode {
    /// Whether the UDF uses pandas DataFrames (true) or Arrow RecordBatches (false).
    pub is_pandas: bool,
    /// Whether the UDF is the iterator variant that receives/yields multiple batches.
    pub is_iter: bool,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkGroupMapUDF {
    signature: Signature,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    input_names: Vec<String>,
    input_types: Vec<DataType>,
    output_type: DataType,
    mode: PySparkGroupMapMode,
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
        mode: PySparkGroupMapMode,
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
            mode,
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

    pub fn is_pandas(&self) -> bool {
        self.mode.is_pandas
    }

    pub fn is_iter(&self) -> bool {
        self.mode.is_iter
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }

    fn udf(&self, py: Python) -> Result<Py<PyAny>> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            Ok(PySpark::group_map_udf(
                py,
                udf,
                self.input_names.clone(),
                self.is_pandas(),
                self.is_iter(),
                &self.config,
            )?
            .unbind())
        })?;
        Ok(udf.clone_ref(py))
    }
}

impl AggregateUDFImpl for PySparkGroupMapUDF {
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
        let field = get_list_field(&self.output_type)?;
        let udf = Python::attach(|py| self.udf(py))?;
        let aggregator = Box::new(PySparkGroupMapper {
            udf,
            field,
            assign_columns_by_name: self.config.pandas_grouped_map_assign_columns_by_name,
            large_var_types: self.config.arrow_use_large_var_types,
            session_timezone: self.config.session_timezone.clone(),
        });
        Ok(Box::new(BatchAggregateAccumulator::new(
            self.input_types.clone(),
            self.output_type.clone(),
            aggregator,
            self.input_types.len(), // all inputs are real (no dummy)
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        BatchAggregateAccumulator::state_fields(args)
    }
}

struct PySparkGroupMapper {
    udf: Py<PyAny>,
    field: FieldRef,
    assign_columns_by_name: bool,
    large_var_types: bool,
    session_timezone: String,
}

pub(super) fn cast_group_map_output(
    array: &ArrayRef,
    target_type: &DataType,
    assign_columns_by_name: bool,
) -> Result<ArrayRef> {
    if assign_columns_by_name {
        cast_array_recursively(array, target_type)
    } else {
        cast_array_positionally_recursively(array, target_type)
    }
}

impl BatchAggregator for PySparkGroupMapper {
    fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let data = Python::attach(|py| -> PyUdfResult<_> {
            let output = self.udf.call1(
                py,
                (args.try_to_py(py, self.large_var_types, Some(&self.session_timezone))?,),
            )?;
            Ok(ArrayData::try_from_py(py, &output)?)
        })?;
        let array = cast_group_map_output(
            &make_array(data),
            self.field.data_type(),
            self.assign_columns_by_name,
        )?;
        Ok(build_singleton_list_array(array))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};

    use super::*;

    fn target_type() -> DataType {
        DataType::Struct(
            vec![
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Int64, true)),
            ]
            .into(),
        )
    }

    #[test]
    fn output_columns_are_assigned_positionally_when_configured() {
        let fields: Fields = vec![
            Arc::new(Field::new("x", DataType::Utf8, false)),
            Arc::new(Field::new("y", DataType::Int64, false)),
        ]
        .into();
        let input: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["hi"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
            None,
        ));

        let output = cast_group_map_output(&input, &target_type(), false).unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        let a = output
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let b = output
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(a.value(0), "hi");
        assert_eq!(b.value(0), 1);
    }
}
