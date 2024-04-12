use std::sync::Arc;
use std::any::Any;

use datafusion::arrow::datatypes::{DataType as ArrowDataType};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature, expr,
    ScalarFunctionDefinition, Volatility,
};

use pyo3::{PyResult, Python};
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{IntoPyDict, PyBytes, PyTuple};

// TODO: Use `use datafusion::arrow::datatypes::{DataType}` instead?
// use framework_proto::spark::connect::DataType as SCDataType;

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,

    // TODO: See what I exactly need. This is a placeholder.
    function_name: String,
    arguments: Vec<expr::Expr>,
    input_types: Vec<ArrowDataType>,
    output_type: ArrowDataType,
    eval_type: i32,
    command: Vec<u8>,
    python_ver: String,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        arguments: Vec<expr::Expr>,
        input_types: Vec<ArrowDataType>,
        command: Vec<u8>,
        output_type: ArrowDataType,
        eval_type: i32,
        python_ver: String,
    ) -> Self {
        Self {
            signature: Signature::exact(
                input_types.clone(),
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            arguments,
            input_types,
            command,
            output_type,
            eval_type,
            python_ver,
        }
    }

    pub fn to_scalar_function(&self) -> expr::Expr {
        expr::Expr::ScalarFunction(expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(self.clone()))),
            args: self.arguments.clone(),
        })
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

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != &self.input_types[..] {
            return Err(DataFusionError::Internal(format!("Input types do not match the expected types")));
        }
        Ok(self.output_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        Python::with_gil(|py| {
            let cloudpickle = PyModule::import_bound(py, "pyspark.cloudpickle") // TODO: make name a variable instead of hardcoding
                .expect("Unable to import 'pyspark.cloudpickle'")
                .getattr("loads")
                .unwrap();

            let binary_sequence = PyBytes::new_bound(py, &self.command);
            let python_function = cloudpickle
                .call1((binary_sequence, ))?;

            // Convert each ColumnarValue in args to an array and collect them into a tuple
            let args_tuple: PyTuple = PyTuple::new_bound(
                py,
                args.iter()
                    .map(|arg| arg.to_array())
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );

            // Call the Python function with the tuple of arguments
            let result = python_function.call1(args_tuple)?;

            // Convert the result back to a ColumnarValue and return it
            ColumnarValue::from_array(result.extract()?)
        })
    }
}