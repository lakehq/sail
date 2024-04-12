use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature,
};

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,
}

impl PythonUDF {}

impl ScalarUDFImpl for PythonUDF {}