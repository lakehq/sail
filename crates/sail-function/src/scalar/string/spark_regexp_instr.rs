use std::sync::Arc;

use datafusion::arrow::array::make_array;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::regex::regexpinstr::regexp_instr_func;

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpInstr {
    signature: Signature,
}

impl Default for SparkRegexpInstr {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpInstr {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpInstr {
    fn name(&self) -> &str {
        "spark_regexp_instr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(|arrays| {
            let [string, pattern, index] = arrays else {
                return exec_err!("regexp_instr requires 3 resolved arguments");
            };
            // The index participates in evaluation and NULL propagation, but not the search.
            let nulls = NullBuffer::union(string.nulls(), index.logical_nulls().as_ref());
            let string = make_array(string.to_data().into_builder().nulls(nulls).build()?);
            let result = regexp_instr_func(&[string, pattern.clone()])?;
            Ok(cast(result.as_ref(), &DataType::Int32)?)
        })(&args.args)
    }
}
