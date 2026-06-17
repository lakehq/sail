use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{not_impl_err, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct GroupingIdFunction {
    signature: Signature,
}

impl Default for GroupingIdFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupingIdFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for GroupingIdFunction {
    fn name(&self) -> &str {
        "grouping_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("grouping_id", DataType::Int64, false).into()
        ])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("grouping_id should be rewritten by planning")
    }
}
