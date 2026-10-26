use std::sync::Arc;

use self::TableFunctionBuilder as F;
use crate::analyzer::model::{Expr, Query};
use crate::error::PlanResult;
use crate::function::common::FunctionContextInput;

pub(crate) mod range;

pub(crate) struct TableFunctionInput<'a> {
    pub arguments: Vec<Expr>,
    pub context: FunctionContextInput<'a>,
}
pub(crate) type TableFunction =
    Arc<dyn Fn(TableFunctionInput<'_>) -> PlanResult<Query> + Send + Sync>;

pub(crate) fn list_built_in_table_functions() -> Vec<(&'static str, TableFunction)> {
    vec![("range", F::custom(range::range))]
}

pub(crate) struct TableFunctionBuilder;
impl TableFunctionBuilder {
    pub fn custom(
        function: impl Fn(TableFunctionInput<'_>) -> PlanResult<Query> + Send + Sync + 'static,
    ) -> TableFunction {
        Arc::new(function)
    }
}
