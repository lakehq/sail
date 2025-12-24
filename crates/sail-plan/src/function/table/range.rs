use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{logical_plan, Expr, LogicalPlan, TableType, UserDefinedLogicalNodeCore};
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
use sail_logical_plan::range::RangeNode;
use sail_physical_plan::range::RangeExec;

#[derive(Debug)]
struct RangeTableProvider {
    node: Arc<RangeNode>,
}

#[tonic::async_trait]
impl TableProvider for RangeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.node.schema().inner().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        Some(Cow::Owned(LogicalPlan::Extension(
            logical_plan::Extension {
                node: self.node.clone(),
            },
        )))
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RangeExec::new(
            self.node.range().clone(),
            self.node.num_partitions(),
            self.node.schema().inner().clone(),
        )))
    }
}

pub struct RangeTableFunction {
    evaluator: LiteralEvaluator,
}

impl Debug for RangeTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeTableFunction").finish()
    }
}

impl RangeTableFunction {
    pub fn new() -> Self {
        let evaluator = LiteralEvaluator::new();
        Self { evaluator }
    }
}

impl TableFunctionImpl for RangeTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let args = args
            .iter()
            .map(|x| self.evaluator.evaluate(x))
            .collect::<Result<Vec<_>>>()?;
        let (start, end, step, num_partitions) = match args.len() {
            1 => (0, LiteralValue(&args[0]).try_to_i64()?, 1, 1),
            2 => (
                LiteralValue(&args[0]).try_to_i64()?,
                LiteralValue(&args[1]).try_to_i64()?,
                1,
                1,
            ),
            3 => (
                LiteralValue(&args[0]).try_to_i64()?,
                LiteralValue(&args[1]).try_to_i64()?,
                LiteralValue(&args[2]).try_to_i64()?,
                1,
            ),
            4 => (
                LiteralValue(&args[0]).try_to_i64()?,
                LiteralValue(&args[1]).try_to_i64()?,
                LiteralValue(&args[2]).try_to_i64()?,
                LiteralValue(&args[3]).try_to_usize()?,
            ),
            _ => {
                return exec_err!(
                    "expected 1 to 4 arguments for range, but got {}",
                    args.len()
                )
            }
        };
        let node = RangeNode::try_new("id".to_string(), start, end, step, num_partitions)?;
        Ok(Arc::new(RangeTableProvider {
            node: Arc::new(node),
        }))
    }
}
