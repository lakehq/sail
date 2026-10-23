use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Aggregate, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct StreamAggregateNode {
    input: Arc<LogicalPlan>,
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl StreamAggregateNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Self> {
        if group_expr
            .iter()
            .chain(&aggr_expr)
            .any(|expr| expr.is_volatile())
        {
            return plan_err!("volatile streaming aggregate expressions are not supported");
        }
        if group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_)))
        {
            return plan_err!("streaming grouping sets are not supported");
        }
        let aggregate = Aggregate::try_new(input.clone(), group_expr.clone(), aggr_expr.clone())?;
        let flow_schema = to_flow_event_schema(aggregate.schema.inner());
        let fields = flow_schema
            .fields()
            .iter()
            .take(2)
            .map(|field| (None, field.clone()))
            .chain(
                aggregate
                    .schema
                    .iter()
                    .map(|(qualifier, field)| (qualifier.cloned(), field.clone())),
            )
            .collect();
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            aggregate.schema.metadata().clone(),
        )?);
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }

    pub fn group_expr(&self) -> &[Expr] {
        &self.group_expr
    }
    pub fn aggr_expr(&self) -> &[Expr] {
        &self.aggr_expr
    }
}

impl UserDefinedLogicalNodeCore for StreamAggregateNode {
    fn name(&self) -> &str {
        "StreamAggregate"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(&self.aggr_expr)
            .cloned()
            .collect()
    }
    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StreamAggregate: groupBy=[{}], aggr=[{}]",
            self.group_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.aggr_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return plan_err!("StreamAggregate expects exactly one input");
        };
        if exprs.len() != self.group_expr.len() + self.aggr_expr.len() {
            return plan_err!("StreamAggregate expression count changed");
        }
        let aggr_expr = exprs.split_off(self.group_expr.len());
        Self::try_new(Arc::new(input), exprs, aggr_expr)
    }
    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
