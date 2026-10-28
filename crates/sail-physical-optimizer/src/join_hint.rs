use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, NullEquality, Result};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::utils::{JoinFilter, JoinOn};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
    SortMergeJoinExec,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use log::warn;
use sail_physical_plan::join_hint::{JoinHintExec, JoinStrategy};

#[derive(Debug)]
pub struct ApplyJoinHints;

impl PhysicalOptimizerRule for ApplyJoinHints {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !plan.exists(|node| Ok(node.is::<JoinHintExec>()))? {
            return Ok(plan);
        }
        let (plan, unused) = resolve(plan)?;
        if let Some(strategy) = unused {
            warn!("Join hint {strategy:?} has no enclosing join");
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "ApplyJoinHints"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

fn resolve(plan: Arc<dyn ExecutionPlan>) -> Result<(Arc<dyn ExecutionPlan>, Option<JoinStrategy>)> {
    if let Some(hint) = plan.downcast_ref::<JoinHintExec>() {
        let (input, _) = resolve(Arc::clone(&hint.input))?;
        return Ok((input, Some(hint.strategy)));
    }
    if plan.children().is_empty() {
        return Ok((plan, None));
    }
    let (children, hints): (Vec<_>, Vec<_>) = plan
        .children()
        .into_iter()
        .map(|child| resolve(Arc::clone(child)))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();
    let plan = replace_children_if_necessary(plan, children)?;
    if hints.iter().all(Option::is_none) {
        return Ok((plan, None));
    }
    if let Some(join) = JoinInput::from_plan(&plan)? {
        return Ok((join.apply(plan, hints[0], hints[1])?, None));
    }
    if let [hint] = hints.as_slice() {
        return Ok((plan, *hint));
    }
    for strategy in hints.into_iter().flatten() {
        warn!("Join hint {strategy:?} has no enclosing join in this input");
    }
    Ok((plan, None))
}

struct JoinInput {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    null_equality: NullEquality,
    null_aware: bool,
    projection: Option<Vec<usize>>,
}

impl JoinInput {
    fn from_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Self>> {
        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            return Ok(Some(Self {
                left: Arc::clone(&join.left),
                right: Arc::clone(&join.right),
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                null_equality: join.null_equality,
                null_aware: join.null_aware,
                projection: join.projection.as_ref().map(|p| p.to_vec()),
            }));
        }
        if let Some(join) = plan.downcast_ref::<SortMergeJoinExec>() {
            return Ok(Some(Self {
                left: Arc::clone(&join.left),
                right: Arc::clone(&join.right),
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                null_equality: join.null_equality,
                null_aware: false,
                projection: None,
            }));
        }
        if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
            return Ok(Some(Self {
                left: Arc::clone(join.left()),
                right: Arc::clone(join.right()),
                on: vec![],
                filter: join.filter().cloned(),
                join_type: *join.join_type(),
                null_equality: NullEquality::NullEqualsNothing,
                null_aware: false,
                projection: join.projection().as_ref().map(|p| p.to_vec()),
            }));
        }
        if let Some(join) = plan.downcast_ref::<CrossJoinExec>() {
            return Ok(Some(Self {
                left: Arc::clone(join.left()),
                right: Arc::clone(join.right()),
                on: vec![],
                filter: None,
                join_type: JoinType::Inner,
                null_equality: NullEquality::NullEqualsNothing,
                null_aware: false,
                projection: None,
            }));
        }
        if let Some(join) = plan.downcast_ref::<PiecewiseMergeJoinExec>() {
            let mut input = Self {
                left: Arc::clone(&join.buffered),
                right: Arc::clone(&join.streamed),
                on: vec![],
                filter: None,
                join_type: join.join_type,
                null_equality: NullEquality::NullEqualsNothing,
                null_aware: false,
                projection: None,
            };
            let expr = Arc::new(BinaryExpr::new(
                Arc::clone(&join.on.0),
                join.operator,
                offset_columns(Arc::clone(&join.on.1), input.left.schema().fields().len())?,
            ));
            input.filter = Some(input.full_filter(expr));
            return Ok(Some(input));
        }
        Ok(None)
    }

    fn apply(
        &self,
        original: Arc<dyn ExecutionPlan>,
        left: Option<JoinStrategy>,
        right: Option<JoinStrategy>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.null_aware {
            return Ok(original);
        }
        let mut strategies = [left, right].into_iter().flatten().collect::<Vec<_>>();
        strategies.sort();
        strategies.dedup();
        for strategy in strategies {
            let hinted_left = left == Some(strategy);
            let hinted_right = right == Some(strategy);
            match strategy {
                JoinStrategy::Broadcast | JoinStrategy::ShuffleHash if !self.on.is_empty() => {
                    let broadcast = strategy == JoinStrategy::Broadcast;
                    let can_left = !broadcast
                        || matches!(
                            self.join_type,
                            JoinType::Inner
                                | JoinType::Right
                                | JoinType::RightSemi
                                | JoinType::RightAnti
                        );
                    let can_right = !broadcast
                        || matches!(
                            self.join_type,
                            JoinType::Inner
                                | JoinType::Left
                                | JoinType::LeftSemi
                                | JoinType::LeftAnti
                        );
                    let build_left = hinted_left && can_left;
                    let build_right = hinted_right && can_right;
                    if !build_left && !build_right {
                        warn!(
                            "Join hint {strategy:?} does not support the requested build side for {:?}",
                            self.join_type
                        );
                        continue;
                    }
                    let swap = build_right && (!build_left || self.right_is_smaller()?);
                    let mode = if broadcast {
                        PartitionMode::CollectLeft
                    } else {
                        PartitionMode::Partitioned
                    };
                    let join = HashJoinExec::try_new(
                        Arc::clone(&self.left),
                        Arc::clone(&self.right),
                        self.on.clone(),
                        self.filter.clone(),
                        &self.join_type,
                        self.projection.clone(),
                        mode,
                        self.null_equality,
                        false,
                    )?;
                    return if swap {
                        join.swap_inputs(mode)
                    } else {
                        Ok(Arc::new(join))
                    };
                }
                JoinStrategy::Merge if !self.on.is_empty() => {
                    let join = Arc::new(SortMergeJoinExec::try_new(
                        Arc::clone(&self.left),
                        Arc::clone(&self.right),
                        self.on.clone(),
                        self.filter.as_ref().map(sort_merge_filter).transpose()?,
                        self.join_type,
                        vec![SortOptions::default(); self.on.len()],
                        self.null_equality,
                    )?);
                    return self.project(join);
                }
                JoinStrategy::Broadcast | JoinStrategy::ShuffleReplicateNl => {
                    if strategy == JoinStrategy::ShuffleReplicateNl
                        && self.join_type != JoinType::Inner
                    {
                        warn!("SHUFFLE_REPLICATE_NL requires an inner join");
                        continue;
                    }
                    let swap = hinted_right && (!hinted_left || self.right_is_smaller()?);
                    let join = NestedLoopJoinExec::try_new(
                        Arc::clone(&self.left),
                        Arc::clone(&self.right),
                        self.nested_loop_filter()?,
                        &self.join_type,
                        self.projection.clone(),
                    )?;
                    return if swap {
                        join.swap_inputs()
                    } else {
                        Ok(Arc::new(join))
                    };
                }
                _ => warn!("Join hint {strategy:?} requires equi-join keys"),
            }
        }
        Ok(original)
    }

    fn right_is_smaller(&self) -> Result<bool> {
        let context = StatisticsContext::new();
        let left = context.compute(self.left.as_ref(), &StatisticsArgs::new())?;
        let right = context.compute(self.right.as_ref(), &StatisticsArgs::new())?;
        Ok(
            match (
                left.total_byte_size.get_value(),
                right.total_byte_size.get_value(),
            ) {
                (Some(l), Some(r)) => r < l,
                _ => match (left.num_rows.get_value(), right.num_rows.get_value()) {
                    (Some(l), Some(r)) => r < l,
                    _ => false,
                },
            },
        )
    }

    fn project(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(projection) = &self.projection else {
            return Ok(plan);
        };
        let schema = plan.schema();
        let exprs = projection
            .iter()
            .map(|&i| {
                let name = schema.field(i).name();
                (
                    Arc::new(Column::new(name, i)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    fn full_filter(&self, expression: Arc<dyn PhysicalExpr>) -> JoinFilter {
        let left = self.left.schema();
        let right = self.right.schema();
        let fields = left
            .fields()
            .iter()
            .chain(right.fields().iter())
            .cloned()
            .collect::<Vec<_>>();
        JoinFilter::new(
            expression,
            JoinFilter::build_column_indices(
                (0..left.fields().len()).collect(),
                (0..right.fields().len()).collect(),
            ),
            Arc::new(Schema::new(fields)),
        )
    }

    fn nested_loop_filter(&self) -> Result<Option<JoinFilter>> {
        let left_len = self.left.schema().fields().len();
        let mut expression = self
            .filter
            .as_ref()
            .map(|filter| {
                Arc::clone(filter.expression())
                    .transform_up(|expr| {
                        if let Some(column) = expr.downcast_ref::<Column>() {
                            let index = &filter.column_indices()[column.index()];
                            let offset = if index.side == datafusion::common::JoinSide::Left {
                                0
                            } else {
                                left_len
                            };
                            return Ok(Transformed::yes(Arc::new(Column::new(
                                column.name(),
                                index.index + offset,
                            ))
                                as Arc<dyn PhysicalExpr>));
                        }
                        Ok(Transformed::no(expr))
                    })
                    .map(|x| x.data)
            })
            .transpose()?;
        for (left, right) in &self.on {
            let op = if self.null_equality == NullEquality::NullEqualsNull {
                Operator::IsNotDistinctFrom
            } else {
                Operator::Eq
            };
            let condition = Arc::new(BinaryExpr::new(
                Arc::clone(left),
                op,
                offset_columns(Arc::clone(right), left_len)?,
            )) as Arc<dyn PhysicalExpr>;
            expression = Some(match expression {
                Some(previous) => Arc::new(BinaryExpr::new(previous, Operator::And, condition)),
                None => condition,
            });
        }
        Ok(expression.map(|expression| self.full_filter(expression)))
    }
}

fn offset_columns(
    expression: Arc<dyn PhysicalExpr>,
    offset: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(expression
        .transform_up(|expr| {
            if let Some(column) = expr.downcast_ref::<Column>() {
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), column.index() + offset))
                        as Arc<dyn PhysicalExpr>,
                ))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

/// SortMergeJoin evaluates filter columns with all left fields before right fields.
/// HashJoin input swapping can leave the intermediate filter schema in the opposite order.
fn sort_merge_filter(filter: &JoinFilter) -> Result<JoinFilter> {
    use datafusion::common::JoinSide;

    let positions = [JoinSide::Left, JoinSide::Right]
        .into_iter()
        .flat_map(|side| {
            filter
                .column_indices()
                .iter()
                .enumerate()
                .filter_map(move |(i, col)| (col.side == side).then_some(i))
        })
        .collect::<Vec<_>>();
    let mut mapping = vec![0; positions.len()];
    for (new, &old) in positions.iter().enumerate() {
        mapping[old] = new;
    }
    let expression = Arc::clone(filter.expression())
        .transform_up(|expr| {
            if let Some(column) = expr.downcast_ref::<Column>() {
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), mapping[column.index()]))
                        as Arc<dyn PhysicalExpr>,
                ))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data;
    let fields = positions
        .iter()
        .map(|&i| Arc::clone(&filter.schema().fields()[i]))
        .collect::<Vec<_>>();
    let indices = positions
        .into_iter()
        .map(|i| filter.column_indices()[i].clone())
        .collect();
    Ok(JoinFilter::new(
        expression,
        indices,
        Arc::new(Schema::new_with_metadata(
            fields,
            filter.schema().metadata().clone(),
        )),
    ))
}
