//! Remove generated reductions made adjacent to their source join by reordering.

use super::*;

pub(super) fn prune(
    plan: Arc<dyn ExecutionPlan>,
    reductions: &Reductions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|child| prune(Arc::clone(child), reductions))
        .collect::<Result<Vec<_>>>()?;
    let plan = replace_children_if_necessary(plan, children)?;
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    if !matches!(
        join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    ) {
        return Ok(plan);
    }
    let mut children = vec![Arc::clone(join.left()), Arc::clone(join.right())];
    for source_side in 0..2 {
        let pairs = join
            .on()
            .iter()
            .filter_map(|(left, right)| {
                let left = left.downcast_ref::<Column>()?.index();
                let right = right.downcast_ref::<Column>()?.index();
                Some(if source_side == 0 {
                    (left, right)
                } else {
                    (right, left)
                })
            })
            .collect::<Vec<_>>();
        children[1 - source_side] = remove_adjacent(
            Arc::clone(&children[1 - source_side]),
            &children[source_side],
            &pairs,
            join.null_equality(),
            reductions,
        )?;
    }
    replace_children_if_necessary(plan, children)
}

fn remove_adjacent(
    plan: Arc<dyn ExecutionPlan>,
    source: &Arc<dyn ExecutionPlan>,
    pairs: &[(usize, usize)],
    null_equality: NullEquality,
    reductions: &Reductions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.fetch().is_some() {
        return Ok(plan);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        if projection.expr().iter().any(|p| !p.expr.is::<Column>()) {
            return Ok(plan);
        }
        let mapped = pairs
            .iter()
            .filter_map(|&(s, t)| {
                Some((
                    s,
                    projection.expr()[t].expr.downcast_ref::<Column>()?.index(),
                ))
            })
            .collect::<Vec<_>>();
        let child = remove_adjacent(
            Arc::clone(projection.input()),
            source,
            &mapped,
            null_equality,
            reductions,
        )?;
        return replace_children_if_necessary(plan, vec![child]);
    }
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    if join.join_type() != &JoinType::RightSemi
        || join.projection.is_some()
        || join.filter().is_some()
        || join.null_equality() != null_equality
    {
        return Ok(plan);
    }
    let Some(reduction) = reductions.generated.iter().find(|r| {
        join.on()
            .first()
            .is_some_and(|(key, _)| Arc::ptr_eq(key, &r.marker))
    }) else {
        // Never infer provenance from aliases or delete a user-written semijoin.
        return Ok(plan);
    };
    let implied = reduction.source_keys.len() == join.on().len()
        && reduction
            .source_keys
            .iter()
            .zip(join.on())
            .all(|(&source_key, (_, target_key))| {
                let Some(target_key) = target_key.downcast_ref::<Column>() else {
                    return false;
                };
                pairs.iter().any(|&(s, t)| {
                    t == target_key.index()
                        && source_column(source, s, &reduction.source) == Some(source_key)
                })
            });
    if implied {
        log::trace!("JoinReorder: removing an adjacent generated semijoin");
        return remove_adjacent(
            Arc::clone(join.right()),
            source,
            pairs,
            null_equality,
            reductions,
        );
    }
    // In particular, do not descend through a different join, aggregation,
    // filter, or exchange: the reduction may save work in that operator.
    Ok(plan)
}

fn source_column(
    plan: &Arc<dyn ExecutionPlan>,
    index: usize,
    original: &Arc<dyn ExecutionPlan>,
) -> Option<usize> {
    if Arc::ptr_eq(plan, original) {
        return Some(index);
    }
    if plan.fetch().is_some() {
        return None;
    }
    let projection = plan.downcast_ref::<ProjectionExec>()?;
    let column = projection
        .expr()
        .get(index)?
        .expr
        .downcast_ref::<Column>()?;
    source_column(projection.input(), column.index(), original)
}
