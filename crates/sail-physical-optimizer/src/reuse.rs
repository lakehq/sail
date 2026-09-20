//! Conservative common physical subplan recognition. Unknown operators and
//! expressions are barriers, not evidence that two computations are equivalent.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::{AggregateUDF, Volatility};
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr,
    Literal, NegativeExpr, NotExpr, TryCastExpr, UnKnownColumn,
};
use datafusion::physical_expr::{Partitioning, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::buffer::BufferExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, replace_children_if_necessary,
};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use sail_common_datafusion::file_scan_identity::ParquetScanIdentity;
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::shared::SharedPlanExec;

#[derive(PartialEq, Eq, Hash)]
struct Key {
    kind: &'static str,
    children: Vec<usize>,
    schema: SchemaRef,
    settings: Vec<String>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    aggregates: Vec<AggregateUDF>,
    source: Vec<u8>,
}

#[derive(Debug)]
pub struct ReuseSubplans;

impl PhysicalOptimizerRule for ReuseSubplans {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        reuse_subplans(plan)
    }
    fn name(&self) -> &str {
        "reuse_subplans"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

fn schema_key(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| field.as_ref().clone().with_name(i.to_string()))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

/// Column names are labels at this point: evaluation uses the column index.
/// Preserve all other expression state through the expression's equality/hash.
fn expression_key(
    expr: Arc<dyn PhysicalExpr>,
    partitioning: bool,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let mut valid = true;
    let expr = expr
        .transform_up(|expr| {
            // Partitioning can refer to keys projected out of the output. These
            // placeholders are labels, never expressions evaluated on batches.
            if partitioning && expr.is::<UnKnownColumn>() {
                return Ok(Transformed::yes(
                    // UnKnownColumn deliberately compares unequal to itself.
                    // Use a reserved, non-evaluable column only in this key.
                    Arc::new(Column::new("projected_partition_key", usize::MAX))
                        as Arc<dyn PhysicalExpr>,
                ));
            }
            if let Some(column) = expr.downcast_ref::<Column>() {
                return Ok(Transformed::yes(
                    Arc::new(Column::new("", column.index())) as Arc<dyn PhysicalExpr>
                ));
            }
            let supported = expr.is::<Literal>()
                || expr.is::<BinaryExpr>()
                || expr.is::<CastExpr>()
                || expr.is::<TryCastExpr>()
                || expr.is::<CaseExpr>()
                || expr.is::<InListExpr>()
                || expr.is::<IsNullExpr>()
                || expr.is::<IsNotNullExpr>()
                || expr.is::<NotExpr>()
                || expr.is::<NegativeExpr>()
                || expr.is::<LikeExpr>()
                || expr
                    .downcast_ref::<ScalarFunctionExpr>()
                    .is_some_and(|function| {
                        function.fun().signature().volatility != Volatility::Volatile
                    });
            valid &= supported;
            Ok(Transformed::no(expr))
        })?
        .data;
    Ok(valid.then_some(expr))
}

impl Key {
    fn expr(&mut self, expr: &Arc<dyn PhysicalExpr>) -> Result<bool> {
        if let Some(expr) = expression_key(expr.clone(), false)? {
            self.expressions.push(expr);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn ordering(&mut self, ordering: &[PhysicalSortExpr]) -> Result<bool> {
        self.settings.push(format!("ordering:{}", ordering.len()));
        for sort in ordering {
            if !self.expr(&sort.expr)? {
                return Ok(false);
            }
            self.settings.push(format!("{:?}", sort.options));
        }
        Ok(true)
    }

    fn partitioning(&mut self, partitioning: &Partitioning) -> Result<bool> {
        match partitioning {
            Partitioning::UnknownPartitioning(n) => self.settings.push(format!("unknown:{n}")),
            Partitioning::RoundRobinBatch(n) => self.settings.push(format!("round_robin:{n}")),
            Partitioning::Hash(exprs, n) => {
                self.settings.push(format!("hash:{n}:{}", exprs.len()));
                for expr in exprs {
                    if let Some(expr) = expression_key(expr.clone(), true)? {
                        self.expressions.push(expr);
                    } else {
                        return Ok(false);
                    }
                }
            }
            // Range bounds are intentionally not approximated by an ordering.
            Partitioning::Range(_) => return Ok(false),
        }
        Ok(true)
    }
}

fn node_key(
    plan: &Arc<dyn ExecutionPlan>,
    children: Vec<usize>,
    used_dynamic: &HashSet<u64>,
) -> Result<Option<Key>> {
    if !matches!(plan.boundedness(), Boundedness::Bounded) || plan.fetch().is_some() {
        return Ok(None);
    }
    // Sharing must not suppress publication of a filter another operator uses.
    // DataFusion also attaches filters that were never pushed into a consumer;
    // those have no effect on the producer's output and do not prevent reuse.
    if plan.dynamic_expressions_produced().iter().any(|expr| {
        expr.expression_id()
            .is_none_or(|id| used_dynamic.contains(&id))
    }) {
        return Ok(None);
    }
    let mut key = Key {
        kind: "",
        children,
        schema: schema_key(&plan.schema()),
        settings: vec![],
        expressions: vec![],
        aggregates: vec![],
        source: vec![],
    };
    if !key.partitioning(plan.output_partitioning())? {
        return Ok(None);
    }
    if let Some(ordering) = plan.output_ordering() {
        if !key.ordering(ordering)? {
            return Ok(None);
        }
    } else {
        key.settings.push("no_ordering".into());
    }

    if let Some(node) = plan.downcast_ref::<ProjectionExec>() {
        key.kind = "projection";
        for expr in node.expr() {
            if !key.expr(&expr.expr)? {
                return Ok(None);
            }
        }
    } else if let Some(node) = plan.downcast_ref::<FilterExec>() {
        key.kind = "filter";
        if !key.expr(node.predicate())? {
            return Ok(None);
        }
        key.settings.push(format!("{:?}", node.projection()));
    } else if let Some(node) = plan.downcast_ref::<RepartitionExec>() {
        key.kind = "repartition";
        key.settings.push(node.preserve_order().to_string());
    } else if plan.is::<CoalescePartitionsExec>() {
        key.kind = "coalesce";
    } else if let Some(node) = plan.downcast_ref::<SortExec>() {
        key.kind = "sort";
        if !key.ordering(node.expr())? {
            return Ok(None);
        }
        key.settings.push(node.preserve_partitioning().to_string());
    } else if let Some(node) = plan.downcast_ref::<SortPreservingMergeExec>() {
        key.kind = "merge";
        if !key.ordering(node.expr())? {
            return Ok(None);
        }
    } else if let Some(node) = plan.downcast_ref::<HashJoinExec>() {
        key.kind = "hash_join";
        key.settings.extend([
            format!("{:?}", node.mode),
            format!("{:?}", node.join_type),
            format!("{:?}", node.null_equality),
            node.null_aware.to_string(),
            format!("{:?}", node.projection),
            node.on.len().to_string(),
        ]);
        for (left, right) in &node.on {
            if !key.expr(left)? || !key.expr(right)? {
                return Ok(None);
            }
        }
        if let Some(filter) = node.filter() {
            if !key.expr(filter.expression())? {
                return Ok(None);
            }
            key.settings.push(format!("{:?}", filter.column_indices()));
            key.settings
                .push(format!("{:?}", schema_key(filter.schema())));
        } else {
            key.settings.push("no_filter".into());
        }
    } else if let Some(node) = plan.downcast_ref::<AggregateExec>() {
        key.kind = "aggregate";
        key.settings.extend([
            format!("{:?}", node.mode()),
            format!("{:?}", node.group_expr().groups()),
            format!("{:?}", node.limit_options()),
            format!("{:?}", node.input_order_mode()),
            node.group_expr().expr().len().to_string(),
            node.group_expr().null_expr().len().to_string(),
        ]);
        for (expr, _) in node
            .group_expr()
            .expr()
            .iter()
            .chain(node.group_expr().null_expr())
        {
            if !key.expr(expr)? {
                return Ok(None);
            }
        }
        for aggregate in node.aggr_expr() {
            if aggregate.fun().signature().volatility == Volatility::Volatile {
                return Ok(None);
            }
            key.aggregates.push(aggregate.fun().clone());
            key.settings.extend([
                aggregate.is_distinct().to_string(),
                aggregate.ignore_nulls().to_string(),
                aggregate.is_reversed().to_string(),
                format!("{:?}", aggregate.field().data_type()),
                aggregate.expressions().len().to_string(),
            ]);
            for expr in aggregate.expressions() {
                if !key.expr(&expr)? {
                    return Ok(None);
                }
            }
            if !key.ordering(aggregate.order_bys())? {
                return Ok(None);
            }
        }
        for filter in node.filter_expr() {
            key.settings.push(filter.is_some().to_string());
            if let Some(filter) = filter
                && !key.expr(filter)?
            {
                return Ok(None);
            }
        }
    } else if plan.is::<CrossJoinExec>() {
        key.kind = "cross_join";
    } else if plan.is::<UnionExec>() {
        key.kind = "union";
    } else if plan.is::<CooperativeExec>() {
        key.kind = "cooperative";
    } else if let Some(node) = plan.downcast_ref::<BufferExec>() {
        key.kind = "buffer";
        key.settings.push(node.capacity().to_string());
    } else if let Some(node) = plan.downcast_ref::<RangeExec>() {
        key.kind = "range";
        key.settings.extend([
            node.range().start.to_string(),
            node.range().end.to_string(),
            node.range().step.to_string(),
            format!("{:?}", node.projection()),
        ]);
    } else if let Some(node) = plan.downcast_ref::<DataSourceExec>() {
        let source = node.data_source();
        if let Some(memory) = source.downcast_ref::<MemorySourceConfig>() {
            key.kind = "memory";
            key.settings.extend([
                format!("{:?}", memory.original_schema()),
                format!("{:?}", memory.projection()),
            ]);
            for partition in memory.partitions() {
                key.settings.push(format!("partition:{}", partition.len()));
                for batch in partition {
                    key.settings.push(format!("rows:{}", batch.num_rows()));
                    // Array identity is conservative and avoids hashing/serializing
                    // the contents of in-memory tables during optimization.
                    for array in batch.columns() {
                        key.settings.push(format!("{:p}", Arc::as_ptr(array)));
                    }
                }
            }
        } else if let Some(scan) = source.downcast_ref::<FileScanConfig>() {
            let Some(parquet) = scan.file_source.downcast_ref::<ParquetSource>() else {
                return Ok(None);
            };
            if scan.limit.is_some() || parquet.table_parquet_options().crypto.factory_id.is_some() {
                return Ok(None);
            }
            let mut valid = true;
            scan.file_source.apply_expressions(&mut |expr| {
                valid &= expression_key(expr.clone(), false)?.is_some();
                Ok(TreeNodeRecursion::Continue)
            })?;
            if !valid {
                return Ok(None);
            }
            // The built-in codec supplies the complete scan configuration. We
            // additionally include object versions and reject opaque extensions.
            for group in &scan.file_groups {
                for file in group.iter() {
                    if let Some(identity) = file.extensions.get::<ParquetScanIdentity>() {
                        if file.extensions.len() != 1
                            || !parquet
                                .parquet_file_reader_factory()
                                .is_some_and(|factory| {
                                    Arc::ptr_eq(factory, &identity.reader_factory)
                                })
                            || !scan.expr_adapter_factory.as_ref().is_some_and(|factory| {
                                Arc::ptr_eq(factory, &identity.adapter_factory)
                            })
                        {
                            return Ok(None);
                        }
                        key.settings
                            .push(format!("store:{:p}", Arc::as_ptr(&identity.store)));
                    } else if !file.extensions.is_empty()
                        || scan.expr_adapter_factory.is_some()
                        || parquet.parquet_file_reader_factory().is_some()
                    {
                        return Ok(None);
                    }
                    key.settings.push(format!("{:?}", file.object_meta));
                }
            }
            let Ok(mut proto) = PhysicalPlanNode::try_from_physical_plan(
                plan.clone(),
                &DefaultPhysicalExtensionCodec {},
            ) else {
                return Ok(None);
            };
            if let Some(
                datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::ParquetScan(scan),
            ) = &mut proto.physical_plan_type
                && let Some(config) = &mut scan.base_conf
                && let Some(projection) = &mut config.projection_exprs
            {
                for expr in &mut projection.projections {
                    expr.alias.clear();
                }
            }
            key.kind = "parquet";
            key.source = proto.encode_to_vec();
            key.settings
                .push(format!("{:?}", parquet.table_parquet_options()));
        } else {
            return Ok(None);
        }
    } else {
        return Ok(None);
    }
    Ok(Some(key))
}

struct Node {
    plan: Arc<dyn ExecutionPlan>,
    id: usize,
    valid: bool,
    children: Vec<Node>,
}

fn used_dynamic_expressions(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
    let mut used = HashSet::new();
    plan.apply(|node| {
        let produced = node.dynamic_expressions_produced();
        node.apply_expressions(&mut |expr| {
            expr.apply(|expr| {
                // Transformed dynamic filters retain the expression ID even
                // when their wrapper and column mapping differ from the producer.
                if let Some(id) = expr.expression_id()
                    && !produced
                        .iter()
                        .any(|producer| producer.expression_id() == Some(id))
                {
                    used.insert(id);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(used)
}

/// Run once after all other physical rewrites. A hash match always checks the
/// complete structural key; collisions cannot cause two plans to be shared.
pub fn reuse_subplans(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    fn identify(
        plan: Arc<dyn ExecutionPlan>,
        keys: &mut HashMap<(usize, Key), usize>,
        next: &mut usize,
        scope: usize,
        next_scope: &mut usize,
        used_dynamic: &HashSet<u64>,
    ) -> Result<Node> {
        let children = plan
            .children()
            .into_iter()
            .map(|child| {
                let child_scope = if plan.is::<BarrierExec>() {
                    *next_scope += 1;
                    *next_scope
                } else {
                    scope
                };
                identify(
                    child.clone(),
                    keys,
                    next,
                    child_scope,
                    next_scope,
                    used_dynamic,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let key = if children.iter().all(|child| child.valid) {
            node_key(
                &plan,
                children.iter().map(|child| child.id).collect(),
                used_dynamic,
            )?
        } else {
            None
        };
        let valid = key.is_some();
        let id = if let Some(key) = key {
            *keys.entry((scope, key)).or_insert_with(|| {
                let id = *next;
                *next += 1;
                id
            })
        } else {
            let id = *next;
            *next += 1;
            id
        };
        Ok(Node {
            plan,
            id,
            valid,
            children,
        })
    }
    fn count(node: &Node, counts: &mut [usize]) {
        counts[node.id] += 1;
        // Descendants of a duplicate producer are not executed again.
        if counts[node.id] == 1 {
            for child in &node.children {
                count(child, counts);
            }
        }
    }
    fn rewrite(node: Node, counts: &[usize]) -> Result<Arc<dyn ExecutionPlan>> {
        let children = node
            .children
            .into_iter()
            .map(|child| rewrite(child, counts))
            .collect::<Result<Vec<_>>>()?;
        let plan = replace_children_if_necessary(node.plan, children)?;
        if counts[node.id] > 1 {
            Ok(Arc::new(SharedPlanExec::new(node.id, plan)))
        } else {
            Ok(plan)
        }
    }
    // Reoptimization must not mix independently assigned producer IDs.
    let plan = plan
        .transform_up(|plan| {
            if let Some(shared) = plan.downcast_ref::<SharedPlanExec>() {
                Ok(Transformed::yes(shared.input().clone()))
            } else {
                Ok(Transformed::no(plan))
            }
        })?
        .data;
    let mut next = 0;
    let used_dynamic = used_dynamic_expressions(&plan)?;
    let root = identify(
        plan,
        &mut HashMap::new(),
        &mut next,
        0,
        &mut 0,
        &used_dynamic,
    )?;
    let mut counts = vec![0; next];
    count(&root, &mut counts);
    rewrite(root, &counts)
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::displayable;

    #[test]
    fn projected_out_partition_keys_have_stable_identity() -> Result<()> {
        let left = expression_key(Arc::new(UnKnownColumn::new("left")), true)?;
        let right = expression_key(Arc::new(UnKnownColumn::new("right")), true)?;
        assert!(left.is_some());
        assert_eq!(left, right);
        assert!(expression_key(Arc::new(UnKnownColumn::new("value")), false)?.is_none());
        Ok(())
    }

    use super::*;

    fn duplicate_sources() -> Result<Arc<dyn ExecutionPlan>> {
        let source = MemorySourceConfig::try_new_exec(&[vec![]], Arc::new(Schema::empty()), None)?;
        UnionExec::try_new(vec![source.clone(), source])
    }

    #[test]
    fn reoptimization_preserves_reuse_ids() -> Result<()> {
        let once = reuse_subplans(duplicate_sources()?)?;
        let twice = reuse_subplans(once.clone())?;
        assert_eq!(
            displayable(once.as_ref()).indent(true).to_string(),
            displayable(twice.as_ref()).indent(true).to_string()
        );
        Ok(())
    }

    #[test]
    fn barrier_children_have_independent_reuse_scopes() -> Result<()> {
        let plan = Arc::new(BarrierExec::new(
            vec![duplicate_sources()?],
            duplicate_sources()?,
        )) as Arc<dyn ExecutionPlan>;
        let plan = reuse_subplans(plan)?;
        let mut ids = Vec::new();
        plan.apply(|plan| {
            if let Some(shared) = plan.downcast_ref::<SharedPlanExec>() {
                ids.push(shared.id());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(ids.len(), 4);
        assert_eq!(ids[0], ids[1]);
        assert_eq!(ids[2], ids[3]);
        assert_ne!(ids[0], ids[2]);
        Ok(())
    }
}
