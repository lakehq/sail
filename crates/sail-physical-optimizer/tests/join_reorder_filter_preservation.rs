use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{JoinType, NullEquality, Result};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

fn mem_exec(schema: SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)? as Arc<dyn ExecutionPlan>)
}

fn schema_lr() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("val", DataType::Int32, true),
    ]))
}

fn schema_t() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "t_key",
        DataType::Int32,
        true,
    )]))
}

#[test]
fn preserves_non_equi_join_filter_through_reordering() -> Result<()> {
    let l = mem_exec(schema_lr())?;
    let r = mem_exec(schema_lr())?;
    let t = mem_exec(schema_t())?;

    let on_lr = vec![(
        Arc::new(Column::new("key", 0)) as _,
        Arc::new(Column::new("key", 0)) as _,
    )];
    let filter_lr = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("val", 1)),
            datafusion::logical_expr::Operator::Gt,
            Arc::new(Column::new("val", 3)),
        )),
        vec![],
        Arc::new(Schema::empty()),
    );
    let lr = Arc::new(HashJoinExec::try_new(
        Arc::clone(&l),
        Arc::clone(&r),
        on_lr,
        Some(filter_lr),
        &JoinType::Inner,
        None,
        PartitionMode::Auto,
        NullEquality::NullEqualsNull,
    )?) as Arc<dyn ExecutionPlan>;

    let on_lrt = vec![(
        Arc::new(Column::new("key", 2)) as _,
        Arc::new(Column::new("t_key", 0)) as _,
    )];
    let lrt = Arc::new(HashJoinExec::try_new(
        Arc::clone(&lr),
        Arc::clone(&t),
        on_lrt,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Auto,
        NullEquality::NullEqualsNull,
    )?) as Arc<dyn ExecutionPlan>;

    let before = DisplayableExecutionPlan::new(lrt.as_ref())
        .indent(true)
        .to_string();
    assert!(
        before.contains("filter="),
        "expected pre-optimization plan to contain a HashJoin filter, got:\n{}",
        before
    );

    let rule = sail_physical_optimizer::get_custom_sail_optimizers()
        .into_iter()
        .next()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "No JoinReorder rule found in optimizers".to_string()
            )
        })?;
    let optimized = rule.optimize(Arc::clone(&lrt), &ConfigOptions::new())?;

    let after = DisplayableExecutionPlan::new(optimized.as_ref())
        .indent(true)
        .to_string();
    assert!(
        after.contains("filter="),
        "expected optimized plan to preserve the HashJoin filter, got:\n{}",
        after
    );

    Ok(())
}
