use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions, make_array,
};
use datafusion::arrow::compute::{concat, concat_batches};
use datafusion::arrow::datatypes::{DataType, FieldRef, Float32Type, Float64Type, Schema};
use datafusion::catalog::MemTable;
use datafusion::datasource::{DefaultTableSource, provider_as_source};
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchema, Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, EmptyRelation, Expr, JoinType, LogicalPlan, Operator, Projection,
    TableScanBuilder, Volatility,
};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use super::locally_evaluable;

/// Preserve Spark's early ConvertToLocalRelation evaluation for IN inputs.
/// Replacing evaluated projections also prevents volatile expressions from running twice.
pub(super) fn materialize(plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<LogicalPlan> {
    let mut props = ExecutionProps::new();
    props.query_execution_start_time = config.query_execution_start_time();
    props.config_options = Some(config.options());
    let batch_size = config.options().execution.batch_size.get();
    plan.transform_up(|plan| {
        propagate_empty(plan, &props, config)?.transform_data(|plan| {
            let batches = match &plan {
                LogicalPlan::Projection(projection) => {
                    for expr in &projection.expr {
                        if !locally_evaluable(expr)? {
                            return Ok(Transformed::no(plan));
                        }
                    }
                    let Some(input) = evaluation_batches(
                        &projection.input,
                        &props,
                        projection.expr.iter().any(Expr::is_volatile),
                    )?
                    else {
                        return Ok(Transformed::no(plan));
                    };
                    let expressions = projection
                        .expr
                        .iter()
                        .map(|expr| physical(expr, projection.input.schema(), &props))
                        .collect::<Result<Vec<_>>>()?;
                    let schema = Arc::clone(&projection.schema);
                    map_batches(
                        input,
                        batch_size,
                        if projection.expr.iter().any(Expr::is_volatile) {
                            1
                        } else {
                            config.options().execution.target_partitions
                        },
                        move |batch| project(batch, &expressions, &schema),
                    )?
                }
                LogicalPlan::Filter(filter) if locally_evaluable(&filter.predicate)? => {
                    let Some(input) =
                        evaluation_batches(&filter.input, &props, filter.predicate.is_volatile())?
                    else {
                        return Ok(Transformed::no(plan));
                    };
                    let predicate = physical(&filter.predicate, filter.input.schema(), &props)?;
                    map_batches(
                        input,
                        batch_size,
                        if filter.predicate.is_volatile() {
                            1
                        } else {
                            config.options().execution.target_partitions
                        },
                        move |batch| {
                            if batch.num_rows() == 0 {
                                return Ok(batch.clone());
                            }
                            let value = predicate.evaluate(batch)?.into_array(batch.num_rows())?;
                            Ok(datafusion::arrow::compute::filter_record_batch(
                                batch,
                                datafusion_common::cast::as_boolean_array(value.as_ref())?,
                            )?)
                        },
                    )?
                }
                LogicalPlan::Limit(limit) if limit.skip.is_none() => {
                    // Computed limits are a Spark local-materialization boundary.
                    let fetch = match limit.fetch.as_deref() {
                        Some(Expr::Literal(value, _)) => value,
                        Some(Expr::Cast(cast))
                            if cast.field.data_type()
                                == &datafusion::arrow::datatypes::DataType::Int64 =>
                        {
                            let Expr::Literal(value @ ScalarValue::Int32(_), _) =
                                cast.expr.as_ref()
                            else {
                                return Ok(Transformed::no(plan));
                            };
                            value
                        }
                        _ => return Ok(Transformed::no(plan)),
                    };
                    let remaining = match fetch {
                        ScalarValue::Int32(Some(value)) if *value >= 0 => *value as usize,
                        ScalarValue::Int64(Some(value)) if *value >= 0 => *value as usize,
                        _ => return Ok(Transformed::no(plan)),
                    };
                    let Some(input) = local_batches(&limit.input, &props)? else {
                        return Ok(Transformed::no(plan));
                    };
                    let mut remaining = remaining;
                    input
                        .into_iter()
                        .map(|batch| {
                            let rows = remaining.min(batch.num_rows());
                            remaining -= rows;
                            batch.slice(0, rows)
                        })
                        .collect()
                }
                _ => return Ok(Transformed::no(plan)),
            };
            let schema = Arc::clone(plan.schema());
            if batches.iter().all(|batch| batch.num_rows() == 0) {
                return Ok(Transformed::yes(empty(&plan)));
            }
            // MemorySource repartitions existing batches, but cannot divide one large
            // batch. Slice evaluated outputs so eager evaluation preserves downstream
            // parallelism without changing expression evaluation or copying buffers.
            let batches = split_batches(batches, batch_size);
            let source = provider_as_source(Arc::new(MemTable::try_new(
                Arc::clone(schema.inner()),
                vec![batches],
            )?));
            let scan =
                TableScanBuilder::new(config.alias_generator().next("__sail_local_in"), source)
                    .build()?;
            let expressions = scan
                .projected_schema
                .columns()
                .into_iter()
                .zip(schema.columns())
                .map(|(source, target)| {
                    Expr::Column(source).alias_qualified(target.relation, target.name)
                })
                .collect();
            // A scan's qualifier is rebuilt during column pruning. Restore the original
            // names through ordinary aliases rather than overriding its derived schema.
            Ok(Transformed::yes(LogicalPlan::Projection(
                Projection::try_new_with_schema(
                    expressions,
                    Arc::new(LogicalPlan::TableScan(scan)),
                    schema,
                )?,
            )))
        })
    })
    .data()
}

fn empty(plan: &LogicalPlan) -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::clone(plan.schema()),
    })
}

/// Spark runs empty propagation alongside its early local evaluation. Reuse
/// DataFusion's union pruning and join null-padding after exposing empty local
/// scans, with Spark's additional literal-false and conditionless join triggers.
fn propagate_empty(
    plan: LogicalPlan,
    props: &ExecutionProps,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    if matches!(&plan, LogicalPlan::Values(values) if values.values.is_empty()) {
        return Ok(Transformed::yes(empty(&plan)));
    }
    if matches!(plan, LogicalPlan::TableScan(_))
        && local_batches(&plan, props)?
            .is_some_and(|batches| batches.iter().all(|batch| batch.num_rows() == 0))
    {
        return Ok(Transformed::yes(empty(&plan)));
    }
    let mut prepared = Transformed::no(plan);
    if let LogicalPlan::Join(join) = &mut prepared.data
        && join.on.is_empty()
    {
        if matches!(
            join.filter,
            Some(Expr::Literal(ScalarValue::Boolean(Some(false)), _))
        ) {
            // A computed false predicate must remain a boundary until ordinary
            // optimization, and Spark retains a FULL JOIN with a false condition.
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
                    join.right = Arc::new(empty(&join.right));
                    prepared.transformed = true;
                }
                JoinType::Right => {
                    join.left = Arc::new(empty(&join.left));
                    prepared.transformed = true;
                }
                _ => {}
            }
        } else if join.filter.is_none()
            && matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            && local_batches(&join.right, props)?
                .is_some_and(|batches| batches.iter().any(|batch| batch.num_rows() > 0))
        {
            return Ok(Transformed::yes(if join.join_type == JoinType::LeftSemi {
                Arc::unwrap_or_clone(Arc::clone(&join.left))
            } else {
                empty(&prepared.data)
            }));
        }
    }
    prepared.transform_data(|plan| PropagateEmptyRelation.rewrite(plan, config))
}

fn physical(
    expr: &Expr,
    schema: &DFSchema,
    props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(schema.clone()))
        .with_config_options(props.config_options.clone().unwrap_or_default())
        .with_query_execution_start_time(props.query_execution_start_time)
        .build();
    // These UDFs require structural lowering before physical execution. Do not
    // run constant folding: an empty local input must not evaluate any values.
    let expr = expr
        .clone()
        .transform_up(|expr| {
            if let Expr::ScalarFunction(function) = &expr
                && (function.func.short_circuits()
                    || function.func.signature().volatility == Volatility::Stable)
                && let ExprSimplifyResult::Simplified(lowered) =
                    function.func.simplify(function.args.clone(), &context)?
            {
                return Ok(Transformed::yes(lowered));
            }
            Ok(Transformed::no(expr))
        })?
        .data;
    create_physical_expr(&expr, schema, props, &PhysicalPlanningContext::default())?
        .transform_up(|expr| {
            if expr
                .downcast_ref::<BinaryExpr>()
                .is_some_and(|binary| matches!(binary.op(), Operator::And | Operator::Or))
            {
                Ok(Transformed::yes(
                    Arc::new(LocalBooleanExpr { input: expr }) as Arc<dyn PhysicalExpr>
                ))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()
}

/// Used only while materializing local rows; this expression never enters a plan.
/// Always mask Boolean RHS rows, unlike DataFusion's selectivity-based shortcut.
#[derive(Debug, Clone, Eq)]
struct LocalBooleanExpr {
    input: Arc<dyn PhysicalExpr>,
}

impl PartialEq for LocalBooleanExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input)
    }
}

impl std::hash::Hash for LocalBooleanExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl Display for LocalBooleanExpr {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.input, formatter)
    }
}

impl PhysicalExpr for LocalBooleanExpr {
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.input.data_type(schema)
    }

    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.input.nullable(schema)
    }

    fn return_field(&self, schema: &Schema) -> Result<FieldRef> {
        self.input.return_field(schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let Some(binary) = self.input.downcast_ref::<BinaryExpr>() else {
            return internal_err!("expected a local Boolean expression");
        };
        let left = binary
            .left()
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        let left = datafusion_common::cast::as_boolean_array(left.as_ref())?;
        let is_and = binary.op() == &Operator::And;
        let mut selection = if is_and {
            left.values().clone()
        } else {
            !left.values()
        };
        if let Some(nulls) = left.nulls().filter(|nulls| nulls.null_count() > 0) {
            selection |= &!nulls.inner();
        }
        let selection = BooleanArray::new(selection, None);
        let right = binary
            .right()
            .evaluate_selection(batch, &selection)?
            .into_array(batch.num_rows())?;
        let right = datafusion_common::cast::as_boolean_array(right.as_ref())?;
        let result = if is_and {
            datafusion::arrow::compute::and_kleene(left, right)?
        } else {
            datafusion::arrow::compute::or_kleene(left, right)?
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [input] = children.as_slice() else {
            return internal_err!("local Boolean expression requires one input");
        };
        Ok(Arc::new(Self {
            input: Arc::clone(input),
        }))
    }

    fn fmt_sql(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, formatter)
    }
}

fn evaluation_batches(
    plan: &LogicalPlan,
    props: &ExecutionProps,
    volatile: bool,
) -> Result<Option<Vec<RecordBatch>>> {
    let Some(batches) = local_batches(plan, props)? else {
        return Ok(None);
    };
    // Spark initializes local nondeterministic expressions once for all rows.
    // One evaluation keeps seeded random sequences independent of Arrow chunking.
    if volatile && batches.len() > 1 {
        return Ok(Some(vec![concat_batches(plan.schema().inner(), &batches)?]));
    }
    Ok(Some(batches))
}

/// Compare evaluated SQL-created local relations, reusing equal Arrow data.
/// Column names and batch boundaries are incidental to Catalyst LocalRelation.
pub(super) fn same_result(left: &LogicalPlan, right: &LogicalPlan) -> Result<bool> {
    if left.schema().fields().len() != right.schema().fields().len()
        || left
            .schema()
            .fields()
            .iter()
            .zip(right.schema().fields())
            .any(|(left, right)| {
                left.data_type() != right.data_type() || left.is_nullable() != right.is_nullable()
            })
    {
        return Ok(false);
    }
    let props = ExecutionProps::new();
    let (Some(left), Some(right)) = (local_batches(left, &props)?, local_batches(right, &props)?)
    else {
        return Ok(false);
    };
    let mut left = left.iter().filter(|batch| batch.num_rows() > 0).peekable();
    let mut right = right.iter().filter(|batch| batch.num_rows() > 0).peekable();
    let (mut left_offset, mut right_offset) = (0, 0);
    while let (Some(a), Some(b)) = (left.peek(), right.peek()) {
        let rows = (a.num_rows() - left_offset).min(b.num_rows() - right_offset);
        let left_slice = a.slice(left_offset, rows);
        let right_slice = b.slice(right_offset, rows);
        for (a, b) in left_slice.columns().iter().zip(right_slice.columns()) {
            if a != b {
                let left = normalize_local_floats(a)?;
                let right = normalize_local_floats(b)?;
                if (Arc::ptr_eq(a, &left) && Arc::ptr_eq(b, &right)) || left != right {
                    return Ok(false);
                }
            }
        }
        left_offset += rows;
        right_offset += rows;
        if left_offset == a.num_rows() {
            left.next();
            left_offset = 0;
        }
        if right_offset == b.num_rows() {
            right.next();
            right_offset = 0;
        }
    }
    Ok(left.next().is_none() && right.next().is_none())
}

// Catalyst's generic rows/arrays equate signed zeros and all NaN payloads.
// Arrow equality compares float bits. Normalize only the comparison fallback,
// including nested values, without changing the relation that will execute.
fn normalize_local_floats(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Float32 => Ok(Arc::new(
            array
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(|value| {
                    if value.to_bits() << 1 == 0 {
                        0.0
                    } else if value.is_nan() {
                        f32::NAN
                    } else {
                        value
                    }
                }),
        )),
        DataType::Float64 => Ok(Arc::new(
            array
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|value| {
                    if value.to_bits() << 1 == 0 {
                        0.0
                    } else if value.is_nan() {
                        f64::NAN
                    } else {
                        value
                    }
                }),
        )),
        _ => {
            let data = array.to_data();
            if data.child_data().is_empty() {
                return Ok(Arc::clone(array));
            }
            let children = data
                .child_data()
                .iter()
                .map(|child| {
                    normalize_local_floats(&make_array(child.clone())).map(|a| a.to_data())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(make_array(
                data.into_builder().child_data(children).build()?,
            ))
        }
    }
}

/// Reuse the existing runtime workers for large pure local evaluations.
/// Volatile callers use one worker. Results are received in input order, and
/// every job finishes before this call returns, including when a batch fails.
fn map_batches(
    input: Vec<RecordBatch>,
    batch_size: usize,
    parallelism: usize,
    evaluate: impl Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync + 'static,
) -> Result<Vec<RecordBatch>> {
    let parallelism = parallelism.min(
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
    );
    let rows = input.iter().map(RecordBatch::num_rows).sum::<usize>();
    if parallelism <= 1 || rows <= batch_size.saturating_mul(parallelism) {
        return input.iter().map(evaluate).collect();
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return input.iter().map(evaluate).collect();
    };
    let input = split_batches(input, rows.div_ceil(parallelism));
    let evaluate = Arc::new(evaluate);
    let jobs = input
        .chunks(input.len().div_ceil(parallelism))
        .map(|batches| {
            let batches = batches.to_vec();
            let evaluate = Arc::clone(&evaluate);
            let (sender, receiver) = std::sync::mpsc::channel();
            runtime.spawn_blocking(move || {
                let result = batches
                    .iter()
                    .map(|batch| evaluate(batch))
                    .collect::<Result<Vec<_>>>();
                let _ = sender.send(result);
            });
            receiver
        })
        .collect::<Vec<_>>();
    // Receive every result before propagating the first error, so failed plans
    // cannot leave detached evaluation work running in the session's pool.
    let results = jobs
        .into_iter()
        .map(|receiver| {
            receiver
                .recv()
                .map_err(|_| exec_datafusion_err!("local IN evaluation worker stopped"))?
        })
        .collect::<Vec<_>>();
    let mut output = Vec::with_capacity(input.len());
    for result in results {
        output.extend(result?);
    }
    Ok(output)
}

fn split_batches(batches: Vec<RecordBatch>, batch_size: usize) -> Vec<RecordBatch> {
    let mut output = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() <= batch_size {
            output.push(batch);
        } else {
            for offset in (0..batch.num_rows()).step_by(batch_size) {
                output.push(batch.slice(offset, (batch.num_rows() - offset).min(batch_size)));
            }
        }
    }
    output
}

fn local_batches(plan: &LogicalPlan, props: &ExecutionProps) -> Result<Option<Vec<RecordBatch>>> {
    let batches = match plan {
        // A no-FROM SELECT is Spark's OneRowRelation, not a local relation.
        LogicalPlan::EmptyRelation(empty) if !empty.produce_one_row => {
            Some(vec![RecordBatch::new_empty(Arc::clone(
                plan.schema().inner(),
            ))])
        }
        LogicalPlan::SubqueryAlias(alias) => local_batches(&alias.input, props)?,
        LogicalPlan::Projection(projection)
            if projection
                .expr
                .iter()
                .all(|expr| matches!(expr.clone().unalias(), Expr::Column(_))) =>
        {
            let indices = projection
                .expr
                .iter()
                .map(|expr| {
                    let Expr::Column(column) = expr.clone().unalias() else {
                        unreachable!("column-only projection checked above")
                    };
                    projection.input.schema().index_of_column(&column)
                })
                .collect::<Result<Vec<_>>>()?;
            local_batches(&projection.input, props)?
                .map(|batches| {
                    batches
                        .iter()
                        .map(|batch| batch.project(&indices).map_err(Into::into))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
        }
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
            let Some(source) = scan.source.downcast_ref::<DefaultTableSource>() else {
                return Ok(None);
            };
            let mut provider = &source.table_provider;
            while let Some(renamed) = provider.downcast_ref::<RenameTableProvider>() {
                provider = renamed.inner();
            }
            let Some(table) = provider.downcast_ref::<MemTable>() else {
                return Ok(None);
            };
            let mut batches = Vec::new();
            for partition in &table.batches {
                let partition = partition
                    .try_read()
                    .map_err(|error| exec_datafusion_err!("Cannot read local IN input: {error}"))?;
                for batch in partition.iter() {
                    batches.push(match &scan.projection {
                        Some(projection) => batch.project(projection)?,
                        None => batch.clone(),
                    });
                }
            }
            Some(batches)
        }
        LogicalPlan::Extension(extension) => {
            let (input, monotonic) =
                if let Some(node) = extension.node.as_any().downcast_ref::<MonotonicIdNode>() {
                    (node.input(), true)
                } else if let Some(node) = extension
                    .node
                    .as_any()
                    .downcast_ref::<SparkPartitionIdNode>()
                {
                    (node.input(), false)
                } else {
                    return Ok(None);
                };
            let Some(input) = local_batches(input, props)? else {
                return Ok(None);
            };
            // These nodes represent locally evaluable Spark expressions.
            // ConvertToLocalRelation initializes both at partition zero and
            // keeps the monotonic counter across all local input batches.
            let mut offset = 0_i64;
            Some(
                input
                    .into_iter()
                    .map(|batch| {
                        let end = offset + batch.num_rows() as i64;
                        let value = if monotonic {
                            Arc::new(Int64Array::from_iter_values(offset..end)) as ArrayRef
                        } else {
                            ScalarValue::Int32(Some(0)).to_array_of_size(batch.num_rows())?
                        };
                        offset = end;
                        let mut columns = batch.columns().to_vec();
                        columns.push(value);
                        RecordBatch::try_new(Arc::clone(plan.schema().inner()), columns)
                            .map_err(Into::into)
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        LogicalPlan::Values(values) => {
            for expr in values.values.iter().flatten() {
                if !locally_evaluable(expr)? {
                    return Ok(None);
                }
            }
            let input = RecordBatch::try_new_with_options(
                Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                vec![],
                &RecordBatchOptions::default().with_row_count(Some(1)),
            )?;
            let schema = DFSchema::empty();
            let mut columns = vec![
                Vec::<ArrayRef>::with_capacity(values.values.len());
                values.schema.fields().len()
            ];
            for row in &values.values {
                for (expr, column) in row.iter().zip(&mut columns) {
                    column.push(
                        physical(expr, &schema, props)?
                            .evaluate(&input)?
                            .into_array(1)?,
                    );
                }
            }
            Some(vec![collect_rows(
                columns,
                values.schema.as_ref(),
                values.values.len(),
            )?])
        }
        _ => None,
    };
    batches
        .map(|batches| {
            batches
                .into_iter()
                .map(|batch| {
                    RecordBatch::try_new_with_options(
                        Arc::clone(plan.schema().inner()),
                        batch.columns().to_vec(),
                        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
                    )
                    .map_err(Into::into)
                })
                .collect()
        })
        .transpose()
}

fn project(
    batch: &RecordBatch,
    expressions: &[Arc<dyn PhysicalExpr>],
    schema: &DFSchema,
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(Arc::clone(schema.inner())));
    }
    let columns = expressions
        .iter()
        .map(|expression| expression.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema.inner()),
        columns,
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
}

fn collect_rows(
    columns: Vec<Vec<ArrayRef>>,
    schema: &DFSchema,
    rows: usize,
) -> Result<RecordBatch> {
    if rows == 0 {
        return Ok(RecordBatch::new_empty(Arc::clone(schema.inner())));
    }
    let columns = columns
        .into_iter()
        .map(|column| {
            let arrays = column
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>();
            concat(&arrays).map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema.inner()),
        columns,
        &RecordBatchOptions::default().with_row_count(Some(rows)),
    )?)
}
