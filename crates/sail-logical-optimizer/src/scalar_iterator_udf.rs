use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, DFSchema, Result, plan_datafusion_err, plan_err};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder};
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_python_udf::udf::pyspark_scalar_iter_udf::PySparkScalarPandasIterUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};

#[derive(Debug)]
pub struct ExtractScalarIteratorUDF;

impl AnalyzerRule for ExtractScalarIteratorUDF {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let aliases = AliasGenerator::new();
        plan.transform_up_with_subqueries(|plan| extract_scalar_iterators(plan, &aliases))
            .data()
    }

    fn name(&self) -> &str {
        "extract_scalar_iterator_udf"
    }
}

fn next_column_name(
    aliases: &AliasGenerator,
    inputs: &[LogicalPlan],
    output_schema: &DFSchema,
) -> String {
    loop {
        let name = aliases.next("__scalar_iterator");
        if !output_schema.has_column_with_unqualified_name(&name)
            && inputs
                .iter()
                .all(|input| !input.schema().has_column_with_unqualified_name(&name))
        {
            return name;
        }
    }
}

fn extract_scalar_iterators(
    plan: LogicalPlan,
    aliases: &AliasGenerator,
) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Join(join) = &plan
        && let Some(filter) = &join.filter
    {
        let mut join_predicates = vec![];
        let mut output_predicates = vec![];
        for predicate in split_conjunction(filter) {
            let needs_join_output = predicate.exists(|expression| {
                let Expr::ScalarFunction(function) = expression else {
                    return Ok(false);
                };
                let Some(udf) = function.func.inner().downcast_ref::<PySparkUDF>() else {
                    return Ok(false);
                };
                let columns = expression.column_refs();
                Ok(udf.kind() == PySparkUdfKind::ScalarPandasIter
                    && !columns
                        .iter()
                        .all(|column| join.left.schema().has_column(column))
                    && !columns
                        .iter()
                        .all(|column| join.right.schema().has_column(column)))
            })?;
            if needs_join_output {
                output_predicates.push(predicate.clone());
            } else {
                join_predicates.push(predicate.clone());
            }
        }
        if let Some(predicate) = conjunction(output_predicates) {
            if join.join_type != JoinType::Inner {
                return plan_err!(
                    "scalar iterator UDF referencing both join inputs requires an inner join"
                );
            }
            let mut join = join.clone();
            join.filter = conjunction(join_predicates);
            let input = extract_scalar_iterators(LogicalPlan::Join(join), aliases)?.data;
            let filter = LogicalPlanBuilder::from(input).filter(predicate)?.build()?;
            return extract_scalar_iterators(filter, aliases);
        }
    }
    let output_schema = Arc::clone(plan.schema());
    let mut inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
    let names = NamePreserver::new(&plan);
    let mut replacements = HashMap::<Expr, Expr>::new();
    let mut changed = false;
    let expressions = plan
        .expressions()
        .into_iter()
        .map(|expression| {
            let name = names.save(&expression);
            let rewritten = expression
                .transform_up(|expression| {
                    let Expr::ScalarFunction(function) = &expression else {
                        return Ok(Transformed::no(expression));
                    };
                    let Some(udf) = function.func.inner().downcast_ref::<PySparkUDF>() else {
                        return Ok(Transformed::no(expression));
                    };
                    if udf.kind() != PySparkUdfKind::ScalarPandasIter {
                        return Ok(Transformed::no(expression));
                    }
                    if let Some(column) = replacements.get(&expression) {
                        return Ok(Transformed::yes(column.clone()));
                    }
                    let columns = expression.column_refs();
                    let input_index = inputs
                        .iter()
                        .position(|input| columns.iter().all(|column| input.schema().has_column(column)))
                        .ok_or_else(|| {
                            plan_datafusion_err!(
                                "scalar iterator UDF arguments must be evaluable on one relational input"
                            )
                        })?;
                    let input = inputs[input_index].clone();
                    let result_name = next_column_name(aliases, &inputs, &output_schema);
                    let mut projection = input
                        .schema()
                        .columns()
                        .into_iter()
                        .map(Expr::Column)
                        .collect::<Vec<_>>();
                    for argument in &function.args {
                        projection.push(argument.clone().alias(next_column_name(
                            aliases,
                            &inputs,
                            &output_schema,
                        )));
                    }
                    let mut fields = input.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new(
                        &result_name,
                        udf.output_type().clone(),
                        true,
                    )));
                    let output_names = fields.iter().map(|field| field.name().clone()).collect();
                    let mut qualifiers = input
                        .schema()
                        .iter()
                        .map(|(qualifier, _)| qualifier.cloned())
                        .collect::<Vec<_>>();
                    qualifiers.push(None);
                    let stream_udf = PySparkScalarPandasIterUDF::try_new(
                        function.func.name().to_string(),
                        udf.payload().to_vec(),
                        Arc::new(Schema::new_with_metadata(
                            fields,
                            input.schema().metadata().clone(),
                        )),
                        Arc::clone(udf.config()),
                    )?;
                    let input = LogicalPlanBuilder::from(input).project(projection)?.build()?;
                    inputs[input_index] = LogicalPlan::Extension(Extension {
                        node: Arc::new(MapPartitionsNode::try_new(
                            Arc::new(input),
                            output_names,
                            qualifiers,
                            Arc::new(stream_udf),
                        )?),
                    });
                    let column = Expr::Column(Column::from_name(result_name));
                    if udf.deterministic() {
                        replacements.insert(expression, column.clone());
                    }
                    changed = true;
                    Ok(Transformed::yes(column))
                })?
                .data;
            Ok(name.restore(rewritten))
        })
        .collect::<Result<Vec<_>>>()?;
    if !changed {
        return Ok(Transformed::no(plan));
    }
    let plan = plan.with_new_exprs(expressions, inputs)?;
    // Filters, sorts, joins, and windows also pass through their input columns.
    let plan = if plan.schema() != &output_schema {
        LogicalPlanBuilder::from(plan)
            .project(output_schema.columns().into_iter().map(Expr::Column))?
            .build()?
    } else {
        plan
    };
    Ok(Transformed::yes(plan))
}
