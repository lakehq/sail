use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, Window};
use datafusion::optimizer::AnalyzerRule;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_data_source::listing::table::ListingTableSource;
use sail_delta_lake::logical::table_source::DeltaTableSource;
use sail_iceberg::logical::table_source::IcebergTableSource;

/// Validates file provenance and materializes window metadata before window shuffles.
#[derive(Debug)]
pub(super) struct ResolveInputFileMetadata;

impl AnalyzerRule for ResolveInputFileMetadata {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Spark checks invalid join conditions before multi-source provenance,
        // including when an ancestor projection also references file metadata.
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Join(_) = node {
                for expression in node.expressions() {
                    expression.apply(|expression| {
                        if let Expr::ScalarFunction(function) = expression
                            && is_input_file_metadata_function(&function.func)
                        {
                            return plan_err!(
                                "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The expression {}() is not allowed in a join condition",
                                function.func.name()
                            );
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut names = HashSet::new();
        let mut metadata_required = false;
        plan.apply_with_subqueries(|node| {
            names.extend(node.schema().fields().iter().map(|field| field.name().clone()));
            for expression in node.expressions() {
                expression.apply(|expression| {
                    if let Expr::ScalarFunction(function) = expression
                        && is_input_file_metadata_function(&function.func)
                    {
                        metadata_required = true;
                        if file_source_count(node) > 1 {
                            return plan_err!(
                                "[MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION] The expression {}() does not support more than one source",
                                function.func.name()
                            );
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let plan = if metadata_required {
            // Constant-projection Iceberg scans synthesize rows without file context.
            // Mark query-local sources, including subqueries, before physical planning.
            plan.transform_up_with_subqueries(|plan| {
                let LogicalPlan::TableScan(mut scan) = plan else {
                    return Ok(Transformed::no(plan));
                };
                if let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() {
                    let marked = source.with_input_file_metadata_required();
                    if !Arc::ptr_eq(source.scan(), marked.scan()) {
                        scan.source = Arc::new(marked);
                        return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
                    }
                }
                Ok(Transformed::no(LogicalPlan::TableScan(scan)))
            })?
            .data
        } else {
            plan
        };

        let mut next_alias = 0;
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Window(window) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let mut metadata: Vec<(Expr, String)> = Vec::new();
            let mut expressions = Vec::with_capacity(window.window_expr.len());
            for expression in &window.window_expr {
                let original_name = expression.schema_name().to_string();
                let rewritten = expression.clone().transform_up(|expression| {
                    let Expr::ScalarFunction(function) = &expression else {
                        return Ok(Transformed::no(expression));
                    };
                    if !is_input_file_metadata_function(&function.func) {
                        return Ok(Transformed::no(expression));
                    }
                    let existing = metadata
                        .iter()
                        .find(|(candidate, _)| candidate == &expression);
                    let alias = if let Some((_, alias)) = existing {
                        alias.clone()
                    } else {
                        let alias = loop {
                            let alias = format!("__sail_input_file_metadata_{next_alias}");
                            next_alias += 1;
                            if names.insert(alias.clone()) {
                                break alias;
                            }
                        };
                        metadata.push((expression, alias.clone()));
                        alias
                    };
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        alias,
                    ))))
                })?;
                expressions.push(rewritten.data.alias_if_changed(original_name)?);
            }
            if metadata.is_empty() {
                return Ok(Transformed::no(plan));
            }

            let projection = window
                .input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .chain(
                    metadata
                        .into_iter()
                        .map(|(expression, alias)| expression.alias(alias)),
                )
                .collect();
            let input = Arc::new(LogicalPlan::Projection(Projection::try_new(
                projection,
                Arc::clone(&window.input),
            )?));
            let rewritten = LogicalPlan::Window(Window::try_new(expressions, input)?);
            let recovery = Projection::try_new_with_schema(
                window
                    .schema
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect(),
                Arc::new(rewritten),
                Arc::clone(&window.schema),
            )?;
            Ok(Transformed::yes(LogicalPlan::Projection(recovery)))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "resolve_input_file_metadata"
    }
}

fn file_source_count(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::TableScan(scan) => usize::from(
            scan.source.is::<ListingTableSource>()
                || scan.source.is::<DeltaTableSource>()
                || scan.source.is::<IcebergTableSource>(),
        ),
        // Union inputs are consumed independently and can each establish a file context.
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .map(|input| file_source_count(input))
            .max()
            .unwrap_or(0),
        _ => plan.inputs().into_iter().map(file_source_count).sum(),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Constraints;
    use datafusion::common::parsers::CompressionTypeVariant;
    use datafusion::functions::expr_fn::input_file_name;
    use datafusion::logical_expr::{JoinType, LogicalPlanBuilder, ScalarUDF, col, exists};
    use datafusion::prelude::SessionContext;
    use datafusion_datasource::TableSchema;
    use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};
    use sail_data_source::formats::parquet::ParquetFormatFactory;
    use sail_data_source::listing::source::FormatFactory;
    use sail_data_source::listing::table::ListingTableSourceConfig;

    use super::*;

    fn scan(name: &str) -> Result<LogicalPlanBuilder> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let source = ListingTableSource::try_new(ListingTableSourceConfig {
            table_paths: vec![],
            schema: TableSchema::from(schema),
            constraints: Constraints::default(),
            file_sort_order: vec![],
            collect_stat: false,
            target_partitions: 1,
            read_format: Arc::new(ParquetFormatFactory::read(
                &SessionContext::new().state(),
                vec![],
            )?),
            path_glob_filter: None,
            compression: CompressionTypeVariant::UNCOMPRESSED,
        })?;
        LogicalPlanBuilder::scan(name, Arc::new(source), None)
    }

    fn metadata_expressions() -> Vec<Expr> {
        vec![
            input_file_name(),
            ScalarUDF::from(InputFileBlockStartFunc::new()).call(vec![]),
            ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]),
        ]
    }

    fn assert_invalid_join(plan: LogicalPlan) {
        let error = ResolveInputFileMetadata
            .analyze(plan, &ConfigOptions::default())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("[INVALID_NON_DETERMINISTIC_EXPRESSIONS]"),
            "{error}"
        );
        assert!(
            !error.contains("[MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION]"),
            "{error}"
        );
    }

    #[test]
    fn marks_only_query_local_iceberg_sources_including_subqueries() -> Result<()> {
        use datafusion::logical_expr::lit;
        use sail_iceberg::datasource::scan::IcebergScan;
        use sail_iceberg::spec::types::{NestedField, PrimitiveType, Type};
        use sail_iceberg::spec::{PartitionSpec, Schema as IcebergSchema};

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields([Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .map_err(|error| datafusion::common::plan_datafusion_err!("{error}"))?;
        let source = Arc::new(IcebergTableSource::new(Arc::new(IcebergScan::new_empty(
            "file:///tmp/input-file-test",
            schema,
            vec![PartitionSpec::unpartitioned_spec()],
            0,
        )?)));
        let mut input = LogicalPlanBuilder::scan("i", source.clone(), Some(vec![0]))?.build()?;
        let LogicalPlan::TableScan(scan) = &mut input else {
            unreachable!()
        };
        scan.fetch = Some(7);
        scan.filters = vec![col("id").gt(lit(0_i64))];
        assert_eq!(
            ResolveInputFileMetadata.analyze(input.clone(), &ConfigOptions::default())?,
            input
        );
        let query = LogicalPlanBuilder::from(input.clone())
            .project(vec![input_file_name()])?
            .build()?;
        for query in [
            query.clone(),
            LogicalPlanBuilder::empty(false)
                .project(vec![exists(Arc::new(query))])?
                .build()?,
        ] {
            let marked = ResolveInputFileMetadata.analyze(query, &ConfigOptions::default())?;
            let repeated = ResolveInputFileMetadata.analyze(marked, &ConfigOptions::default())?;
            let mut scans = 0;
            repeated.apply_with_subqueries(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    scans += 1;
                    let marked = scan.source.downcast_ref::<IcebergTableSource>().unwrap();
                    assert!(!Arc::ptr_eq(source.scan(), marked.scan()));
                    assert!(Arc::ptr_eq(
                        marked.scan(),
                        marked.with_input_file_metadata_required().scan()
                    ));
                    let LogicalPlan::TableScan(original) = &input else {
                        unreachable!()
                    };
                    assert_eq!(scan.table_name, original.table_name);
                    assert_eq!(scan.projected_schema, original.projected_schema);
                    assert_eq!(scan.projection, original.projection);
                    assert_eq!(scan.fetch, original.fetch);
                    assert_eq!(scan.filters, original.filters);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(scans, 1);
        }
        assert!(!Arc::ptr_eq(
            source.scan(),
            source.with_input_file_metadata_required().scan()
        ));
        Ok(())
    }

    #[test]
    fn rejects_metadata_in_join_keys_and_residuals() -> Result<()> {
        for expression in metadata_expressions() {
            let right = scan("r")?.build()?;
            let non_equi = scan("l")?
                .join_on(
                    right.clone(),
                    JoinType::Inner,
                    [
                        col("l.id").lt(col("r.id")),
                        expression.clone().is_not_null(),
                    ],
                )?
                .build()?;
            assert_invalid_join(non_equi);
            let equi = scan("l")?
                .join(
                    right,
                    JoinType::Inner,
                    (vec!["l.id"], vec!["r.id"]),
                    Some(expression.clone().is_not_null()),
                )?
                .build()?;
            assert_invalid_join(equi.clone());
            let LogicalPlan::Join(mut join) = equi else {
                unreachable!()
            };
            join.filter = None;
            join.on = vec![(expression.clone(), expression)];
            assert_invalid_join(LogicalPlan::Join(join));
        }
        Ok(())
    }

    #[test]
    fn validates_join_before_ancestor_provenance_and_inside_subqueries() -> Result<()> {
        let invalid = scan("l")?
            .join_on(
                scan("r")?.build()?,
                JoinType::Inner,
                [input_file_name().is_not_null()],
            )?
            .build()?;
        let parent = LogicalPlanBuilder::from(invalid.clone())
            .project(vec![input_file_name()])?
            .build()?;
        assert_invalid_join(parent);
        let subquery = LogicalPlanBuilder::empty(false)
            .project(vec![exists(Arc::new(invalid))])?
            .build()?;
        assert_invalid_join(subquery);
        Ok(())
    }

    #[test]
    fn allows_materialized_join_metadata_but_rejects_two_source_projection() -> Result<()> {
        for expression in metadata_expressions() {
            let left = scan("source")?
                .project(vec![col("id"), expression.clone().alias("metadata")])?
                .alias("l")?;
            let joined = left
                .join(
                    scan("r")?.build()?,
                    JoinType::Inner,
                    (vec!["l.id"], vec!["r.id"]),
                    Some(col("l.metadata").is_not_null()),
                )?
                .build()?;
            assert_eq!(
                ResolveInputFileMetadata.analyze(joined.clone(), &ConfigOptions::default())?,
                joined
            );
            let parent = LogicalPlanBuilder::from(joined)
                .project(vec![expression])?
                .build()?;
            let error = ResolveInputFileMetadata
                .analyze(parent, &ConfigOptions::default())
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("[MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION]"),
                "{error}"
            );
        }
        Ok(())
    }
}
