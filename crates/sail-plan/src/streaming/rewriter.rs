use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{Result, internal_err, not_impl_err, plan_err};
use datafusion_expr::{
    Explain, FetchType, Filter, Projection, SkipType, SubqueryAlias, TableScan, Union,
    UserDefinedLogicalNode, col, or,
};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::streaming::event::schema::{
    MARKER_FIELD_NAME, RETRACTED_FIELD_NAME, is_flow_event_schema,
};
use sail_common_datafusion::streaming::source::{StreamSource, StreamSourceTableProvider};
use sail_data_source::formats::console::ConsoleWriteNode;
use sail_data_source::formats::noop::NoopWriteNode;
use sail_data_source::listing::write::FileWriteNode;
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::show_string::ShowStringNode;
use sail_logical_plan::streaming::collector::StreamCollectorNode;
use sail_logical_plan::streaming::filter::StreamFilterNode;
use sail_logical_plan::streaming::limit::StreamLimitNode;
use sail_logical_plan::streaming::source_adapter::StreamSourceAdapterNode;
use sail_logical_plan::streaming::source_wrapper::StreamSourceWrapperNode;

/// A logical plan rewriter that rewrites a batch logical plan
/// into a streaming logical plan. All the nodes (except the sink) in the plan
/// will have a flow event schema which contains additional fields
/// along with the original data fields.
struct StreamingRewriter;

impl StreamingRewriter {
    fn f_up_extension(&mut self, extension: Extension) -> Result<Transformed<LogicalPlan>> {
        let node = extension.node.as_ref();
        if node.as_any().is::<RangeNode>() {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(
                    LogicalPlan::Extension(extension),
                ))?),
            })))
        } else if let Some(show) = node.as_any().downcast_ref::<ShowStringNode>() {
            let input = LogicalPlan::Extension(Extension {
                node: Arc::new(StreamCollectorNode::try_new(Arc::clone(show.input()))?),
            });
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: show.with_exprs_and_inputs(vec![], vec![input])?,
            })))
        } else if node.as_any().is::<ConsoleWriteNode>()
            || node.as_any().is::<FileWriteNode>()
            || node.as_any().is::<NoopWriteNode>()
        {
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else if node.as_any().is::<BarrierNode>() {
            // TODO: support BarrierNode for streaming properly.
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else {
            plan_err!("unsupported extension node for streaming: {node:?}")
        }
    }
}

impl TreeNodeRewriter for StreamingRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => self.f_up_extension(extension),
            LogicalPlan::Projection(projection) => {
                let Projection {
                    mut expr, input, ..
                } = projection;
                expr.insert(0, col(MARKER_FIELD_NAME));
                expr.insert(1, col(RETRACTED_FIELD_NAME));
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(expr, input)?,
                )))
            }
            LogicalPlan::Filter(filter) => {
                let Filter {
                    predicate, input, ..
                } = filter;
                let predicate = or(predicate, col(MARKER_FIELD_NAME).is_not_null());
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamFilterNode::new(input, predicate)),
                })))
            }
            LogicalPlan::Window(_) => {
                not_impl_err!("streaming window: {plan:?}")
            }
            LogicalPlan::Aggregate(_) => {
                not_impl_err!("streaming aggregate: {plan:?}")
            }
            LogicalPlan::Sort(_) => {
                plan_err!("sort is not supported for streaming: {plan:?}")
            }
            LogicalPlan::Join(_) | LogicalPlan::AsOfJoin(_) => {
                not_impl_err!("streaming join: {plan:?}")
            }
            LogicalPlan::Repartition(_) => {
                not_impl_err!("streaming repartition: {plan:?}")
            }
            LogicalPlan::TableScan(ref scan) => match source_as_provider(&scan.source) {
                Ok(provider) => match get_stream_source_opt(provider.as_ref()) {
                    Some(source) => {
                        let NamedStreamSource { source, names } = source;
                        let TableScan {
                            table_name,
                            source: _,
                            projection,
                            projected_schema: _,
                            filters,
                            fetch,
                            skip,
                            statistics_requests: _,
                        } = scan;
                        // Stream sources do not support skip pushdown. Reject unexpected
                        // pushed offsets rather than silently dropping them in the wrapper.
                        if skip.is_some_and(|skip| skip > 0) {
                            return not_impl_err!(
                                "streaming table scan with pushed-down skip: {skip:?}"
                            );
                        }
                        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                            node: Arc::new(StreamSourceWrapperNode::try_new(
                                table_name.clone(),
                                source,
                                names,
                                projection.clone(),
                                filters.clone(),
                                *fetch,
                            )?),
                        })))
                    }
                    _ => Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                    }))),
                },
                _ => Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                }))),
            },
            LogicalPlan::Union(union) => Ok(Transformed::yes(LogicalPlan::Union(
                Union::try_new_with_loose_types(union.inputs)?,
            ))),
            LogicalPlan::SubqueryAlias(alias) => Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(alias.input, alias.alias)?,
            ))),
            LogicalPlan::Limit(ref limit) => {
                let SkipType::Literal(skip) = limit.get_skip_type()? else {
                    return plan_err!("streaming limit requires literal skip: {plan:?}");
                };
                let FetchType::Literal(fetch) = limit.get_fetch_type()? else {
                    return plan_err!("streaming limit requires literal fetch: {plan:?}");
                };
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamLimitNode::new(Arc::clone(&limit.input), skip, fetch)),
                })))
            }
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                })))
            }
            LogicalPlan::Unnest(_) => {
                // We need to preserve all markers in the unnested record batches.
                // This can be done by having a placeholder one-element nested value
                // for each marker row.
                not_impl_err!("streaming unnest: {plan:?}")
            }
            LogicalPlan::RecursiveQuery(_) => {
                not_impl_err!("recursive streaming query: {plan:?}")
            }
            LogicalPlan::MaterializedCte(_) => {
                not_impl_err!("streaming materialized CTE: {plan:?}")
            }
            LogicalPlan::Subquery(_) | LogicalPlan::Distinct(_) => {
                internal_err!("not rewritten before streaming rewriter: {plan:?}")
            }
            LogicalPlan::Explain(explain) => {
                let input = LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamCollectorNode::try_new(Arc::clone(&explain.plan))?),
                });
                Ok(Transformed::yes(LogicalPlan::Explain(Explain {
                    plan: Arc::new(input),
                    ..explain
                })))
            }
            LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Show(_) => {
                internal_err!("unexpected command for streaming rewriter: {plan:?}")
            }
        }
    }
}

fn is_streaming_table_provider(provider: &dyn TableProvider) -> bool {
    if provider.is::<StreamSourceTableProvider>() {
        true
    } else if let Some(rename) = provider.downcast_ref::<RenameTableProvider>() {
        is_streaming_table_provider(rename.inner().as_ref())
    } else {
        false
    }
}

struct NamedStreamSource {
    source: Arc<dyn StreamSource>,
    names: Option<Vec<String>>,
}

fn get_stream_source_opt(provider: &dyn TableProvider) -> Option<NamedStreamSource> {
    if let Some(stream) = provider.downcast_ref::<StreamSourceTableProvider>() {
        Some(NamedStreamSource {
            source: stream.source().clone(),
            names: None,
        })
    } else if let Some(rename) = provider.downcast_ref::<RenameTableProvider>() {
        match get_stream_source_opt(rename.inner().as_ref()) {
            Some(stream) => Some(NamedStreamSource {
                source: stream.source,
                names: Some(
                    rename
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect(),
                ),
            }),
            _ => None,
        }
    } else {
        None
    }
}

pub fn is_streaming_plan(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|plan| {
        if let LogicalPlan::TableScan(scan) = plan {
            Ok(source_as_provider(&scan.source)
                .is_ok_and(|p| is_streaming_table_provider(p.as_ref())))
        } else {
            Ok(false)
        }
    })
}

/// Rewrite a logical plan for streaming execution.
/// This function needs to be called on an optimized logical plan, and after
/// all logical commands are executed. An error will be returned if the plan
/// contains logical command nodes or nodes that should be eliminated by the
/// optimizer (e.g. subquery).
pub fn rewrite_streaming_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    let node = plan.rewrite(&mut StreamingRewriter)?;
    let plan = node.data;

    if is_flow_event_schema(plan.schema().inner()) {
        // If the plan has a flow event schema, it is a streaming query without sink.
        // So we need to collect the (retractable) data batches.
        // During physical planning, the stream collector will return an error if the plan
        // is not bounded, since the query result cannot have infinite size.
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(StreamCollectorNode::try_new(Arc::new(plan))?),
        }))
    } else {
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::Session;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_common::{DFSchema, DataFusionError, internal_datafusion_err};
    use datafusion_expr::logical_plan::information_schema::ColumnsDetail;
    use datafusion_expr::{
        AsOfJoin, AsOfMatch, Expr, JoinConstraint, LogicalPlanBuilder, LogicalTableSource,
        MaterializedCte, MaterializedCteId, MaterializedCtePlan, MaterializedCteScan, Operator,
        Show, ShowKind, TableScanBuilder, TableSource, lit,
    };
    use datafusion_expr_common::statistics::StatisticsRequest;

    use super::*;

    #[derive(Debug)]
    struct TestStreamSource(SchemaRef);

    #[async_trait::async_trait]
    impl StreamSource for TestStreamSource {
        fn data_schema(&self) -> SchemaRef {
            Arc::clone(&self.0)
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            internal_err!("planning test must not execute a stream source")
        }
    }

    fn data_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn table_scan(
        source: Arc<dyn TableSource>,
        skip: Option<usize>,
        filter_column: &str,
    ) -> Result<TableScan> {
        TableScanBuilder::new("t", source)
            .with_projection(Some(vec![1]))
            .with_filters(vec![col(filter_column).gt(lit(1i64))])
            .with_fetch(Some(8))
            .with_skip(skip)
            .with_statistics_requests([StatisticsRequest::RowCount].into_iter().collect())
            .build()
    }

    fn extension<T: UserDefinedLogicalNode + 'static>(plan: &LogicalPlan) -> Result<&T> {
        let LogicalPlan::Extension(extension) = plan else {
            return internal_err!("expected extension node, got {plan:?}");
        };
        extension
            .node
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| internal_datafusion_err!("expected {}", std::any::type_name::<T>()))
    }

    #[test]
    fn stream_scan_skip_preserves_options_or_is_rejected() -> Result<()> {
        for renamed in [false, true] {
            let source: Arc<dyn StreamSource> = Arc::new(TestStreamSource(data_schema()));
            let provider: Arc<dyn TableProvider> =
                Arc::new(StreamSourceTableProvider::new(Arc::clone(&source)));
            let names = renamed.then(|| vec!["x".to_string(), "y".to_string()]);
            let provider: Arc<dyn TableProvider> = match &names {
                Some(names) => Arc::new(RenameTableProvider::try_new(provider, names.clone())?),
                None => provider,
            };
            assert!(!provider.supports_skip_pushdown());
            for skip in [None, Some(0), Some(1)] {
                let scan = table_scan(
                    provider_as_source(Arc::clone(&provider)),
                    skip,
                    if renamed { "x" } else { "a" },
                )?;
                // The residual limit owns the offset; the wrapper must not apply it again.
                let plan = LogicalPlanBuilder::from(LogicalPlan::TableScan(scan))
                    .limit(2, Some(3))?
                    .build()?;
                let result = rewrite_streaming_plan(plan);
                if skip == Some(1) {
                    assert!(
                        matches!(result, Err(DataFusionError::NotImplemented(message))
                        if message == "streaming table scan with pushed-down skip: Some(1)")
                    );
                    continue;
                }
                let plan = result?;
                let collector = extension::<StreamCollectorNode>(&plan)?;
                let limit = extension::<StreamLimitNode>(collector.inputs()[0])?;
                assert_eq!(limit.skip(), 2);
                assert_eq!(limit.fetch(), Some(3));
                let wrapper = extension::<StreamSourceWrapperNode>(limit.input())?;
                assert!(Arc::ptr_eq(wrapper.source(), &source));
                assert_eq!(wrapper.names(), names.as_ref());
                assert_eq!(wrapper.projection(), Some(&vec![1]));
                assert_eq!(wrapper.filters(), &vec![col("a").gt(lit(1i64))]);
                assert_eq!(wrapper.fetch(), Some(8));
            }
        }
        Ok(())
    }

    #[test]
    fn non_stream_scan_adapter_preserves_original_scan() -> Result<()> {
        let schema = data_schema();
        let sources: [Arc<dyn TableSource>; 2] = [
            provider_as_source(Arc::new(EmptyTable::new(Arc::clone(&schema)))),
            Arc::new(LogicalTableSource::new(schema)),
        ];
        for source in sources {
            for skip in [None, Some(0), Some(1)] {
                let scan = table_scan(Arc::clone(&source), skip, "a")?;
                let original = LogicalPlan::TableScan(scan.clone());
                let plan = rewrite_streaming_plan(original.clone())?;
                let collector = extension::<StreamCollectorNode>(&plan)?;
                let adapter = extension::<StreamSourceAdapterNode>(collector.inputs()[0])?;
                let input = adapter.inputs()[0];
                assert_eq!(input, &original);
                let LogicalPlan::TableScan(actual) = input else {
                    return internal_err!("expected original table scan");
                };
                // TableScan equality does not compare skip, statistics requests or source.
                assert_eq!(actual.skip, skip);
                assert_eq!(actual.statistics_requests, scan.statistics_requests);
                assert!(Arc::ptr_eq(&actual.source, &source));
            }
        }
        Ok(())
    }

    #[test]
    fn show_commands_are_rejected() {
        for kind in [
            ShowKind::Describe {
                schema: data_schema(),
            },
            ShowKind::Tables,
            ShowKind::Columns {
                table_ref: "t".into(),
                detail: ColumnsDetail::Describe,
            },
            ShowKind::CreateTable {
                table_ref: "t".into(),
            },
            ShowKind::Variables {
                name: None,
                verbose: false,
            },
            ShowKind::Functions { filter: None },
        ] {
            let plan = LogicalPlan::Show(Show {
                kind,
                output_schema: Arc::clone(DFSchema::empty_ref()),
            });
            assert!(
                matches!(rewrite_streaming_plan(plan), Err(DataFusionError::Internal(message))
                if message.starts_with("unexpected command for streaming rewriter: "))
            );
        }
    }

    #[test]
    fn materialized_cte_nodes_are_not_supported() -> Result<()> {
        let id = MaterializedCteId::next();
        let input = Arc::new(LogicalPlan::default());
        let scan = LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(
            MaterializedCteScan::new(id, "cte", Arc::clone(input.schema())),
        ));
        let cte = LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(MaterializedCte::try_new(
            id,
            "cte",
            input,
            Arc::new(scan.clone()),
        )?));
        for plan in [scan, cte] {
            for result in [
                StreamingRewriter.f_up(plan.clone()).map(|x| x.data),
                rewrite_streaming_plan(plan),
            ] {
                assert!(
                    matches!(result, Err(DataFusionError::NotImplemented(message))
                    if message.starts_with("streaming materialized CTE: "))
                );
            }
        }
        Ok(())
    }

    #[test]
    fn asof_join_is_not_supported() -> Result<()> {
        let source = Arc::new(LogicalTableSource::new(data_schema()));
        let left = LogicalPlan::TableScan(TableScanBuilder::new("l", source.clone()).build()?);
        let right = LogicalPlan::TableScan(TableScanBuilder::new("r", source).build()?);
        let plan = LogicalPlan::AsOfJoin(AsOfJoin::try_new(
            Arc::new(left),
            Arc::new(right),
            vec![],
            AsOfMatch::new(col("l.a"), Operator::GtEq, col("r.a")),
            JoinConstraint::On,
        )?);
        assert!(
            matches!(rewrite_streaming_plan(plan), Err(DataFusionError::NotImplemented(message))
            if message.starts_with("streaming join: "))
        );
        Ok(())
    }
}
