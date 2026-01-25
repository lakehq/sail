use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::{source_as_provider, TableProvider};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{internal_err, not_impl_err, plan_err, Result};
use datafusion_expr::{
    col, or, Explain, FetchType, Filter, Projection, SkipType, SubqueryAlias, TableScan, Union,
    UserDefinedLogicalNode,
};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::streaming::event::schema::{
    is_flow_event_schema, MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
};
use sail_common_datafusion::streaming::source::{StreamSource, StreamSourceTableProvider};
use sail_logical_plan::file_write::FileWriteNode;
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
        } else if node.as_any().is::<FileWriteNode>() {
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
            LogicalPlan::Join(_) => {
                not_impl_err!("streaming join: {plan:?}")
            }
            LogicalPlan::Repartition(_) => {
                not_impl_err!("streaming repartition: {plan:?}")
            }
            LogicalPlan::TableScan(ref scan) => {
                let provider = source_as_provider(&scan.source)?;
                if let Some(source) = get_stream_source_opt(provider.as_ref()) {
                    let NamedStreamSource { source, names } = source;
                    let TableScan {
                        table_name,
                        source: _,
                        projection,
                        projected_schema: _,
                        filters,
                        fetch,
                    } = scan;
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
                } else {
                    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                    })))
                }
            }
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
            | LogicalPlan::DescribeTable(_) => {
                internal_err!("unexpected command for streaming rewriter: {plan:?}")
            }
        }
    }
}

fn is_streaming_table_provider(provider: &dyn TableProvider) -> bool {
    if provider.as_any().is::<StreamSourceTableProvider>() {
        true
    } else if let Some(rename) = provider.as_any().downcast_ref::<RenameTableProvider>() {
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
    if let Some(stream) = provider
        .as_any()
        .downcast_ref::<StreamSourceTableProvider>()
    {
        Some(NamedStreamSource {
            source: stream.source().clone(),
            names: None,
        })
    } else if let Some(rename) = provider.as_any().downcast_ref::<RenameTableProvider>() {
        if let Some(stream) = get_stream_source_opt(rename.inner().as_ref()) {
            Some(NamedStreamSource {
                source: stream.source,
                names: Some(
                    rename
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect(),
                ),
            })
        } else {
            None
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
