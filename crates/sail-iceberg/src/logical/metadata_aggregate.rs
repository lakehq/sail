use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{Expr, LogicalPlan};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::metadata_aggregate::{
    AggregateInput, ExactAggregateStatistics, rewrite_aggregate,
};

use super::IcebergTableSource;
use crate::datasource::scan::{IcebergScan, IcebergScanPlan};

#[derive(Debug)]
pub struct IcebergMetadataAggregateRewriter;

struct AggregateRequest {
    scan: Arc<IcebergScan>,
    filters: Vec<Expr>,
    planned: Option<Arc<IcebergScanPlan>>,
}

#[async_trait]
impl LogicalRewriter for IcebergMetadataAggregateRewriter {
    fn name(&self) -> &str {
        "iceberg_metadata_aggregate"
    }

    async fn rewrite(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut requests: Vec<AggregateRequest> = vec![];
        plan.apply_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(aggregate) = plan else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if !aggregate.group_expr.is_empty()
                || !aggregate.aggr_expr.iter().any(supported_aggregate)
            {
                return Ok(TreeNodeRecursion::Continue);
            }
            let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let scan = input.scan();
            let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if scan.fetch.is_some()
                || scan.skip.is_some_and(|skip| skip > 0)
                || !source.scan().metadata_aggregate_enabled()
            {
                return Ok(TreeNodeRecursion::Continue);
            }
            let filters = unnormalize_cols(scan.filters.clone());
            if !requests.iter().any(|request| {
                Arc::ptr_eq(&request.scan, source.scan()) && request.filters == filters
            }) {
                requests.push(AggregateRequest {
                    scan: Arc::clone(source.scan()),
                    filters,
                    planned: None,
                });
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        for request in &mut requests {
            request.planned = Some(Arc::new(
                request
                    .scan
                    .plan_files(session, &request.filters, None)
                    .await?,
            ));
        }
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(mut aggregate) = plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let scan = input.scan();
            let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let filters = unnormalize_cols(scan.filters.clone());
            // A shared source's request does not validate another scan's row bounds.
            let Some(request) = requests.iter().find(|request| {
                Arc::ptr_eq(&request.scan, source.scan())
                    && request.filters == filters
                    && scan.fetch.is_none()
                    && !scan.skip.is_some_and(|skip| skip > 0)
            }) else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let Some(planned) = &request.planned else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let statistics =
                source
                    .scan()
                    .exact_statistics(planned)
                    .map(|statistics| AggregateStatistics {
                        schema: source.scan().schema(),
                        statistics,
                    });
            aggregate.input =
                Arc::new(input.replace_source(Arc::new(source.prepare(Arc::clone(planned))))?);
            if let Some(statistics) = statistics
                && let Some(rewritten) = rewrite_aggregate(&aggregate, &statistics)?
            {
                return Ok(Transformed::yes(rewritten));
            }
            // Even when metrics are incomplete, the residual scan reuses this request's file plan.
            Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
        })
    }
}

fn supported_aggregate(expression: &Expr) -> bool {
    match expression {
        Expr::Alias(alias) => supported_aggregate(&alias.expr),
        Expr::AggregateFunction(function) => {
            use datafusion::functions_aggregate::count::Count;
            use datafusion::functions_aggregate::min_max::{Max, Min};
            if function.params.filter.is_some()
                || !function.params.order_by.is_empty()
                || function.params.null_treatment.is_some()
            {
                return false;
            }
            let function = function.func.inner();
            function.is::<Count>() || function.is::<Min>() || function.is::<Max>()
        }
        _ => false,
    }
}

struct AggregateStatistics {
    schema: SchemaRef,
    statistics: Statistics,
}

impl ExactAggregateStatistics for AggregateStatistics {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn row_count(&self) -> Option<usize> {
        match self.statistics.num_rows {
            Precision::Exact(rows) => Some(rows),
            _ => None,
        }
    }
    fn column_statistics(&self, logical_path: &[String]) -> Option<ColumnStatistics> {
        let [name] = logical_path else {
            return None;
        };
        self.statistics
            .column_statistics
            .get(self.schema.index_of(name).ok()?)
            .cloned()
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::functions_aggregate::expr_fn::{count, sum};
    use datafusion::logical_expr::{LogicalPlanBuilder, TableScanBuilder, TableSource};
    use datafusion::prelude::{SessionContext, col, lit};
    use object_store::ObjectStoreExt;
    use object_store::path::Path;
    use url::Url;

    use super::*;
    use crate::io::StoreContext;
    use crate::spec::manifest::{ManifestMetadata, ManifestWriterBuilder};
    use crate::spec::manifest_list::ManifestListWriter;
    use crate::spec::snapshots::{Operation, SnapshotBuilder, Summary};
    use crate::spec::types::{NestedField, PrimitiveType, Type};
    use crate::spec::{DataFile, FormatVersion, ManifestContentType, PartitionSpec, Schema};

    async fn source_fixture(
        context: &SessionContext,
    ) -> Result<(Arc<dyn TableSource>, StoreContext)> {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields([Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let table_url = Url::parse("memory:///metadata-aggregate/").expect("URL");
        let store = Arc::new(object_store::memory::InMemory::new());
        context.register_object_store(&table_url, store.clone());
        let store = StoreContext::new(store, &table_url)?;
        let spec = PartitionSpec::unpartitioned_spec();
        let mut writer = ManifestWriterBuilder::new(
            Some(1),
            None,
            ManifestMetadata::new(
                Arc::new(schema.clone()),
                0,
                spec.clone(),
                FormatVersion::V2,
                ManifestContentType::Data,
            ),
        )
        .build();
        let file: DataFile = serde_json::from_value(serde_json::json!({
            "content": "DATA", "file_path": "data.parquet", "file_format": "PARQUET",
            "partition": [], "record_count": 3, "file_size_in_bytes": 100, "partition_spec_id": 0
        }))
        .expect("data file");
        writer.add(file);
        store
            .prefixed
            .put(
                &Path::from("manifest.avro"),
                writer.to_avro_bytes_v2().expect("manifest bytes").into(),
            )
            .await?;
        let mut list = ManifestListWriter::new();
        list.append(
            writer
                .into_manifest_file("manifest.avro".into(), 1, 1)
                .expect("manifest file"),
        );
        store
            .prefixed
            .put(
                &Path::from("list.avro"),
                list.to_bytes(FormatVersion::V2)
                    .expect("manifest list")
                    .into(),
            )
            .await?;
        let snapshot = SnapshotBuilder::new()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_manifest_list("list.avro")
            .with_summary(Summary::new(Operation::Append))
            .build()
            .expect("snapshot");
        let scan = IcebergScan::new(table_url, schema, snapshot, vec![spec], 0)?;
        Ok((Arc::new(IcebergTableSource::new(Arc::new(scan))), store))
    }

    fn aggregate_plan(
        source: Arc<dyn TableSource>,
        skip: Option<usize>,
        fetch: Option<usize>,
        expressions: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let scan = TableScanBuilder::new("t", source)
            .with_skip(skip)
            .with_fetch(fetch)
            .build()?;
        LogicalPlanBuilder::from(LogicalPlan::TableScan(scan))
            .aggregate(Vec::<Expr>::new(), expressions)?
            .build()
    }

    #[tokio::test]
    async fn scan_skip_does_not_borrow_a_shared_aggregate_request() -> Result<()> {
        let context = SessionContext::new();
        let (source, _) = source_fixture(&context).await?;
        assert!(!source.supports_skip_pushdown());
        let state = context.state();
        for partial in [false, true] {
            let mut expressions = vec![count(lit(1))];
            if partial {
                expressions.push(sum(col("id")));
            }
            let mut branches = vec![];
            for (skip, fetch) in [
                (None, None),
                (Some(0), None),
                (Some(1), None),
                (Some(4), None),
                (None, Some(1)),
            ] {
                branches.push(Arc::new(aggregate_plan(
                    Arc::clone(&source),
                    skip,
                    fetch,
                    expressions.clone(),
                )?));
            }
            let filters = vec![col("id").gt(lit(1))];
            let filtered_scan = TableScanBuilder::new("t", Arc::clone(&source))
                .with_filters(filters.clone())
                .build()?;
            branches.push(Arc::new(
                LogicalPlanBuilder::from(LogicalPlan::TableScan(filtered_scan))
                    .aggregate(Vec::<Expr>::new(), expressions)?
                    .build()?,
            ));
            let plan = LogicalPlan::Union(datafusion::logical_expr::logical_plan::Union::try_new(
                branches.clone(),
            )?);
            let rewritten = IcebergMetadataAggregateRewriter
                .rewrite(plan.clone(), &state)
                .await?;
            assert!(rewritten.transformed);
            assert_eq!(rewritten.data.schema(), plan.schema());
            let LogicalPlan::Union(union) = rewritten.data else {
                panic!("expected union");
            };
            let mut prepared = vec![];
            for (index, branch) in union.inputs.iter().enumerate() {
                if (2..5).contains(&index) {
                    assert_eq!(branch, &branches[index]);
                } else if index == 5 {
                    assert!(matches!(branch.as_ref(), LogicalPlan::Aggregate(_)));
                } else {
                    let LogicalPlan::Projection(projection) = branch.as_ref() else {
                        panic!("expected metadata projection");
                    };
                    let Expr::Alias(alias) = &projection.expr[0] else {
                        panic!("expected count alias");
                    };
                    assert_eq!(alias.expr.as_ref(), &lit(3_i64));
                    if !partial {
                        assert!(
                            matches!(projection.input.as_ref(), LogicalPlan::EmptyRelation(row) if row.produce_one_row)
                        );
                    }
                }
                branch.apply(|node| {
                    if let LogicalPlan::TableScan(scan) = node {
                        let table = scan
                            .source
                            .downcast_ref::<IcebergTableSource>()
                            .expect("Iceberg source");
                        if (2..5).contains(&index) {
                            assert!(Arc::ptr_eq(&scan.source, &source));
                            assert!(table.prepared(&scan.filters, scan.fetch).is_none());
                        } else {
                            if index == 5 {
                                assert_eq!(scan.filters, filters);
                            }
                            prepared.push(
                                table
                                    .prepared(&scan.filters, scan.fetch)
                                    .expect("prepared request"),
                            );
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            if partial {
                assert_eq!(prepared.len(), 3);
                assert!(Arc::ptr_eq(&prepared[0], &prepared[1]));
                assert!(!Arc::ptr_eq(&prepared[0], &prepared[2]));
                assert_eq!(prepared[0].tasks.len(), 1);
            } else {
                assert_eq!(prepared.len(), 1);
            }
            assert!(
                prepared
                    .last()
                    .expect("filtered request")
                    .matches(&filters, None)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn scan_skip_rejection_does_not_read_manifests() -> Result<()> {
        let context = SessionContext::new();
        let (source, store) = source_fixture(&context).await?;
        store.prefixed.delete(&Path::from("manifest.avro")).await?;
        store.prefixed.delete(&Path::from("list.avro")).await?;
        let state = context.state();
        for (skip, fetch) in [
            (Some(1), None),
            (Some(4), None),
            (Some(1), Some(1)),
            (None, Some(1)),
            (Some(0), Some(1)),
        ] {
            let plan = aggregate_plan(Arc::clone(&source), skip, fetch, vec![count(lit(1))])?;
            let rewritten = IcebergMetadataAggregateRewriter
                .rewrite(plan.clone(), &state)
                .await?;
            assert!(!rewritten.transformed);
            assert_eq!(rewritten.data, plan);
        }
        let filtered = LogicalPlanBuilder::scan("t", Arc::clone(&source), None)?
            .filter(col("id").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
            .build()?;
        let rewritten = IcebergMetadataAggregateRewriter
            .rewrite(filtered.clone(), &state)
            .await?;
        assert!(!rewritten.transformed);
        assert_eq!(rewritten.data, filtered);
        for skip in [None, Some(0)] {
            let plan = aggregate_plan(Arc::clone(&source), skip, None, vec![count(lit(1))])?;
            assert!(
                IcebergMetadataAggregateRewriter
                    .rewrite(plan, &state)
                    .await
                    .is_err()
            );
        }
        Ok(())
    }
}
