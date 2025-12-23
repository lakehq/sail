== Codegen ==
Whole-stage codegen is not supported; showing physical plan instead.

== Plan Steps ==
initial_logical_plan:
Projection: t.#2 AS #4, sum(t.#3) AS #5
  Aggregate: groupBy=[[t.#2]], aggr=[[sum(t.#3)]]
    SubqueryAlias: t
      Projection: #0 AS #2, #1 AS #3
        Projection: column1 AS #0, column2 AS #1
          Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

logical_plan after resolve_grouping_function:
SAME TEXT AS ABOVE

logical_plan after type_coercion:
Projection: t.#2 AS #4, sum(t.#3) AS #5
  Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
    SubqueryAlias: t
      Projection: #0 AS #2, #1 AS #3
        Projection: column1 AS #0, column2 AS #1
          Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

analyzed_logical_plan:
SAME TEXT AS ABOVE

logical_plan after expand_merge:
SAME TEXT AS ABOVE

logical_plan after eliminate_nested_union:
SAME TEXT AS ABOVE

logical_plan after simplify_expressions:
SAME TEXT AS ABOVE

logical_plan after replace_distinct_aggregate:
SAME TEXT AS ABOVE

logical_plan after eliminate_join:
SAME TEXT AS ABOVE

logical_plan after decorrelate_predicate_subquery:
SAME TEXT AS ABOVE

logical_plan after scalar_subquery_to_join:
SAME TEXT AS ABOVE

logical_plan after decorrelate_lateral_join:
SAME TEXT AS ABOVE

logical_plan after extract_equijoin_predicate:
SAME TEXT AS ABOVE

logical_plan after eliminate_duplicated_expr:
SAME TEXT AS ABOVE

logical_plan after eliminate_filter:
SAME TEXT AS ABOVE

logical_plan after eliminate_cross_join:
SAME TEXT AS ABOVE

logical_plan after eliminate_limit:
SAME TEXT AS ABOVE

logical_plan after propagate_empty_relation:
SAME TEXT AS ABOVE

logical_plan after eliminate_one_union:
SAME TEXT AS ABOVE

logical_plan after filter_null_join_keys:
SAME TEXT AS ABOVE

logical_plan after eliminate_outer_join:
SAME TEXT AS ABOVE

logical_plan after push_down_limit:
SAME TEXT AS ABOVE

logical_plan after push_down_filter:
SAME TEXT AS ABOVE

logical_plan after single_distinct_aggregation_to_group_by:
SAME TEXT AS ABOVE

logical_plan after eliminate_group_by_constant:
SAME TEXT AS ABOVE

logical_plan after common_sub_expression_eliminate:
SAME TEXT AS ABOVE

logical_plan after optimize_projections:
Projection: t.#2 AS #4, sum(t.#3) AS #5
  Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
    SubqueryAlias: t
      Projection: column1 AS #2, column2 AS #3
        Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

logical_plan after expand_merge:
SAME TEXT AS ABOVE

logical_plan after eliminate_nested_union:
SAME TEXT AS ABOVE

logical_plan after simplify_expressions:
SAME TEXT AS ABOVE

logical_plan after replace_distinct_aggregate:
SAME TEXT AS ABOVE

logical_plan after eliminate_join:
SAME TEXT AS ABOVE

logical_plan after decorrelate_predicate_subquery:
SAME TEXT AS ABOVE

logical_plan after scalar_subquery_to_join:
SAME TEXT AS ABOVE

logical_plan after decorrelate_lateral_join:
SAME TEXT AS ABOVE

logical_plan after extract_equijoin_predicate:
SAME TEXT AS ABOVE

logical_plan after eliminate_duplicated_expr:
SAME TEXT AS ABOVE

logical_plan after eliminate_filter:
SAME TEXT AS ABOVE

logical_plan after eliminate_cross_join:
SAME TEXT AS ABOVE

logical_plan after eliminate_limit:
SAME TEXT AS ABOVE

logical_plan after propagate_empty_relation:
SAME TEXT AS ABOVE

logical_plan after eliminate_one_union:
SAME TEXT AS ABOVE

logical_plan after filter_null_join_keys:
SAME TEXT AS ABOVE

logical_plan after eliminate_outer_join:
SAME TEXT AS ABOVE

logical_plan after push_down_limit:
SAME TEXT AS ABOVE

logical_plan after push_down_filter:
SAME TEXT AS ABOVE

logical_plan after single_distinct_aggregation_to_group_by:
SAME TEXT AS ABOVE

logical_plan after eliminate_group_by_constant:
SAME TEXT AS ABOVE

logical_plan after common_sub_expression_eliminate:
SAME TEXT AS ABOVE

logical_plan after optimize_projections:
SAME TEXT AS ABOVE

logical_plan:
Projection: t.#2 AS #4, sum(t.#3) AS #5
  Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
    SubqueryAlias: t
      Projection: column1 AS #2, column2 AS #3
        Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

initial_physical_plan:
ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
  AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
        DataSourceExec: partitions=1, partition_sizes=[<sizes>]


initial_physical_plan_with_stats:
ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], statistics=[Rows=Inexact(2), Bytes=Inexact(<bytes>), [(Col[0]:),(Col[1]:)]]
  AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], statistics=[Rows=Exact(2), Bytes=Exact(<bytes>), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]
        DataSourceExec: partitions=1, partition_sizes=[<sizes>], statistics=[Rows=Exact(2), Bytes=Exact(<bytes>), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]


initial_physical_plan_with_schema:
ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], schema=[#4:Int32;N, #5:Int64;N]
  AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3):Int64;N]
    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], schema=[#2:Int32;N, #3:Int32;N]
        DataSourceExec: partitions=1, partition_sizes=[<sizes>], schema=[column1:Int32;N, column2:Int32;N]


physical_plan after OutputRequirements:
OutputRequirementExec: order_by=[], dist_by=Unspecified
  ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
    AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
      AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
        ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
          DataSourceExec: partitions=1, partition_sizes=[<sizes>]


physical_plan after aggregate_statistics:
SAME TEXT AS ABOVE

physical_plan after join_selection:
SAME TEXT AS ABOVE

physical_plan after LimitedDistinctAggregation:
SAME TEXT AS ABOVE

physical_plan after FilterPushdown:
SAME TEXT AS ABOVE

physical_plan after EnforceDistribution:
OutputRequirementExec: order_by=[], dist_by=Unspecified
  ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
    AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
      RepartitionExec: partitioning=Hash([#2@0], <partitions>), input_partitions=<partitions>
        RepartitionExec: partitioning=RoundRobinBatch(<partitions>), input_partitions=<partitions>
          AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
              DataSourceExec: partitions=1, partition_sizes=[<sizes>]


physical_plan after CombinePartialFinalAggregate:
SAME TEXT AS ABOVE

physical_plan after EnforceSorting:
SAME TEXT AS ABOVE

physical_plan after OptimizeAggregateOrder:
SAME TEXT AS ABOVE

physical_plan after ProjectionPushdown:
SAME TEXT AS ABOVE

physical_plan after coalesce_batches:
OutputRequirementExec: order_by=[], dist_by=Unspecified
  ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
    AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([#2@0], <partitions>), input_partitions=<partitions>
          RepartitionExec: partitioning=RoundRobinBatch(<partitions>), input_partitions=<partitions>
            AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
              ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                DataSourceExec: partitions=1, partition_sizes=[<sizes>]


physical_plan after coalesce_async_exec_input:
SAME TEXT AS ABOVE

physical_plan after OutputRequirements:
ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
  AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([#2@0], <partitions>), input_partitions=<partitions>
        RepartitionExec: partitioning=RoundRobinBatch(<partitions>), input_partitions=<partitions>
          AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
              DataSourceExec: partitions=1, partition_sizes=[<sizes>]


physical_plan after LimitAggregation:
SAME TEXT AS ABOVE

physical_plan after LimitPushPastWindows:
SAME TEXT AS ABOVE

physical_plan after LimitPushdown:
SAME TEXT AS ABOVE

physical_plan after ProjectionPushdown:
SAME TEXT AS ABOVE

physical_plan after EnsureCooperative:
SAME TEXT AS ABOVE

physical_plan after FilterPushdown(Post):
SAME TEXT AS ABOVE

physical_plan after RewriteExplicitRepartition:
SAME TEXT AS ABOVE

physical_plan after SanityCheckPlan:
SAME TEXT AS ABOVE

physical_plan:
ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
  ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
    AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([#2@0], <partitions>), input_partitions=<partitions>
          RepartitionExec: partitioning=RoundRobinBatch(<partitions>), input_partitions=<partitions>
            AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
              ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                DataSourceExec: partitions=1, partition_sizes=[<sizes>]


== Physical Plan ==
ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
  ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
    AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([#2@0], <partitions>), input_partitions=<partitions>
          RepartitionExec: partitioning=RoundRobinBatch(<partitions>), input_partitions=<partitions>
            AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
              ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                DataSourceExec: partitions=1, partition_sizes=[<sizes>]