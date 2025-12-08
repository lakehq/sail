Feature: EXPLAIN statement returns Spark-style plan text

  Scenario: Default EXPLAIN returns full physical plan
    When query
      """
      EXPLAIN SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]
      """

  Scenario: EXPLAIN EXTENDED returns logical and physical plans
    When query
      """
      EXPLAIN EXTENDED SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
      == Parsed Logical Plan ==
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(t.#3)]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      == Analyzed Logical Plan ==
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]
      """

  Scenario: EXPLAIN CODEGEN shows codegen notice and physical plan
    When query
      """
      EXPLAIN CODEGEN SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
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
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(t.#3)]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after type_coercion:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      analyzed_logical_plan:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_nested_union:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after simplify_expressions:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after replace_distinct_aggregate:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after decorrelate_predicate_subquery:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after scalar_subquery_to_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after decorrelate_lateral_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after extract_equijoin_predicate:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_duplicated_expr:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_filter:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_cross_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_limit:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after propagate_empty_relation:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_one_union:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after filter_null_join_keys:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_outer_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after push_down_limit:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after push_down_filter:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after single_distinct_aggregation_to_group_by:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_group_by_constant:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after common_sub_expression_eliminate:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after optimize_projections:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_nested_union:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after simplify_expressions:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after replace_distinct_aggregate:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after decorrelate_predicate_subquery:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after scalar_subquery_to_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after decorrelate_lateral_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after extract_equijoin_predicate:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_duplicated_expr:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_filter:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_cross_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_limit:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after propagate_empty_relation:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_one_union:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after filter_null_join_keys:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_outer_join:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after push_down_limit:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after push_down_filter:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after single_distinct_aggregation_to_group_by:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after eliminate_group_by_constant:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after common_sub_expression_eliminate:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan after optimize_projections:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      logical_plan:
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      initial_physical_plan:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                  ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                    DataSourceExec: partitions=1, partition_sizes=[1]


      initial_physical_plan_with_stats:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
          CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                  ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], statistics=[Rows=Exact(2), Bytes=Exact(16), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]
                    DataSourceExec: partitions=1, partition_sizes=[1], statistics=[Rows=Exact(2), Bytes=Exact(224), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]


      initial_physical_plan_with_schema:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], schema=[#4:Int32;N, #5:Int64;N]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3):Int64;N]
          CoalesceBatchesExec: target_batch_size=8192, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                  ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], schema=[#2:Int32;N, #3:Int32;N]
                    DataSourceExec: partitions=1, partition_sizes=[1], schema=[column1:Int32;N, column2:Int32;N]


      physical_plan after OutputRequirements:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after aggregate_statistics:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after join_selection:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after LimitedDistinctAggregation:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after FilterPushdown:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after EnforceDistribution:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after CombinePartialFinalAggregate:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after EnforceSorting:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after OptimizeAggregateOrder:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after ProjectionPushdown:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after coalesce_batches:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after coalesce_async_exec_input:
      OutputRequirementExec: order_by=[], dist_by=Unspecified
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after OutputRequirements:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after LimitAggregation:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after LimitPushPastWindows:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after LimitPushdown:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after ProjectionPushdown:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after EnsureCooperative:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after FilterPushdown(Post):
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after RewriteExplicitRepartition:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan after SanityCheckPlan:
      ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
        AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                CoalesceBatchesExec: target_batch_size=8192
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]


      physical_plan:
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]


      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]
      """

  Scenario: EXPLAIN FORMATTED includes statistics and schema
    When query
      """
      EXPLAIN FORMATTED SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]], schema=[k:Int32;N, SUM(v):Int64;N]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]], schema=[#4:Int32;N, #5:Int64;N]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3):Int64;N]
            CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                  CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], statistics=[Rows=Exact(2), Bytes=Exact(16), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]], schema=[#2:Int32;N, #3:Int32;N]
                        DataSourceExec: partitions=1, partition_sizes=[1], statistics=[Rows=Exact(2), Bytes=Exact(224), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]], schema=[column1:Int32;N, column2:Int32;N]
      """

  Scenario: EXPLAIN COST shows logical plans with stats
    When query
      """
      EXPLAIN COST SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
      == Parsed Logical Plan ==
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(t.#3)]]
          SubqueryAlias: t
            Projection: #0 AS #2, #1 AS #3
              Projection: column1 AS #0, column2 AS #1
                Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      == Analyzed Logical Plan ==
      Projection: t.#2 AS #4, sum(t.#3) AS #5
        Aggregate: groupBy=[[t.#2]], aggr=[[sum(CAST(t.#3 AS Int64))]]
          SubqueryAlias: t
            Projection: column1 AS #2, column2 AS #3
              Values: (Int32(1), Int32(2)), (Int32(1), Int32(3))

      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
            CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                  CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], statistics=[Rows=Exact(2), Bytes=Exact(16), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]
                        DataSourceExec: partitions=1, partition_sizes=[1], statistics=[Rows=Exact(2), Bytes=Exact(224), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]
      """

  Scenario: EXPLAIN ANALYZE executes and returns physical plan
    When query
      """
      EXPLAIN ANALYZE SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)], metrics=[output_rows=1, elapsed_compute=509ns, output_bytes=544.0 B], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]], schema=[k:Int32;N, SUM(v):Int64;N]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], metrics=[output_rows=1, elapsed_compute=1.176µs, output_bytes=544.0 B], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]], schema=[#4:Int32;N, #5:Int64;N]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], metrics=[output_rows=1, elapsed_compute=696.499µs, output_bytes=544.0 B, spill_count=0, spilled_bytes=0.0 B, spilled_rows=0, peak_mem_used=4864, aggregate_arguments_time=1.343µs, aggregation_time=12.009µs, emitting_time=2.884µs, time_calculating_group_ids=1.509µs], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3):Int64;N]
            CoalesceBatchesExec: target_batch_size=8192, metrics=[output_rows=1, elapsed_compute=91.125µs, output_bytes=96.0 KB], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, metrics=[spill_count=0, spilled_bytes=0.0 B, spilled_rows=0, fetch_time=8.746333ms, repartition_time=385.593µs, send_time=2.182µs], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, metrics=[spill_count=0, spilled_bytes=0.0 B, spilled_rows=0, fetch_time=1.242291ms, repartition_time=1ns, send_time=11.925µs], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                  CoalesceBatchesExec: target_batch_size=8192, metrics=[], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], metrics=[output_rows=1, elapsed_compute=1.142792ms, output_bytes=544.0 B, spill_count=0, spilled_bytes=0.0 B, spilled_rows=0, skipped_aggregation_rows=0, peak_mem_used=4288, aggregate_arguments_time=275.458µs, aggregation_time=140.958µs, emitting_time=90.917µs, time_calculating_group_ids=86.375µs, reduction_factor=50% (1/2)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], metrics=[output_rows=2, elapsed_compute=3.292µs, output_bytes=32.0 B], statistics=[Rows=Exact(2), Bytes=Exact(16), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]], schema=[#2:Int32;N, #3:Int32;N]
                        DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(2), Bytes=Exact(224), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]], schema=[column1:Int32;N, column2:Int32;N]
      """

  Scenario: EXPLAIN VERBOSE returns detailed physical plan
    When query
      """
      EXPLAIN VERBOSE SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan equals
      """
      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  CoalesceBatchesExec: target_batch_size=8192
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                        DataSourceExec: partitions=1, partition_sizes=[1]


      == Physical Plan (with statistics) ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
            CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                  CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], statistics=[Rows=Exact(2), Bytes=Exact(16), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]
                        DataSourceExec: partitions=1, partition_sizes=[1], statistics=[Rows=Exact(2), Bytes=Exact(224), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]]


      == Physical Plan (with schema) ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)], schema=[k:Int32;N, SUM(v):Int64;N]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], schema=[#4:Int32;N, #5:Int64;N]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3):Int64;N]
            CoalesceBatchesExec: target_batch_size=8192, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                  CoalesceBatchesExec: target_batch_size=8192, schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                    AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                      ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], schema=[#2:Int32;N, #3:Int32;N]
                        DataSourceExec: partitions=1, partition_sizes=[1], schema=[column1:Int32;N, column2:Int32;N]
      """
