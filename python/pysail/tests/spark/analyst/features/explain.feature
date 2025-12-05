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
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], schema=[#2:Int32;N, #3:Int32;N]
                      DataSourceExec: partitions=1, partition_sizes=[1], schema=[column1:Int32;N, column2:Int32;N]
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

      == Physical Plan ==
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
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
      ProjectionExec: expr=[#4@0 as k, #5@1 as SUM(v)], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]], schema=[k:Int32;N, SUM(v):Int64;N]
        ProjectionExec: expr=[#2@0 as #4, sum(t.#3)@1 as #5], statistics=[Rows=Inexact(2), Bytes=Inexact(24), [(Col[0]:),(Col[1]:)]], schema=[#4:Int32;N, #5:Int64;N]
          AggregateExec: mode=FinalPartitioned, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3):Int64;N]
            CoalesceBatchesExec: target_batch_size=8192, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
              RepartitionExec: partitioning=Hash([#2@0], 10), input_partitions=10, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)], statistics=[Rows=Inexact(2), Bytes=Absent, [(Col[0]:),(Col[1]:)]], schema=[#2:Int32;N, sum(t.#3)[sum]:Int64;N]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3], statistics=[Rows=Exact(2), Bytes=Exact(16), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]], schema=[#2:Int32;N, #3:Int32;N]
                      DataSourceExec: partitions=1, partition_sizes=[1], statistics=[Rows=Exact(2), Bytes=Exact(224), [(Col[0]: Null=Exact(0)),(Col[1]: Null=Exact(0))]], schema=[column1:Int32;N, column2:Int32;N]
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
                  AggregateExec: mode=Partial, gby=[#2@0 as #2], aggr=[sum(t.#3)]
                    ProjectionExec: expr=[column1@0 as #2, column2@1 as #3]
                      DataSourceExec: partitions=1, partition_sizes=[1]
      """
