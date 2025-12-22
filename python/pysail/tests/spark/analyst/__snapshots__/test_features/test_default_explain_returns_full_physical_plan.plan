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