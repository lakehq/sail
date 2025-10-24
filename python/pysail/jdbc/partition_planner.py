"""Partition planning for distributed JDBC reads."""

from __future__ import annotations

import logging

from pysail.jdbc.query_builder import build_partition_predicate

logger = logging.getLogger("lakesail.jdbc")


class PartitionPlanner:
    """Generate partition predicates for distributed reading."""

    def __init__(
        self,
        partition_column: str | None = None,
        lower_bound: int | None = None,
        upper_bound: int | None = None,
        num_partitions: int = 1,
        predicates: list[str] | None = None,
    ):
        """
        Initialize partition planner.

        Args:
            partition_column: Column for range partitioning
            lower_bound: Lower bound (inclusive)
            upper_bound: Upper bound (inclusive)
            num_partitions: Number of partitions
            predicates: Explicit predicates (overrides range partitioning)
        """
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions
        self.predicates = predicates

    def generate_predicates(self) -> list[str | None]:
        """
        Return list of WHERE clause predicates (one per partition).

        Returns:
            List of predicates (None means no WHERE clause for that partition)
        """
        # Option 1: Explicit predicates list
        if self.predicates:
            logger.info("Using %s explicit predicates", len(self.predicates))
            return self.predicates

        # Option 2: Range partitioning
        if self.partition_column:
            if self.lower_bound is None or self.upper_bound is None:
                msg = "partition_column requires lower_bound and upper_bound"
                raise ValueError(msg)

            return self._generate_range_predicates()

        # Option 3: Single partition (no WHERE)
        logger.info("No partitioning specified, using single partition")
        return [None]

    def _generate_range_predicates(self) -> list[str]:
        """
        Generate range-based partition predicates.

        Returns:
            List of WHERE clause predicates
        """
        col = self.partition_column
        lower = self.lower_bound
        upper = self.upper_bound
        num_parts = self.num_partitions

        data_range = upper - lower

        # Edge case: collapse to 1 partition if range < numPartitions
        adjusted_parts = min(num_parts, max(1, data_range))

        # Warn if we had to reduce partition count
        if adjusted_parts < num_parts:
            logger.warning(
                "Adjusted numPartitions from %s to %s due to small range (%s-%s, span=%s). Some executors will have no work.",
                num_parts,
                adjusted_parts,
                lower,
                upper,
                data_range,
            )

        if adjusted_parts == 1:
            # Single partition: return one predicate covering entire range
            pred = build_partition_predicate(col, lower, upper + 1)
            logger.info("Single partition with predicate: %s", pred)
            return [pred]

        stride = data_range // adjusted_parts
        predicates = []

        for i in range(adjusted_parts):
            start = lower + (i * stride)

            # Last partition includes upper bound
            end = upper + 1 if i == adjusted_parts - 1 else start + stride

            pred = build_partition_predicate(col, start, end)
            predicates.append(pred)

        logger.info(
            "Generated %s range partitions on column '%s' (%s-%s, stride=%s)",
            len(predicates),
            col,
            lower,
            upper,
            stride,
        )

        return predicates
