"""Unit tests for partition planner."""

import pytest

from pysail.jdbc.partition_planner import PartitionPlanner


class TestPartitionPlanner:
    """Test partition predicate generation."""

    def test_no_partitioning(self):
        """Single partition (no WHERE clause) should return [None]."""
        planner = PartitionPlanner()
        predicates = planner.generate_predicates()

        assert predicates == [None]

    def test_explicit_predicates(self):
        """Explicit predicates should flow through unchanged."""
        predicate_list = ["status='active'", "status='pending'", "status='completed'"]
        planner = PartitionPlanner(predicates=predicate_list)

        predicates = planner.generate_predicates()

        assert predicates == predicate_list

    def test_range_partitioning_basic(self):
        """Range partitioning should split the interval evenly."""
        lower = 0
        upper = 100
        partitions = 4

        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=lower,
            upper_bound=upper,
            num_partitions=partitions,
        )

        predicates = planner.generate_predicates()
        stride = (upper - lower) // partitions

        assert len(predicates) == partitions
        assert f'"id" >= {lower} AND "id" < {lower + stride}' in predicates[0]
        assert f'"id" >= {lower + stride} AND "id" < {lower + (2 * stride)}' in predicates[1]
        assert f'"id" >= {lower + (2 * stride)} AND "id" < {lower + (3 * stride)}' in predicates[2]
        assert f'"id" >= {lower + (3 * stride)} AND "id" < {upper + 1}' in predicates[3]

    def test_range_partitioning_single_partition(self):
        """A single requested partition should cover the entire range."""
        lower = 1
        upper = 100

        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=lower,
            upper_bound=upper,
            num_partitions=1,
        )

        predicates = planner.generate_predicates()

        assert len(predicates) == 1
        assert f'"id" >= {lower} AND "id" < {upper + 1}' in predicates[0]

    def test_range_partitioning_small_range(self):
        """When range < requested partitions, planner should cap the count."""
        lower = 0
        upper = 10
        requested_partitions = 20

        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=lower,
            upper_bound=upper,
            num_partitions=requested_partitions,
        )

        predicates = planner.generate_predicates()
        expected_partitions = min(requested_partitions, upper - lower or 1)

        assert len(predicates) == expected_partitions

    def test_range_partitioning_zero_range(self):
        """Lower == upper should yield a single partition."""
        fixed_value = 42

        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=fixed_value,
            upper_bound=fixed_value,
            num_partitions=5,
        )

        predicates = planner.generate_predicates()

        assert len(predicates) == 1

    def test_range_partitioning_uneven_split(self):
        """Remainder should be added to the final partition."""
        lower = 0
        upper = 10
        partitions = 3

        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=lower,
            upper_bound=upper,
            num_partitions=partitions,
        )

        predicates = planner.generate_predicates()
        stride = (upper - lower) // partitions

        assert len(predicates) == partitions
        assert f'"id" >= {lower} AND "id" < {lower + stride}' in predicates[0]
        assert f'"id" >= {lower + stride} AND "id" < {lower + (2 * stride)}' in predicates[1]
        assert f'"id" >= {lower + (2 * stride)} AND "id" < {upper + 1}' in predicates[2]

    def test_range_partitioning_quoted_column(self):
        """Column identifiers should always be quoted."""
        planner = PartitionPlanner(
            partition_column="my_column",
            lower_bound=0,
            upper_bound=100,
            num_partitions=2,
        )

        predicates = planner.generate_predicates()

        assert all('"my_column"' in predicate for predicate in predicates)

    def test_range_partitioning_with_negative_bounds(self):
        """Ranges straddling zero should partition correctly."""
        lower = -100
        upper = 100
        partitions = 4

        planner = PartitionPlanner(
            partition_column="temperature",
            lower_bound=lower,
            upper_bound=upper,
            num_partitions=partitions,
        )

        predicates = planner.generate_predicates()
        stride = (upper - lower) // partitions

        assert len(predicates) == partitions
        assert f'"temperature" >= {lower} AND "temperature" < {lower + stride}' in predicates[0]
        assert f'"temperature" >= {lower + stride} AND "temperature" < {lower + (2 * stride)}' in predicates[1]
        assert f'"temperature" >= {lower + (2 * stride)} AND "temperature" < {lower + (3 * stride)}' in predicates[2]
        assert f'"temperature" >= {lower + (3 * stride)} AND "temperature" < {upper + 1}' in predicates[3]

    def test_missing_bounds_raises_error(self):
        """Missing bounds with partition_column should raise."""
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=None,
            upper_bound=100,
            num_partitions=4,
        )

        with pytest.raises(ValueError, match="requires lower_bound and upper_bound"):
            planner.generate_predicates()

    def test_explicit_predicates_override_range(self):
        """Explicit predicates should override range logic."""
        extra_predicates = ["status='active'", "status='inactive'"]
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=0,
            upper_bound=100,
            num_partitions=4,
            predicates=extra_predicates,
        )

        predicates = planner.generate_predicates()

        assert predicates == extra_predicates
