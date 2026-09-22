import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text


@pytest.mark.parametrize("directory", ["/warehouse", "file:///warehouse", "C:\\warehouse"])
def test_pyiceberg_file_normalization_preserves_partition_and_file_layout(directory):
    first = "11111111-2222-3333-4444-555555555555"
    second = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    plan = (
        "DataSourceExec: file_groups={2 groups: "
        f"[[{directory}/p=2/00007-3-{first}.parquet], "
        f"[{directory}/p=1/00001-2-{second}.parquet, {directory}/p=1/00001-2-{first}.parquet]]"
        "}, projection=[id], file_type=parquet, predicate=id@0 = 42"
    )
    directory = directory.replace("\\", "/")
    expected = (
        "DataSourceExec: file_groups={2 groups: "
        f"[[{directory}/p=1/00001-2-<uuid>.parquet, {directory}/p=1/00001-2-<uuid>.parquet], "
        f"[{directory}/p=2/00007-3-<uuid>.parquet]]"
        "}, projection=[id], file_type=parquet, predicate=id@0 = 42"
    )
    assert normalize_plan_text(plan) == expected
    assert normalize_plan_text(expected) == expected


@pytest.mark.parametrize(
    "value",
    [
        "11111111-2222-3333-4444-555555555555.parquet",
        "prefix-00000-0-11111111-2222-3333-4444-555555555555.parquet",
        "00000-0-11111111-2222-3333-4444-555555555555.json",
        "00000-0-not-a-uuid.parquet",
    ],
)
def test_pyiceberg_file_normalization_preserves_other_names(value):
    plan = f"DataSourceExec: file_groups={{1 group: [[/warehouse/{value}]]}}, projection=[id]"
    assert normalize_plan_text(plan) == plan


def test_pyiceberg_file_normalization_preserves_predicate_literals():
    filename = "00000-0-11111111-2222-3333-4444-555555555555.parquet"
    plan = f"DataSourceExec: file_groups={{1 group: [[/warehouse/{filename}]]}}, predicate=name@0 = '{filename}'"
    expected = (
        "DataSourceExec: file_groups={1 group: [[/warehouse/00000-0-<uuid>.parquet]]}, "
        f"predicate=name@0 = '{filename}'"
    )
    assert normalize_plan_text(plan) == expected
