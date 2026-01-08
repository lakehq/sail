from __future__ import annotations

import re
import textwrap
from typing import TYPE_CHECKING

from pytest_bdd import then
from syrupy.extensions.single_file import SingleFileSnapshotExtension

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion
    from syrupy.types import SerializableData


def normalize_plan_text(plan_text: str) -> str:
    """Normalize plan text by scrubbing non-deterministic fields."""
    text = textwrap.dedent(plan_text).strip()
    # Make Windows paths match the regexes and snapshots early, so the
    # raw-text substitutions below also work cross-platform.
    text = text.replace("\\", "/")
    text = re.sub(r"([A-Za-z][A-Za-z0-9+.\-]*:)//", r"\1__SCHEME_SLASHSLASH__", text)
    text = re.sub(r"/{2,}", "/", text)
    text = text.replace("__SCHEME_SLASHSLASH__", "//")

    def _normalize_metrics_block(match: re.Match[str]) -> str:
        body = match.group(1)
        body = re.sub(r"=\s*[^,\]]+", "=<metric>", body)
        body = re.sub(r"-?\d+(?:\.\d+)?", "<metric>", body)
        return f", metrics=[{body}]"

    text = re.sub(r", metrics=\[([^\]]*)\]", _normalize_metrics_block, text)
    text = re.sub(r"Hash\(\[([^\]]+)\], \d+\)", r"Hash([\1], <partitions>)", text)
    text = re.sub(r"RoundRobinBatch\(\d+\)", r"RoundRobinBatch(<partitions>)", text)
    text = re.sub(r"input_partitions=\d+", r"input_partitions=<partitions>", text)
    text = re.sub(r"partition_sizes=\[[^\]]+\]", r"partition_sizes=[<sizes>]", text)

    # Normalize temp paths / file URIs that appear in plans.
    pytest_tmp_prefix = re.compile(
        r"(^|[\s\[\(=,:{\"])"
        r"(?!\[)"
        r"(?:(?:[A-Za-z]:)?/|private/|tmp/)"
        r"(?:[^ \t\r\n\),\]]+/)*"
        r"pytest-of-[^/]+/pytest-\d+/[^/]+/",
        re.IGNORECASE,
    )

    def normalize_path(path: str) -> str:
        path = path.replace("\\", "/")
        path = pytest_tmp_prefix.sub(lambda m: f"{m.group(1)}<tmp>/", path)
        return re.sub(
            r"part-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-c\d+\.snappy\.parquet",
            "part-<id>.snappy.parquet",
            path,
            flags=re.IGNORECASE,
        )

    text = re.sub(
        r"table_path=file://([^\s\),]+)",
        lambda m: f"table_path=file://{normalize_path(m.group(1))}",
        text,
    )
    text = re.sub(
        r'location: "([^"]+)"',
        lambda m: f'location: "{normalize_path(m.group(1))}"',
        text,
    )
    text = pytest_tmp_prefix.sub(lambda m: f"{m.group(1)}<tmp>/", text)
    text = re.sub(
        r"part-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-c\d+\.snappy\.parquet",
        "part-<id>.snappy.parquet",
        text,
        flags=re.IGNORECASE,
    )

    # Normalize file_groups ordering: group ordering is not guaranteed (e.g. parallel listing / async head).
    # TODO: consider sorting the file groups during planner.
    def _normalize_file_groups_block(match: re.Match[str]) -> str:
        block = match.group(0)  # e.g. "file_groups={2 groups: [[...], [...]]}"
        # Extract the group list between the first "[" and the last "]"
        start = block.find("[")
        end = block.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return block
        groups_list = block[start : end + 1]

        # Parse top-level groups inside the outer list.
        # groups_list looks like: "[[a], [b]]" or "[[a]]"
        # Depth 2 means we're inside an inner group (outer list is depth 1, inner group is depth 2)
        inner_group_depth = 2
        groups: list[str] = []
        depth = 0
        current: list[str] = []
        in_group = False
        for ch in groups_list:
            if ch == "[":
                depth += 1
                if depth == inner_group_depth:
                    in_group = True
            if in_group:
                current.append(ch)
            if ch == "]":
                if depth == inner_group_depth and in_group:
                    # End of one group
                    grp = "".join(current).strip()
                    if grp:
                        groups.append(grp)
                    current = []
                    in_group = False
                depth -= 1

        if not groups:
            return block

        groups_sorted = sorted(groups)
        normalized_groups_list = "[" + ", ".join(groups_sorted) + "]"
        return block[:start] + normalized_groups_list + block[end + 1 :]

    text = re.sub(r"file_groups=\{[^}]+\}", _normalize_file_groups_block, text)

    text = re.sub(r"Bytes=Exact\(\d+\)", r"Bytes=Exact(<bytes>)", text)
    return re.sub(r"Bytes=Inexact\(\d+\)", r"Bytes=Inexact(<bytes>)", text)


def _collect_plan(query: str, spark) -> str:
    df = spark.sql(query)
    rows = df.collect()
    assert len(rows) == 1, f"expected single row, got {len(rows)}"
    plan = rows[0][0]
    assert isinstance(plan, str), "expected string plan output"
    assert plan, "expected non-empty plan output"
    return plan


class PlanSnapshotExtension(SingleFileSnapshotExtension):
    """Snapshot extension that stores normalized plan text."""

    file_extension = "plan"

    def serialize(self, data: SerializableData, **_: object) -> bytes:
        return normalize_plan_text(str(data)).encode()


@then("query plan matches snapshot")
def query_plan_matches_snapshot(query, spark, snapshot: SnapshotAssertion):
    """Executes the SQL query and only asserts against the stored snapshot."""
    plan = _collect_plan(query, spark)
    assert snapshot(extension_class=PlanSnapshotExtension) == plan
