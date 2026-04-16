from __future__ import annotations

import re
import textwrap
from typing import TYPE_CHECKING

from pytest_bdd import then

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


def normalize_plan_text(plan_text: str) -> str:
    """Normalize plan text by scrubbing non-deterministic fields."""
    text = textwrap.dedent(plan_text).strip()
    # Make Windows paths match the regexes and snapshots early, so the
    # raw-text substitutions below also work cross-platform.
    text = text.replace("\\", "/")
    text = re.sub(r"([A-Za-z][A-Za-z0-9+.\-]+:)//", r"\1__SCHEME_SLASHSLASH__", text)
    text = re.sub(r"/{2,}", "/", text)
    text = text.replace("__SCHEME_SLASHSLASH__", "//")

    def _normalize_metrics_block(match: re.Match[str]) -> str:
        body = match.group(1)
        body = re.sub(r"=\s*[^,\]]+", "=<metric>", body)
        body = re.sub(r"-?\d+(?:\.\d+)?", "<metric>", body)
        return f", metrics=[{body}]"

    text = re.sub(r", metrics=\[([^\]]*)\]", _normalize_metrics_block, text)

    # Normalize temp paths / file URIs that appear in plans.
    pytest_tmp_prefix = re.compile(
        r"(^|[\s\[\(=,:{\"])"
        r"(?!\[)"
        r"(?:(?:[A-Za-z]:)?/|private/|tmp/)"
        r"(?:[^ \t\r\n\),\]/]+/)*"
        r"pytest-of-[^/]+/pytest-\d+/[^/]+/",
        re.IGNORECASE,
    )

    def normalize_path(path: str) -> str:
        path = path.replace("\\", "/")
        path = pytest_tmp_prefix.sub(lambda m: f"{m.group(1)}<tmp>/", path)
        # Normalize Delta Lake parquet files: part-<number>-<UUID>-c<number>.snappy.parquet
        path = re.sub(
            r"part-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-c\d+\.snappy\.parquet",
            "part-<id>.snappy.parquet",
            path,
            flags=re.IGNORECASE,
        )
        # Normalize Iceberg parquet files: part-<UUID>-<sequence>.parquet.
        # The sequence is assigned by write order and is not semantically meaningful in EXPLAIN snapshots.
        return re.sub(
            r"part-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-\d+\.parquet",
            "part-<uuid>.parquet",
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
    # Normalize Delta Lake parquet files: part-<number>-<UUID>-c<number>.snappy.parquet
    text = re.sub(
        r"part-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-c\d+\.snappy\.parquet",
        "part-<id>.snappy.parquet",
        text,
        flags=re.IGNORECASE,
    )
    # Normalize Iceberg parquet files: part-<UUID>-<sequence>.parquet.
    text = re.sub(
        r"part-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-\d+\.parquet",
        "part-<uuid>.parquet",
        text,
        flags=re.IGNORECASE,
    )
    # Normalize Delta V2 UUID-named checkpoint files:
    # e.g. 00000000000000000001.checkpoint.{uuid}.parquet -> 00000000000000000001.checkpoint.<uuid>.parquet
    text = re.sub(
        r"(\d{20}\.checkpoint\.)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(\.parquet)",
        r"\1<uuid>\2",
        text,
        flags=re.IGNORECASE,
    )
    # Normalize Delta V2 sidecar files: _sidecars/{uuid}.parquet -> _sidecars/<uuid>.parquet
    text = re.sub(
        r"(_sidecars/)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(\.parquet)",
        r"\1<uuid>\2",
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

        normalized_groups: list[str] = []
        next_seq = 0
        for group in sorted(groups):
            inner = group[1:-1].strip()
            if not inner:
                normalized_groups.append(group)
                continue
            normalized_entries: list[str] = []
            for entry in sorted(part.strip() for part in inner.split(", ")):
                normalized_entry = entry
                if "part-<uuid>.parquet" in normalized_entry:
                    normalized_entry = normalized_entry.replace(
                        "part-<uuid>.parquet",
                        f"part-<uuid>-{next_seq:020d}.parquet",
                    )
                    next_seq += 1
                normalized_entries.append(normalized_entry)
            normalized_groups.append("[" + ", ".join(normalized_entries) + "]")

        normalized_groups_list = "[" + ", ".join(normalized_groups) + "]"
        return block[:start] + normalized_groups_list + block[end + 1 :]

    text = re.sub(r"file_groups=\{[^}]+\}", _normalize_file_groups_block, text)

    text = re.sub(r"Bytes=Exact\(\d+\)", r"Bytes=Exact(<bytes>)", text)
    return re.sub(r"Bytes=Inexact\(\d+\)", r"Bytes=Inexact(<bytes>)", text)


def _collect_plan(query: str, spark) -> str:
    df = spark.sql(query)
    rows = df.collect()
    assert len(rows) == 1, f"expected single row, got {len(rows)}"
    assert len(rows[0]) == 1, f"expected single column, got {len(rows[0])}"
    plan = rows[0][0]
    assert isinstance(plan, str), "expected string plan output"
    assert plan, "expected non-empty plan output"
    return plan


@then("query plan matches snapshot")
def query_plan_matches_snapshot(query, spark, snapshot: SnapshotAssertion):
    """Executes the SQL query and only asserts against the stored snapshot."""
    plan = _collect_plan(query, spark)
    assert snapshot == normalize_plan_text(plan)
