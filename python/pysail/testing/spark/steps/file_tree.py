from __future__ import annotations

import re
import textwrap
from pathlib import Path

from pytest_bdd import given, parsers, then

from pysail.testing.spark.utils.files import assert_file_lifecycle, get_data_files

_SPARK_PART_FILE_RE = re.compile(
    r"^part-\d+-"
    r"(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"-c\d+\.(?P<codec>[A-Za-z0-9]+)\.parquet$"
)

# Iceberg-specific patterns
_ICEBERG_PART_FILE_RE = re.compile(
    r"^part-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"-\d+\.parquet$"
)
_ICEBERG_METADATA_FILE_RE = re.compile(
    r"^\d+-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.metadata\.json$"
)
_ICEBERG_MANIFEST_FILE_RE = re.compile(
    r"^manifest-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.avro$"
)
_ICEBERG_SNAP_FILE_RE = re.compile(r"^snap-\d+\.avro$")

_UUID_SUFFIX_RE = re.compile(r"^(.+)-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

# Delta V2 checkpoint patterns
_DELTA_UUID_CHECKPOINT_RE = re.compile(
    r"^(\d{20}\.checkpoint\.)"
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"\.parquet$"
)
_DELTA_SIDECAR_FILE_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.parquet$"
)


def _normalize_name(name: str) -> str | None:
    """
    Normalize a single path component.

    Returns:
      - normalized name (str), or
      - None to indicate the entry should be omitted from the tree.
    """
    # Ignore hidden files/dirs to keep trees stable and readable.
    if name.startswith("."):
        return None

    # Ignore Delta transaction logs: we have dedicated delta-log steps for that and
    # keeping `_delta_log` in this tree makes it noisy and version-sensitive.
    if name == "_delta_log":
        return None

    # Ignore Spark marker files.
    if name == "_SUCCESS":
        return None

    # Ignore Iceberg version-hint.text (internal file)
    if name == "version-hint.text":
        return None

    # Normalize Iceberg data file names (part-<uuid>-<seq>.parquet)
    if _ICEBERG_PART_FILE_RE.match(name):
        return "*.parquet"

    # Normalize Iceberg metadata files (<seq>-<uuid>.metadata.json)
    if _ICEBERG_METADATA_FILE_RE.match(name):
        return "*.metadata.json"

    # Normalize Iceberg manifest files (manifest-<uuid>.avro)
    if _ICEBERG_MANIFEST_FILE_RE.match(name):
        return None  # Hide manifest files, they're covered by snap files

    # Normalize Iceberg snapshot files (snap-<id>.avro)
    if _ICEBERG_SNAP_FILE_RE.match(name):
        return "snap-*.avro"

    # Normalize Delta V2 UUID-named checkpoint files
    # e.g., `00000000000000000001.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet`
    #     → `00000000000000000001.checkpoint.<uuid>.parquet`
    m = _DELTA_UUID_CHECKPOINT_RE.match(name)
    if m is not None:
        return f"{m.group(1)}<uuid>.parquet"

    # Normalize Delta V2 sidecar files (UUID-named parquet in _sidecars/)
    # e.g., `3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet` → `<uuid>.parquet`
    if _DELTA_SIDECAR_FILE_RE.match(name):
        return "<uuid>.parquet"

    # Normalize Spark data file names.
    m = _SPARK_PART_FILE_RE.match(name)
    if m is not None:
        _ = m.group("codec")
        return "part-<id>.<codec>.parquet"

    # Normalize UUID suffixes in directory names (e.g., `table-<uuid>` -> `table-<uuid>`).
    m = _UUID_SUFFIX_RE.match(name)
    if m is not None:
        return f"{m.group(1)}-<uuid>"

    return name


def normalize_file_tree_text(text: str) -> str:
    text = textwrap.dedent(text).strip()
    return text.replace("\r\n", "\n").replace("\r", "\n")


def render_normalized_file_tree(root_path: Path) -> str:
    """
    Render a deterministic, normalized file tree for a directory.

    Output format is a simple indented list, designed to be human-readable in `.feature`
    docstrings.
    """
    root_path = Path(root_path)
    lines: list[str] = []

    if not root_path.exists():
        return ""

    def render_dir(current: Path, *, depth: int) -> None:
        entries: list[Path] = sorted(current.iterdir(), key=lambda p: p.name)

        dirs: list[tuple[str, Path]] = []
        files: list[str] = []  # Changed to list[str] to store normalized names
        for p in entries:
            rendered = _normalize_name(p.name)
            if rendered is None:
                continue
            if p.is_dir():
                dirs.append((rendered, p))
            else:
                files.append(rendered)

        for name, p in dirs:
            indent = "  " * depth
            lines.append(f"{indent}📂 {name}")
            render_dir(p, depth=depth + 1)

        for name in files:
            indent = "  " * depth
            lines.append(f"{indent}📄 {name}")

    render_dir(root_path, depth=0)

    return "\n".join(lines)


def _assert_file_tree_matches_docstring(
    location_var: str,
    variables: dict,
    docstring: str,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    real_path = Path(location.path)
    assert real_path.exists(), f"Directory {real_path} does not exist"

    actual = render_normalized_file_tree(real_path)
    expected = normalize_file_tree_text(docstring)
    assert actual == expected


@then(parsers.parse("file tree in {location_var} matches"))
def file_tree_matches_docstring(location_var: str, variables: dict, docstring: str) -> None:
    _assert_file_tree_matches_docstring(location_var, variables, docstring)


@given(parsers.parse("file {filename} in {location_var} is deleted"))
def file_in_location_is_deleted(filename: str, location_var: str, variables: dict) -> None:
    """Deletes a named file from the given location directory."""
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    file_path = Path(location.path) / filename
    assert file_path.exists(), f"File {file_path} does not exist"
    file_path.unlink()


@given(parsers.parse("file {filename} in {location_var} is replaced with"))
def file_in_location_is_replaced_with(
    filename: str,
    location_var: str,
    variables: dict,
    docstring: str,
) -> None:
    """Replaces a named file in the given location directory with the provided text."""
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    file_path = Path(location.path) / filename
    assert file_path.exists(), f"File {file_path} does not exist"
    file_path.write_text(docstring, encoding="utf-8")


@then(parsers.parse("data files in {location_var} count is {n:d}"))
def data_files_count_is(location_var: str, n: int, variables: dict) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    real_path = Path(location.path)
    assert real_path.exists(), f"Directory {real_path} does not exist"

    files = get_data_files(str(real_path))
    assert len(files) == n, f"Expected {n} data files under {real_path}, got {len(files)}"


@then(parsers.parse("data files in {location_var} count is at least {n:d}"))
def data_files_count_is_at_least(location_var: str, n: int, variables: dict) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    real_path = Path(location.path)
    assert real_path.exists(), f"Directory {real_path} does not exist"

    files = get_data_files(str(real_path))
    assert len(files) >= n, f"Expected at least {n} data files under {real_path}, got {len(files)}"


@given(parsers.parse("remember data files in {location_var} as {name}"), target_fixture="variables")
def remember_data_files(location_var: str, name: str, variables: dict) -> dict:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    real_path = Path(location.path)
    assert real_path.exists(), f"Directory {real_path} does not exist"

    variables[name] = set(get_data_files(str(real_path)))
    return variables


@then(parsers.parse("data file lifecycle from {before} to {after} is {operation}"))
def data_file_lifecycle(before: str, after: str, operation: str, variables: dict) -> None:
    files_before = variables.get(before)
    files_after = variables.get(after)
    assert isinstance(files_before, set), f"Variable {before!r} not found or not a set"
    assert isinstance(files_after, set), f"Variable {after!r} not found or not a set"
    assert_file_lifecycle(files_before, files_after, operation)


@then(parsers.parse("CSV files in {location_var} first line is {expected_line}"))
def csv_files_first_line_is(location_var: str, expected_line: str, variables: dict) -> None:
    """Verify that the first line of CSV files in the location matches the expected pattern."""
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    real_path = Path(location.path)
    assert real_path.exists(), f"Directory {real_path} does not exist"

    csv_files = get_data_files(str(real_path), extension=".csv")
    assert len(csv_files) > 0, f"No CSV files found under {real_path}"

    first_file_rel = csv_files[0]
    first_file = real_path / first_file_rel
    assert first_file.exists(), f"CSV file {first_file} does not exist"

    with open(first_file, encoding="utf-8") as f:
        actual_first_line = f.readline().rstrip("\n\r")

    assert actual_first_line == expected_line, (
        f"CSV file {first_file} first line is {actual_first_line!r}, expected {expected_line!r}"
    )


@then(parsers.parse("subdirectories in {location_var} count is {n:d}"))
def subdirectories_count_is(location_var: str, n: int, variables: dict) -> None:
    """Verify that the location has exactly n subdirectories."""
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    real_path = Path(location.path)
    assert real_path.exists(), f"Directory {real_path} does not exist"

    subdirs = [p for p in real_path.iterdir() if p.is_dir() and not p.name.startswith(".")]
    assert len(subdirs) == n, (
        f"Expected {n} subdirectories under {real_path}, got {len(subdirs)}: {[d.name for d in subdirs]}"
    )
