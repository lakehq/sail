from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from syrupy.data import Snapshot, SnapshotCollection
from syrupy.exceptions import TaintedSnapshotError
from syrupy.extensions.base import AbstractSyrupyExtension

if TYPE_CHECKING:
    from syrupy.types import SerializableData


class MalformedYamlSnapshotFileError(ValueError):
    @classmethod
    def expected_name(cls, *, line: str) -> MalformedYamlSnapshotFileError:
        msg = f"Expected 'name:' but got: {line!r}"
        return cls(msg)

    @classmethod
    def invalid_quoted_name(cls, *, name_raw: str) -> MalformedYamlSnapshotFileError:
        msg = f"Invalid quoted name: {name_raw!r}"
        return cls(msg)

    @classmethod
    def expected_data_header(cls, *, line: str) -> MalformedYamlSnapshotFileError:
        msg = f"Expected 'data:' but got: {line!r}"
        return cls(msg)

    @classmethod
    def expected_indented_data_line(cls, *, line: str) -> MalformedYamlSnapshotFileError:
        msg = f"Expected indented data line but got: {line!r}"
        return cls(msg)


_SAFE_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_RESERVED_PLAIN = {"null", "Null", "NULL", "~", "true", "True", "TRUE", "false", "False", "FALSE"}


def _quote_yaml_string(value: str) -> str:
    # YAML double-quoted scalars are compatible with JSON string escaping.
    return json.dumps(value, ensure_ascii=False)


def _format_yaml_key(key: Any) -> str:
    s = str(key)
    if _SAFE_KEY_RE.fullmatch(s) and s not in _RESERVED_PLAIN:
        return s
    return _quote_yaml_string(s)


def _format_yaml_number(value: float) -> str:
    if isinstance(value, bool):  # bool is a subclass of int
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    # repr() is stable and round-trippable enough for snapshot text.
    return repr(value)


def _indent_lines(text: str, level: int, *, indent: str = "  ") -> str:
    prefix = indent * level
    return "\n".join((prefix + line) if line else prefix for line in text.splitlines())


def _dump_yaml_value(data: Any, *, level: int = 0, sort_keys: bool = True) -> str:
    """
    Emit a deterministic YAML subset for dict/list/scalars.

    - Scalars are rendered in a single line (strings are always quoted, unless multiline).
    - Multiline strings are rendered as block scalars using `|-`.
    - dict keys are emitted as plain scalars when safe, otherwise quoted.
    """
    if data is None:
        return "null"
    if isinstance(data, bool):
        return "true" if data else "false"
    if isinstance(data, (int, float)):
        return _format_yaml_number(data)
    if isinstance(data, str):
        if "\n" in data or "\r" in data:
            # Normalize newlines to be stable across platforms.
            normalized = data.replace("\r\n", "\n").replace("\r", "\n")
            # Use chomping indicator to avoid trailing newline differences.
            lines = normalized.split("\n")
            block = "\n".join(lines)
            return "|\n" + _indent_lines(block, level + 1)
        return _quote_yaml_string(data)
    if isinstance(data, (list, tuple, set, frozenset)):
        items = list(data) if not isinstance(data, (list, tuple)) else data
        if not items:
            return "[]"
        out_lines: list[str] = []
        for item in items:
            if isinstance(item, (dict, list, tuple, set, frozenset)) or (
                isinstance(item, str) and ("\n" in item or "\r" in item)
            ):
                out_lines.append("-")
                # Dump nested values at column 0, then indent them under the list item.
                out_lines.append(_indent_lines(_dump_yaml_value(item, level=0, sort_keys=sort_keys), 1))
            else:
                out_lines.append(f"- {_dump_yaml_value(item, level=level, sort_keys=sort_keys)}")
        return _indent_lines("\n".join(out_lines), level)
    if isinstance(data, dict):
        if not data:
            return "{}"
        keys = list(data.keys())
        if sort_keys:
            keys = sorted(keys, key=lambda k: str(k))
        out_lines = []
        for k in keys:
            v = data[k]
            k_s = _format_yaml_key(k)
            if isinstance(v, (dict, list, tuple, set, frozenset)) or (isinstance(v, str) and ("\n" in v or "\r" in v)):
                out_lines.append(f"{k_s}:")
                out_lines.append(_indent_lines(_dump_yaml_value(v, level=0, sort_keys=sort_keys), 1))
            else:
                out_lines.append(f"{k_s}: {_dump_yaml_value(v, level=0, sort_keys=sort_keys)}")
        return _indent_lines("\n".join(out_lines), level)

    # Fall back to repr() for unknown objects.
    return _quote_yaml_string(repr(data))


@dataclass(frozen=True)
class _ParsedYamlSnapshot:
    name: str
    data_yaml: str


class YamlDataSerializer:
    """
    YAML multi-doc snapshot serializer.

    File format:
      # serializer version: <VERSION>
      ---
      name: <snapshot_name>
      data:
        <yaml for snapshot data>
      ---
      ...

    This keeps snapshots readable and standard YAML.
    """

    VERSION = "1"
    _marker_prefix = "# "
    _marker_version_key = "serializer version"
    _doc_divider = "---"
    _indent = "  "

    @classmethod
    def snapshot_sort_key(cls, snapshot: Snapshot) -> Any:
        return snapshot.name

    @classmethod
    def serialize(
        cls,
        data: SerializableData,
        *,
        exclude: Any = None,  # accepted for Syrupy compatibility; not used
        include: Any = None,  # accepted for Syrupy compatibility; not used
        matcher: Any = None,
    ) -> str:
        _ = exclude
        _ = include
        # Best-effort support for Syrupy's property matcher hook.
        if matcher is not None:
            try:
                data = matcher(data=data, path=())
            except TypeError:
                data = matcher(data)

        text = _dump_yaml_value(data, level=0, sort_keys=True)
        return text.replace("\r\n", "\n").replace("\r", "\n")

    @classmethod
    def write_file(cls, snapshot_collection: SnapshotCollection, *, merge: bool = False) -> None:
        filepath = snapshot_collection.location
        if merge:
            base_snapshot = cls.read_file(filepath)
            base_snapshot.merge(snapshot_collection)
            snapshot_collection = base_snapshot

        with open(filepath, "w", encoding="utf-8", newline=None) as f:
            f.write(f"{cls._marker_prefix}{cls._marker_version_key}: {cls.VERSION}\n")
            for snapshot in sorted(snapshot_collection, key=cls.snapshot_sort_key):
                if snapshot.data is None:
                    continue
                f.write(f"{cls._doc_divider}\n")
                f.write(f"name: {_quote_yaml_string(snapshot.name)}\n")
                f.write("data:\n")
                f.writelines(f"{cls._indent}{line}\n" for line in snapshot.data.splitlines())
                # Ensure there's always at least one line under data:
                if not snapshot.data.splitlines():
                    f.write(f"{cls._indent}null\n")

    @classmethod
    def _parse_file(cls, filepath: str) -> tuple[list[_ParsedYamlSnapshot], bool]:
        """
        Returns (snapshots, tainted_collection).
        """
        try:
            raw = Path(filepath).read_text(encoding="utf-8")
        except FileNotFoundError:
            return [], False

        lines = raw.splitlines(keepends=False)
        tainted = False

        # Version marker is expected at the top of the file (first non-empty line).
        version_line = None
        for line in lines:
            if not line.strip():
                continue
            version_line = line
            break
        if version_line is None or not version_line.startswith(cls._marker_prefix):
            tainted = True
        else:
            marker = version_line[len(cls._marker_prefix) :]
            if ":" not in marker:
                tainted = True
            else:
                key, value = marker.split(":", 1)
                if key.strip() != cls._marker_version_key or value.strip() != cls.VERSION:
                    tainted = True

        snapshots: list[_ParsedYamlSnapshot] = []
        current_name: str | None = None
        collecting_data = False
        data_lines: list[str] = []

        def flush() -> None:
            nonlocal current_name, collecting_data, data_lines
            if current_name is None:
                current_name = None
                collecting_data = False
                data_lines = []
                return
            data_yaml = "\n".join(data_lines).rstrip("\n")
            if not data_yaml:
                data_yaml = "null"
            snapshots.append(_ParsedYamlSnapshot(name=current_name, data_yaml=data_yaml))
            current_name = None
            collecting_data = False
            data_lines = []

        for line in lines:
            if line.startswith(cls._marker_prefix) and current_name is None and not collecting_data:
                # Ignore comments outside a doc.
                continue
            if line.strip() == cls._doc_divider:
                flush()
                continue
            if not line.strip():
                # Ignore blank lines between docs / headers.
                if current_name is None and not collecting_data:
                    continue
                if collecting_data:
                    # We only accept blank lines as part of data when they are indented.
                    # Our writer always indents blank lines, so an unindented blank line
                    # here is treated as separator.
                    continue
                continue

            if current_name is None:
                if not line.startswith("name:"):
                    raise MalformedYamlSnapshotFileError.expected_name(line=line)
                name_raw = line.split(":", 1)[1].strip()
                if name_raw.startswith(("'", '"')):
                    try:
                        # Parse JSON-style quoted strings (we always write JSON quotes).
                        current_name = json.loads(name_raw)
                    except json.JSONDecodeError as e:
                        raise MalformedYamlSnapshotFileError.invalid_quoted_name(name_raw=name_raw) from e
                else:
                    current_name = name_raw
                continue

            if not collecting_data:
                if line.strip() != "data:":
                    raise MalformedYamlSnapshotFileError.expected_data_header(line=line)
                collecting_data = True
                continue

            # Collect data lines; must be indented by exactly two spaces.
            if line.startswith(cls._indent):
                data_lines.append(line[len(cls._indent) :])
            else:
                # Treat unindented content as malformed; this keeps the format strict and predictable.
                raise MalformedYamlSnapshotFileError.expected_indented_data_line(line=line)

        flush()
        return snapshots, tainted

    @classmethod
    def read_file(cls, filepath: str) -> SnapshotCollection:
        snapshot_collection = SnapshotCollection(location=filepath)
        parsed, tainted = cls._parse_file(filepath)
        if tainted:
            snapshot_collection.tainted = True
        for p in parsed:
            snapshot_collection.add(Snapshot(name=p.name, data=p.data_yaml, tainted=tainted))
        return snapshot_collection


class YamlSnapshotExtension(AbstractSyrupyExtension):
    """
    Syrupy extension that stores snapshots as YAML multi-document files.
    """

    file_extension = "yaml"
    serializer_class: type[YamlDataSerializer] = YamlDataSerializer

    def serialize(self, data: SerializableData, **kwargs: Any) -> str:
        return self.serializer_class.serialize(data, **kwargs)

    def delete_snapshots(self, snapshot_location: str, snapshot_names: set[str]) -> None:
        snapshot_collection_to_update = self.serializer_class.read_file(snapshot_location)
        for snapshot_name in snapshot_names:
            snapshot_collection_to_update.remove(snapshot_name)

        if snapshot_collection_to_update.has_snapshots:
            self.serializer_class.write_file(snapshot_collection_to_update)
        else:
            Path(snapshot_location).unlink()

    def read_snapshot_collection(self, snapshot_location: str) -> SnapshotCollection:
        return self.serializer_class.read_file(snapshot_location)

    @classmethod
    @lru_cache
    def __cacheable_read_snapshot(cls, snapshot_location: str, cache_key: str) -> SnapshotCollection:
        _ = cache_key
        return cls.serializer_class.read_file(snapshot_location)

    def read_snapshot_data_from_location(
        self, snapshot_location: str, snapshot_name: str, session_id: str
    ) -> SerializableData | None:
        snapshots = self.__cacheable_read_snapshot(snapshot_location=snapshot_location, cache_key=session_id)
        snapshot = snapshots.get(snapshot_name)
        tainted = bool(snapshots.tainted or (snapshot and snapshot.tainted))
        data = snapshot.data if snapshot else None
        if tainted:
            raise TaintedSnapshotError(snapshot_data=data)
        return data

    @classmethod
    def write_snapshot_collection(cls, *, snapshot_collection: SnapshotCollection) -> None:
        cls.serializer_class.write_file(snapshot_collection, merge=True)
