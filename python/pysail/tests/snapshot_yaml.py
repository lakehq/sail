from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
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


def _quote_yaml_string(value: str) -> str:
    # Snapshot names use JSON quoting (YAML double-quoted compatible).
    return json.dumps(value, ensure_ascii=False)


def _normalize_for_yaml(data: Any) -> Any:
    """
    Normalize data for deterministic YAML output.
    """
    if isinstance(data, dict):
        return {k: _normalize_for_yaml(v) for k, v in data.items()}
    if isinstance(data, (set, frozenset)):
        return [_normalize_for_yaml(v) for v in sorted(data, key=lambda v: (type(v).__name__, str(v)))]
    if isinstance(data, tuple):
        return [_normalize_for_yaml(v) for v in data]
    if isinstance(data, list):
        return [_normalize_for_yaml(v) for v in data]
    if isinstance(data, str):
        return data.replace("\r\n", "\n").replace("\r", "\n")
    return data


class _SnapshotYamlDumper(yaml.SafeDumper):
    pass


def _repr_str(dumper: yaml.SafeDumper, data: str) -> yaml.nodes.ScalarNode:
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def _repr_float(dumper: yaml.SafeDumper, data: float) -> yaml.nodes.ScalarNode:
    # Deterministic NaN/Inf.
    if math.isnan(data):
        return dumper.represent_scalar("tag:yaml.org,2002:float", "nan")
    if math.isinf(data):
        return dumper.represent_scalar("tag:yaml.org,2002:float", "inf" if data > 0 else "-inf")
    return dumper.represent_scalar("tag:yaml.org,2002:float", repr(data))


_SnapshotYamlDumper.add_representer(str, _repr_str)
_SnapshotYamlDumper.add_representer(float, _repr_float)


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
        if matcher is not None:
            try:
                data = matcher(data=data, path=())
            except TypeError:
                data = matcher(data)

        normalized = _normalize_for_yaml(data)
        # `safe_dump` hardcodes the dumper; use our SafeDumper subclass for stable output.
        text = yaml.dump(
            normalized,
            Dumper=_SnapshotYamlDumper,
            sort_keys=True,
            default_flow_style=False,
            allow_unicode=True,
            width=1000,
        ).strip()
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

        # Version marker: first non-empty line.
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
                continue
            if line.strip() == cls._doc_divider:
                flush()
                continue
            if not line.strip():
                if current_name is None and not collecting_data:
                    continue
                if collecting_data:
                    continue
                continue

            if current_name is None:
                if not line.startswith("name:"):
                    raise MalformedYamlSnapshotFileError.expected_name(line=line)
                name_raw = line.split(":", 1)[1].strip()
                if name_raw.startswith(("'", '"')):
                    try:
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

            # Data lines must be indented by exactly two spaces.
            if line.startswith(cls._indent):
                data_lines.append(line[len(cls._indent) :])
            else:
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
