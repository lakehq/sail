#!/usr/bin/env python
"""Review release package artifacts and write a Markdown report."""

import argparse
import hashlib
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class ArchiveEntry:
    path: PurePosixPath
    is_dir: bool
    mode: int | None
    mtime: datetime | None
    size: int


def code_text(value: str) -> str:
    return f"`{value}`"


def human_size(size: int) -> str:
    value = float(size)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if value < 1024 or unit == "TiB":  # noqa: PLR2004
            if unit == "B":
                return f"{size} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    message = "unreachable"
    raise AssertionError(message)


def mode_text(mode: int | None) -> str:
    return "-" if mode is None else f"{mode & 0o7777:04o}"


def type_text(*, is_dir: bool) -> str:
    return "Directory" if is_dir else "File"


def mtime_text(mtime: datetime) -> str:
    return mtime.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(path: Path, algorithm: str) -> str:
    h = hashlib.new(algorithm)
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def directory_table(path: Path) -> list[str]:
    rows = ["| Type | Mode | Size (B) | Modified | Name |", "| --- | ---: | ---: | ---: | --- |"]
    for item in sorted(path.iterdir(), key=lambda value: value.name.casefold()):
        stat = item.stat()
        rows.append(
            "| "
            f"{type_text(is_dir=item.is_dir())} | "
            f"{mode_text(stat.st_mode)} | "
            f"{stat.st_size} | "
            f"{code_text(mtime_text(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)))} | "
            f"{code_text(item.name)} |"
        )
    return rows


def normalize_archive_path(value: str) -> PurePosixPath | None:
    path = PurePosixPath(value)
    parts = [part for part in path.parts if part not in {"", "."}]
    if not parts or any(part == ".." for part in parts):
        return None
    return PurePosixPath(*parts)


def add_parent_directories(entries: dict[PurePosixPath, ArchiveEntry], path: PurePosixPath) -> None:
    for index in range(1, len(path.parts)):
        parent = PurePosixPath(*path.parts[:index])
        entries.setdefault(parent, ArchiveEntry(path=parent, is_dir=True, mode=None, mtime=None, size=0))


def tar_gz_entries(path: Path) -> list[ArchiveEntry]:
    entries: dict[PurePosixPath, ArchiveEntry] = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            archive_path = normalize_archive_path(member.name)
            if archive_path is None:
                continue
            add_parent_directories(entries, archive_path)
            entries[archive_path] = ArchiveEntry(
                path=archive_path,
                is_dir=member.isdir(),
                mode=member.mode,
                mtime=datetime.fromtimestamp(member.mtime, tz=timezone.utc),
                size=member.size,
            )
    return sorted(entries.values(), key=lambda entry: entry.path.as_posix())


def zip_mode(info: zipfile.ZipInfo) -> int | None:
    mode = (info.external_attr >> 16) & 0o7777
    return mode or None


def zip_mtime(info: zipfile.ZipInfo) -> datetime | None:
    try:
        return datetime(*info.date_time, tzinfo=timezone.utc)
    except ValueError:
        return None


def wheel_entries(path: Path) -> list[ArchiveEntry]:
    entries: dict[PurePosixPath, ArchiveEntry] = {}
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            archive_path = normalize_archive_path(info.filename)
            if archive_path is None:
                continue
            add_parent_directories(entries, archive_path)
            entries[archive_path] = ArchiveEntry(
                path=archive_path,
                is_dir=info.is_dir(),
                mode=zip_mode(info),
                mtime=zip_mtime(info),
                size=info.file_size,
            )
    return sorted(entries.values(), key=lambda entry: entry.path.as_posix())


def archive_entries(path: Path) -> list[ArchiveEntry] | None:
    if path.name.endswith(".tar.gz"):
        return tar_gz_entries(path)
    if path.suffix == ".whl":
        return wheel_entries(path)
    return None


def grouped_file_name(path: PurePosixPath) -> str:
    return f"*{path.suffix}" if path.suffix else path.name


def entry_stats_table(entries: list[ArchiveEntry]) -> list[str]:
    def sort_key(value: tuple[tuple[str, str], list[int]]) -> tuple[str, str]:
        (type_, name), _ = value
        return type_, name.casefold()

    groups: dict[tuple[str, str], list[int]] = {("Directory", "*"): [entry.size for entry in entries if entry.is_dir]}
    for entry in entries:
        if not entry.is_dir:
            groups.setdefault(("File", grouped_file_name(entry.path)), []).append(entry.size)

    rows = [
        "| Type | Name | Count | Maximum Size (B) | Minimum Size (B) | Total Size (B) |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for (type_, name), sizes in sorted(groups.items(), key=sort_key):
        if sizes:
            rows.append(f"| {type_} | {code_text(name)} | {len(sizes)} | {max(sizes)} | {min(sizes)} | {sum(sizes)} |")
    return rows


def top_level_content_table(entries: list[ArchiveEntry]) -> list[str]:
    rows = ["| Type | Mode | Modified | Path |", "| --- | ---: | ---: | --- |"]
    for entry in entries:
        if len(entry.path.parts) > 2:  # noqa: PLR2004
            continue
        rows.append(
            f"| {type_text(is_dir=entry.is_dir)} | "
            f"{mode_text(entry.mode)} | "
            f"{'-' if entry.mtime is None else code_text(mtime_text(entry.mtime))} | "
            f"{code_text(entry.path.as_posix())} |"
        )
    if len(rows) == 2:  # noqa: PLR2004
        rows.append("| (none) | - | - | - |")
    return rows


def artifact_lines(path: Path) -> list[str]:
    entries = archive_entries(path)
    if entries is None:
        return []
    size = path.stat().st_size
    return [
        f"### File: {code_text(path.name)}",
        "",
        "#### Basic Information",
        "",
        f"* MD5 checksum: `{digest(path, 'md5')}`",
        f"* SHA-1 checksum: `{digest(path, 'sha1')}`",
        f"* SHA-256 checksum: `{digest(path, 'sha256')}`",
        f"* Size: {size} B ({human_size(size)})",
        "",
        "#### Top-Level Contents",
        "",
        *top_level_content_table(entries),
        "",
        "#### Entry Statistics",
        "",
        *entry_stats_table(entries),
    ]


def all_artifact_lines(path: Path) -> list[str]:
    lines: list[str] = []
    for artifact in sorted(
        (item for item in path.iterdir() if item.is_file()), key=lambda value: value.name.casefold()
    ):
        if x := artifact_lines(artifact):
            lines.extend(x)
            lines.append("")
    return lines or ["(none)"]


def report(path: Path) -> str:
    if not path.is_dir():
        message = f"artifact path is not a directory: {path}"
        raise OSError(message)

    lines = [
        "## Artifact List",
        "",
        *directory_table(path),
        "",
        "## Artifact Details",
        "",
        *all_artifact_lines(path),
    ]
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path", required=True, type=Path, help="directory containing release artifacts")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        sys.stdout.write(report(args.path))
    except (OSError, tarfile.TarError, zipfile.BadZipFile) as error:
        sys.stderr.write(f"error: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
