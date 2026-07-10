# ruff: noqa: EM101, EM102, TRY003

import os
import stat
import tarfile
import zipfile


def _validate_relative_member(name, root):
    if name is None:
        raise ValueError("unsafe archive member path: <none>")
    name = str(name)
    if "\\" in name:
        raise ValueError(f"unsafe archive member path: {name}")
    name = name.rstrip("/")
    if not name or name.startswith("/"):
        raise ValueError(f"unsafe archive member path: {name}")
    parts = name.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"unsafe archive member path: {name}")
    root_abs = os.path.abspath(root)
    target_abs = os.path.abspath(os.path.join(root_abs, *parts))
    if os.path.commonpath([root_abs, target_abs]) != root_abs:
        raise ValueError(f"unsafe archive member path: {name}")


def validate_archive(archive_name, archive_path, root):
    if archive_name.endswith((".zip", ".jar")):
        with zipfile.ZipFile(archive_path) as archive:
            for info in archive.infolist():
                _validate_relative_member(info.filename, root)
                mode = info.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise ValueError(f"unsafe archive symlink member: {info.filename}")
        return

    with tarfile.open(archive_path, "r:*") as archive:
        for member in archive.getmembers():
            _validate_relative_member(member.name, root)
            if member.issym() or member.islnk():
                raise ValueError(f"unsafe archive link member: {member.name}")
            if not (member.isfile() or member.isdir()):
                raise ValueError(f"unsupported archive member type: {member.name}")


def validate_extracted_tree(root):
    root_abs = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root_abs, followlinks=False):
        for name in dirnames + filenames:
            path = os.path.join(dirpath, name)
            if os.path.islink(path):
                raise ValueError(f"unsafe extracted archive symlink: {path}")
            if os.path.commonpath([root_abs, os.path.abspath(path)]) != root_abs:
                raise ValueError(f"unsafe extracted archive path: {path}")
