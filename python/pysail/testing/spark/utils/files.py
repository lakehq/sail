import os


def get_data_files(path: str, extension: str = ".parquet") -> list[str]:
    """Recursively find all *data* files under the given path.

    Notes:
    - Delta Lake tables store transaction logs under `_delta_log/`, including
      `*.checkpoint.parquet` files. Those are *not* table data files and should
      not be counted as such in tests.
    """

    data_files: list[str] = []
    for root, dirs, files in os.walk(path):
        # Never descend into Delta transaction logs (contains checkpoint parquet files).
        dirs[:] = [d for d in dirs if d != "_delta_log"]

        for file in files:
            if not file.endswith(extension):
                continue
            # Extra safety: even if logs were moved elsewhere, don't treat checkpoints as data.
            if ".checkpoint." in file:
                continue
            data_files.append(os.path.relpath(os.path.join(root, file), path))

    return sorted(data_files)


def get_data_directory_size(path: str, extension: str = ".parquet") -> int:
    return sum(os.path.getsize(os.path.join(str(path), f)) for f in os.listdir(path) if f.endswith(extension))


def get_partition_structure(path: str) -> set[str]:
    """Get partition structure."""
    partitions = set()

    def _collect_partitions(current_path: str, relative_path: str = ""):
        """Recursively collect partition paths"""
        try:
            for item in os.listdir(current_path):
                item_path = os.path.join(current_path, item)
                if os.path.isdir(item_path) and "=" in item:
                    new_relative = os.path.join(relative_path, item) if relative_path else item

                    has_sub_partitions = any(
                        os.path.isdir(os.path.join(item_path, sub_item)) and "=" in sub_item
                        for sub_item in os.listdir(item_path)
                    )

                    if has_sub_partitions:
                        _collect_partitions(item_path, new_relative)
                    else:
                        partitions.add(new_relative)
        except (OSError, PermissionError):
            pass

    _collect_partitions(str(path))
    return partitions


def assert_file_count_in_partitions(path: str, expected_files_per_partition: int = 1):
    """Assert file count in partitions."""

    partitions = get_partition_structure(path)

    for partition in partitions:
        partition_path = os.path.join(str(path), partition)
        parquet_files = [f for f in os.listdir(partition_path) if f.endswith(".parquet")]
        assert len(parquet_files) == expected_files_per_partition, (
            f"Expected {expected_files_per_partition} parquet file(s) in {partition}, got {len(parquet_files)}"
        )


def assert_file_lifecycle(files_before: set[str], files_after: set[str], operation: str):
    """Assert file lifecycle changes."""
    if operation == "append":
        assert len(files_after) > len(files_before), "Append should increase file count"
        assert not files_after.issubset(files_before), "Append should add new files"
        assert files_before.issubset(files_after), "Append should preserve existing files"
    else:
        msg = f"Unknown operation: {operation}. Only 'append' is supported for file lifecycle assertions."
        raise ValueError(msg)
