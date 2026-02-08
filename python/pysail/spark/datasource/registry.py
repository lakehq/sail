"""Entry point discovery for Python datasources."""

from typing import List, Tuple, Type


def discover_entry_points(group: str = "sail.datasources") -> List[Tuple[str, Type]]:
    """
    Discover datasources from Python entry points.

    Args:
        group: Entry point group name to scan

    Returns:
        List of (name, class) tuples for discovered datasources
    """
    from importlib.metadata import entry_points

    result = []
    for ep in entry_points(group=group):
        try:
            cls = ep.load()
            result.append((ep.name, cls))
        except Exception:
            pass
    return result
