"""Entry point discovery for Python datasources."""

from typing import List, Tuple, Type


def discover_entry_points(group: str = "sail.datasources") -> List[Tuple[str, Type]]:
    """
    Discover datasources from Python entry points.

    Compatible with Python 3.9+ (handles API differences between 3.9 and 3.10+).

    Args:
        group: Entry point group name to scan

    Returns:
        List of (name, class) tuples for discovered datasources
    """
    from importlib.metadata import entry_points

    try:
        # Python 3.10+: entry_points(group=...) returns iterable
        eps = entry_points(group=group)
    except TypeError:
        # Python 3.9: entry_points() returns SelectableGroups (dict-like)
        all_eps = entry_points()
        eps = getattr(all_eps, "get", lambda g, d: d)(group, [])

    result = []
    for ep in eps:
        try:
            cls = ep.load()
            result.append((ep.name, cls))
        except Exception:
            pass
    return result
