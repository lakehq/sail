"""Entry point discovery for Python data sources."""

import logging
from importlib.metadata import entry_points

logger = logging.getLogger(__name__)


def discover_entry_points(group: str) -> list[tuple[str, type]]:
    """
    Discover data sources from Python entry points.

    Args:
        group: Entry point group name to scan

    Returns:
        List of (name, class) tuples for discovered data sources
    """
    result = []
    for ep in entry_points(group=group):
        try:
            cls = ep.load()
            result.append((ep.name, cls))
        except Exception:  # noqa: BLE001
            logger.warning("Failed to load datasource entry point: %s", ep.name, exc_info=True)
    return result
