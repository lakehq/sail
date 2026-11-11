from pathlib import Path

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    root = Path(__file__).parent
    for item in items:
        if item.path.is_relative_to(root):
            item.add_marker(pytest.mark.spark(cross_version=True))
