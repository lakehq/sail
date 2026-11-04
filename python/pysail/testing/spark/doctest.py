from __future__ import annotations

import doctest
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from _pytest.doctest import DoctestItem

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

if TYPE_CHECKING:
    from _pytest.mark import MarkDecorator

# Skip Sail-only doctest examples when running against JVM Spark.
SAIL_ONLY = doctest.register_optionflag("SAIL_ONLY")


@dataclass
class DoctestMarker:
    keywords: list[str]
    markers: list[str | MarkDecorator]


DOCTEST_MARKERS = [
    DoctestMarker(
        keywords=["test_python_read.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4,), reason="Python data source requires Spark 4+")],
    ),
    DoctestMarker(
        keywords=["test_python_read_arrow.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4,), reason="Python data source requires Spark 4+")],
    ),
    DoctestMarker(
        keywords=["test_arrow_scalar_udf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4, 1), reason="arrow_udf requires PySpark 4.1+")],
    ),
    DoctestMarker(
        keywords=["test_arrow_agg_udf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4, 1), reason="arrow_udf requires PySpark 4.1+")],
    ),
    DoctestMarker(
        keywords=["test_arrow_grouped_map_udf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4,), reason="applyInArrow requires PySpark 4+")],
    ),
    DoctestMarker(
        keywords=["test_arrow_cogrouped_map_udf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4,), reason="applyInArrow requires PySpark 4+")],
    ),
    DoctestMarker(
        keywords=["test_arrow_udtf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4, 1), reason="arrow_udtf requires PySpark 4.1+")],
    ),
    DoctestMarker(
        keywords=["test_pandas_grouped_map_iter_udf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4, 1), reason="applyInPandas iterator requires PySpark 4.1+")],
    ),
    DoctestMarker(
        keywords=["test_arrow_grouped_map_iter_udf.txt"],
        markers=[pytest.mark.skipif(pyspark_version() < (4, 1), reason="applyInArrow iterator requires PySpark 4.1+")],
    ),
    DoctestMarker(
        keywords=["test_ipython_key_completions.txt"],
        markers=[
            pytest.mark.skipif(
                pyspark_version() < (4,),
                reason="_ipython_key_completions_ is not defined on the PySpark 3.x Connect DataFrame",
            )
        ],
    ),
]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if isinstance(item, DoctestItem):
            for test in DOCTEST_MARKERS:
                if all(k in item.keywords for k in test.keywords):
                    for marker in test.markers:
                        item.add_marker(marker)

    if is_jvm_spark():
        for item in items:
            if isinstance(item, DoctestItem):
                for example in item.dtest.examples:
                    if example.options.get(SAIL_ONLY):
                        example.options[doctest.SKIP] = True
