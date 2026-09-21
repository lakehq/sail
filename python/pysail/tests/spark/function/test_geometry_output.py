"""Tests for returning GEOMETRY values to the client."""

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

FROM_WKB = "FROM VALUES (X'0101000000000000000000F03F0000000000000040') AS t(w)"

SHAPES = [
    pytest.param("SELECT st_geomfromwkb(w) AS v " + FROM_WKB, id="top-level"),
    pytest.param("SELECT struct(st_geomfromwkb(w)) AS v " + FROM_WKB, id="struct"),
    pytest.param("SELECT array(st_geomfromwkb(w)) AS v " + FROM_WKB, id="array"),
    pytest.param("SELECT named_struct('g', st_geomfromwkb(w)) AS v " + FROM_WKB, id="named_struct"),
]


def _geometry_leaves(value):
    if isinstance(value, list):
        return [leaf for item in value for leaf in _geometry_leaves(item)]
    if hasattr(value, "asDict"):
        return [leaf for item in value for leaf in _geometry_leaves(item)]
    return [value]


@pytest.mark.skipif(pyspark_version() < (4, 2), reason="GEOMETRY requires Spark 4.2+")
@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
@pytest.mark.parametrize("query", SHAPES)
def test_collect_returns_geometry_values(spark, query):
    """Spark sends GEOMETRY to the client as `struct<srid, wkb>` and PySpark decodes it into
    `Geometry` objects, at any nesting depth. Sail sends the plain WKB `binary`, which the client
    rejects as `MALFORMED_GEOMETRY`.
    """
    [row] = spark.sql(query).collect()
    leaves = _geometry_leaves(row["v"])
    assert [type(leaf).__name__ for leaf in leaves] == ["Geometry"]


@pytest.mark.skipif(pyspark_version() < (4, 2), reason="GEOMETRY requires Spark 4.2+")
@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
@pytest.mark.parametrize("query", SHAPES)
def test_to_arrow_encodes_geometry_as_srid_and_wkb(spark, query):
    """`toArrow` exposes Spark's Arrow encoding of GEOMETRY, `struct<srid: int32, wkb: binary>`.
    Sail's `binary` cannot be cast to it, so the client fails.
    """
    table = spark.sql(query).toArrow()
    assert "struct<srid: int32 not null, wkb: binary not null>" in str(table.schema.field("v").type)
