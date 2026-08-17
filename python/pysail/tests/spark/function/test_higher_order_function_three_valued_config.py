import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark

_CONF = "spark.sql.legacy.followThreeValuedLogicInArrayExists"
_QUERY = "SELECT exists(array(1, CAST(NULL AS INT), 3), x -> x > 5) AS r"


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail ignores followThreeValuedLogicInArrayExists; exists is always three-valued",
    strict=True,
)
def test_exists_honors_the_three_valued_logic_config(spark):
    """`exists` obeys ``spark.sql.legacy.followThreeValuedLogicInArrayExists``.

    The build default is ``true`` (three-valued: a NULL element with no matching
    element yields NULL), which Sail matches. Setting the config to ``false``
    restores the two-valued form, where a NULL element counts as non-matching and
    ``exists`` returns ``False`` instead of ``NULL``. Sail hardcodes the
    three-valued form and ignores the config, so under ``false`` it still returns
    ``NULL`` — hence the xfail against Sail.
    """
    previous = spark.conf.get(_CONF, None)
    try:
        spark.conf.set(_CONF, "false")
        assert spark.sql(_QUERY).collect() == [Row(r=False)]
    finally:
        if previous is None:
            spark.conf.unset(_CONF)
        else:
            spark.conf.set(_CONF, previous)
