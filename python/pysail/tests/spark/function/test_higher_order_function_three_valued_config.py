from pyspark.sql import Row

_CONF = "spark.sql.legacy.followThreeValuedLogicInArrayExists"
_QUERY = "SELECT exists(array(1, CAST(NULL AS INT), 3), x -> x > 5) AS r"


def test_exists_honors_the_three_valued_logic_config(spark):
    """`exists` obeys ``spark.sql.legacy.followThreeValuedLogicInArrayExists``.

    The build default is ``true`` (three-valued: a NULL element with no matching
    element yields NULL). Setting the config to ``false`` restores the two-valued
    form, where a NULL element counts as non-matching and ``exists`` returns
    ``False`` instead of ``NULL``.
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
