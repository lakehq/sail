from pyspark.sql import functions as F  # noqa: N812


def test_lambda_expression_names_match_spark(spark):
    frame = spark.sql("SELECT array(1, 2, 3) AS a")

    assert frame.select(F.exists("a", lambda x: x < 0)).columns == [
        "exists(a, lambdafunction((namedlambdavariable() < 0), namedlambdavariable()))"
    ]
    assert frame.select(F.array_sort("a", lambda x, y: y - x)).columns == [
        "array_sort(a, lambdafunction((namedlambdavariable() - namedlambdavariable()), "
        "namedlambdavariable(), namedlambdavariable()))"
    ]
    assert frame.select(F.aggregate("a", F.lit(0), lambda acc, x: acc + x)).columns == [
        "aggregate(a, 0, lambdafunction((namedlambdavariable() + namedlambdavariable()), "
        "namedlambdavariable(), namedlambdavariable()), "
        "lambdafunction(namedlambdavariable(), namedlambdavariable()))"
    ]
    assert frame.select(F.transform("a", lambda x: F.transform("a", lambda y: x + y))).columns == [
        "transform(a, lambdafunction(transform(a, lambdafunction((namedlambdavariable() + "
        "namedlambdavariable()), namedlambdavariable())), namedlambdavariable()))"
    ]
