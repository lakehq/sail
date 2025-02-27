import datetime

from pyspark.sql.types import Row


def test_date_plus_int(sail):
    assert sail.sql("SELECT DATE'2025-02-26' + 1 AS result;").collect() == [Row(result=datetime.date(2025, 2, 27))]
    assert sail.sql("SELECT 1 + DATE'2025-02-26' AS result;").collect() == [Row(result=datetime.date(2025, 2, 27))]
