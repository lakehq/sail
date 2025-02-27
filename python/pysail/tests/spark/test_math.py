import datetime

from pyspark.sql.types import Row
import pytest

def test_date_plus_int(sail):
    assert sail.sql("SELECT DATE'2025-02-26' + 1 AS result;").collect() == [Row(result=datetime.date(2025, 2, 27))]
    assert sail.sql("SELECT 1 + DATE'2025-02-26' AS result;").collect() == [Row(result=datetime.date(2025, 2, 27))]

def test_date_minus_int(sail):
    assert sail.sql("SELECT DATE'2025-02-26' - 1 AS result;").collect() == [Row(result=datetime.date(2025, 2, 25))]
    with pytest.raises(Exception, match="Cannot coerce arithmetic expression Int32 - Date32 to valid types"):
        sail.sql("SELECT 1 - DATE'2025-02-26' AS result;").collect()
