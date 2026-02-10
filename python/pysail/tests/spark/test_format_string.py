import re

import pandas as pd
import pytest
from pyspark.errors.exceptions.connect import AnalysisException


def test_format_string_basic(spark):
    actual = spark.sql("SELECT format_string('Hello World %s', 'DataFusion') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hello World DataFusion"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_multiple_args(spark):
    actual = spark.sql("SELECT format_string('Hello World %d %s', 100, 'days') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hello World 100 days"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_no_args(spark):
    actual = spark.sql("SELECT format_string('Hello World') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hello World"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_literal_percent(spark):
    actual = spark.sql("SELECT format_string('Percent: %%') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Percent: %"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_decimal_integer(spark):
    actual = spark.sql("SELECT format_string('Value: %d', 42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Value: 42"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_hex_lowercase(spark):
    actual = spark.sql("SELECT format_string('Hex: %x', 255) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hex: ff"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_hex_uppercase(spark):
    actual = spark.sql("SELECT format_string('Hex: %X', 255) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hex: FF"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_octal(spark):
    actual = spark.sql("SELECT format_string('Octal: %o', 64) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Octal: 100"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_integer_width_padding(spark):
    actual = spark.sql("SELECT format_string('Padded: %5d', 42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Padded:    42"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_integer_zero_padding(spark):
    actual = spark.sql("SELECT format_string('Zero padded: %05d', 42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Zero padded: 00042"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_integer_left_aligned(spark):
    actual = spark.sql("SELECT format_string('Left: %-5d|', 42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Left: 42   |"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_integer_force_sign(spark):
    actual = spark.sql("SELECT format_string('Signed: %+d', 42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Signed: +42"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_negative_integer(spark):
    actual = spark.sql("SELECT format_string('Negative: %d', -42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Negative: -42"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_grouping_separator(spark):
    actual = spark.sql("SELECT format_string('Grouped: %,d', 1234567) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Grouped: 1,234,567"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_basic(spark):
    actual = spark.sql("SELECT format_string('Float: %f', 3.14159) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Float: 3.141590"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_precision(spark):
    actual = spark.sql("SELECT format_string('Precision: %.2f', 3.14159) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Precision: 3.14"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_scientific_lowercase(spark):
    actual = spark.sql("SELECT format_string('Scientific: %e', 1234.5) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Scientific: 1.234500e+03"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_scientific_uppercase(spark):
    actual = spark.sql("SELECT format_string('Scientific: %E', 1234.5) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Scientific: 1.234500E+03"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_compact_lowercase(spark):
    actual = spark.sql("SELECT format_string('Compact: %g', 1234.5) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Compact: 1234.5"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_width_precision(spark):
    actual = spark.sql("SELECT format_string('Formatted: %10.2f', 3.14159) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Formatted:       3.14"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_zero_precision(spark):
    actual = spark.sql("SELECT format_string('Precision 0: %.0f', 3.14) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Precision 0: 3"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_boolean_lowercase(spark):
    actual = spark.sql("SELECT format_string('Bool: %b', true) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Bool: true"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_boolean_uppercase(spark):
    actual = spark.sql("SELECT format_string('Bool: %B', false) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Bool: FALSE"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_string_width(spark):
    actual = spark.sql("SELECT format_string('Padded: %10s|', 'test') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Padded:       test|"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_string_left_aligned(spark):
    actual = spark.sql("SELECT format_string('Left: %-10s|', 'test') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Left: test      |"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_string_precision(spark):
    actual = spark.sql("SELECT format_string('Truncated: %.3s', 'DataFusion') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Truncated: Dat"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_string_uppercase(spark):
    actual = spark.sql("SELECT format_string('Upper: %S', 'datafusion') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Upper: DATAFUSION"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_char_from_int(spark):
    actual = spark.sql("SELECT format_string('Char: %c', 97) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Char: a"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_char_uppercase(spark):
    actual = spark.sql("SELECT format_string('Char: %C', 97) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Char: A"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_time_hour_24(spark):
    actual = spark.sql("SELECT format_string('Hour: %tH', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hour: 14"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_time_minute(spark):
    actual = spark.sql("SELECT format_string('Minute: %tM', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Minute: 30"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_time_second(spark):
    actual = spark.sql("SELECT format_string('Second: %tS', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Second: 45"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_date_year(spark):
    actual = spark.sql("SELECT format_string('Year: %tY', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Year: 2023"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_date_month(spark):
    actual = spark.sql("SELECT format_string('Month: %tm', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Month: 12"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_date_day(spark):
    actual = spark.sql("SELECT format_string('Day: %td', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Day: 25"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_date_iso(spark):
    actual = spark.sql("SELECT format_string('ISO Date: %tF', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["ISO Date: 2023-12-25"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_full_month_name(spark):
    actual = spark.sql("SELECT format_string('Month: %tB', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Month: December"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_full_day_name(spark):
    actual = spark.sql("SELECT format_string('Day: %tA', TIMESTAMP '2023-12-25 14:30:45') AS result").toPandas()
    expected = pd.DataFrame({"result": ["Day: Monday"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_null_format(spark):
    actual = spark.sql("SELECT format_string(NULL, 'test') AS result").toPandas()
    assert pd.isna(actual["result"].iloc[0])


def test_format_string_null_arg_string(spark):
    actual = spark.sql("SELECT format_string('Value: %s', NULL) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Value: null"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_null_arg_string_uppercase(spark):
    actual = spark.sql("SELECT format_string('Upper: %S', NULL) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Upper: NULL"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_positional_args(spark):
    actual = spark.sql("SELECT format_string('%2$s %1$d', 42, 'test') AS result").toPandas()
    expected = pd.DataFrame({"result": ["test 42"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_reuse_positional(spark):
    actual = spark.sql("SELECT format_string('%1$s %1$s', 'repeat') AS result").toPandas()
    expected = pd.DataFrame({"result": ["repeat repeat"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_relative_indexing(spark):
    actual = spark.sql("SELECT format_string('%s %<s %<s', 'repeat') AS result").toPandas()
    expected = pd.DataFrame({"result": ["repeat repeat repeat"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_alternate_hex(spark):
    actual = spark.sql("SELECT format_string('Hex: %#x', 255) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Hex: 0xff"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_alternate_octal(spark):
    actual = spark.sql("SELECT format_string('Octal: %#o', 64) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Octal: 0100"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_parentheses_negative(spark):
    actual = spark.sql("SELECT format_string('Negative: %(d', -42) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Negative: (42)"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_nan(spark):
    actual = spark.sql("SELECT format_string('NaN: %f', CAST('NaN' AS DOUBLE)) AS result").toPandas()
    expected = pd.DataFrame({"result": ["NaN: NaN"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_infinity(spark):
    actual = spark.sql("SELECT format_string('Infinity: %f', CAST('+Inf' AS DOUBLE)) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Infinity: Infinity"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_float_negative_infinity(spark):
    actual = spark.sql("SELECT format_string('Negative Infinity: %f', CAST('-Inf' AS DOUBLE)) AS result").toPandas()
    expected = pd.DataFrame({"result": ["Negative Infinity: -Infinity"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_mixed_specifiers(spark):
    actual = spark.sql(
        "SELECT format_string('String: %s, Integer: %d, Float: %.2f', 'test', 42, 3.14159) AS result"
    ).toPandas()
    expected = pd.DataFrame({"result": ["String: test, Integer: 42, Float: 3.14"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_string_missing_args_error(spark):
    with pytest.raises(
        AnalysisException,
        match="Argument index .* out of bounds",
    ):
        spark.sql("SELECT format_string('Value: %d')").collect()


def test_format_string_too_few_args_error(spark):
    with pytest.raises(
        AnalysisException,
        match="Argument index 2 is out of bounds",
    ):
        spark.sql("SELECT format_string('Values: %d %s', 42)").collect()


def test_format_string_invalid_conversion_error(spark):
    with pytest.raises(
        AnalysisException,
        match=re.escape('Execution("Invalid argument type for integer conversion: Utf8"), format string: "Value: %d"'),
    ):
        spark.sql("SELECT format_string('Value: %d', 'not_a_number')").collect()
