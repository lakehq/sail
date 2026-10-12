@spark-4
Feature: Numeric VARIANT to TIMESTAMP uses checked epoch seconds

  Scenario Outline: numeric variant timestamp values <name> ANSI <ansi> <mode>
    Given config spark.sql.session.timeZone = UTC
    Given config spark.sql.ansi.enabled = <ansi>
    When query template
      """
      <query>
      """
    Then query result
      | result |
      | <result> |

    Examples:
      | name | ansi | mode | query | result |
      | one | true | CAST | SELECT unix_micros(CAST(parse_json('1') AS TIMESTAMP)) AS result | 1000000 |
      | one | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('1') AS TIMESTAMP)) AS result | 1000000 |
      | minus | true | CAST | SELECT unix_micros(CAST(parse_json('-1') AS TIMESTAMP)) AS result | -1000000 |
      | minus | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-1') AS TIMESTAMP)) AS result | -1000000 |
      | zero | true | CAST | SELECT unix_micros(CAST(parse_json('0') AS TIMESTAMP)) AS result | 0 |
      | zero | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('0') AS TIMESTAMP)) AS result | 0 |
      | max_seconds | true | CAST | SELECT unix_micros(CAST(parse_json('9223372036854') AS TIMESTAMP)) AS result | 9223372036854000000 |
      | max_seconds | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036854') AS TIMESTAMP)) AS result | 9223372036854000000 |
      | min_seconds | true | CAST | SELECT unix_micros(CAST(parse_json('-9223372036854') AS TIMESTAMP)) AS result | -9223372036854000000 |
      | min_seconds | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-9223372036854') AS TIMESTAMP)) AS result | -9223372036854000000 |
      | positive_overflow | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036855') AS TIMESTAMP)) AS result | NULL |
      | negative_overflow | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-9223372036855') AS TIMESTAMP)) AS result | NULL |
      | decimal | true | CAST | SELECT unix_micros(CAST(parse_json('1.23456789') AS TIMESTAMP)) AS result | 1234567 |
      | decimal | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('1.23456789') AS TIMESTAMP)) AS result | 1234567 |
      | negative_decimal | true | CAST | SELECT unix_micros(CAST(parse_json('-1.23456789') AS TIMESTAMP)) AS result | -1234567 |
      | negative_decimal | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-1.23456789') AS TIMESTAMP)) AS result | -1234567 |
      | small_decimal | true | CAST | SELECT unix_micros(CAST(parse_json('0.0000009') AS TIMESTAMP)) AS result | 0 |
      | small_decimal | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('0.0000009') AS TIMESTAMP)) AS result | 0 |
      | decimal_max | true | CAST | SELECT unix_micros(CAST(parse_json('9223372036854.775807') AS TIMESTAMP)) AS result | 9223372036854775807 |
      | decimal_max | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036854.775807') AS TIMESTAMP)) AS result | 9223372036854775807 |
      | decimal_overflow | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036854.775808') AS TIMESTAMP)) AS result | NULL |
      | double | true | CAST | SELECT unix_micros(CAST(CAST(CAST(1.23456789 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | double | true | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST(1.23456789 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | float | true | CAST | SELECT unix_micros(CAST(CAST(CAST(1.23456789 AS FLOAT) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | float | true | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST(1.23456789 AS FLOAT) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | double_large | true | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST(1e20 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | nan | true | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST('NaN' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | infinity | true | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST('Infinity' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | boolean | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('true') AS TIMESTAMP)) AS result | NULL |
      | json_null | true | CAST | SELECT unix_micros(CAST(parse_json('null') AS TIMESTAMP)) AS result | NULL |
      | json_null | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('null') AS TIMESTAMP)) AS result | NULL |
      | sql_null | true | CAST | SELECT unix_micros(CAST(CAST(NULL AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | sql_null | true | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(NULL AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | timestamp_string | true | CAST | SELECT unix_micros(CAST(parse_json('"2024-01-02 03:04:05"') AS TIMESTAMP)) AS result | 1704164645000000 |
      | timestamp_string | true | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('"2024-01-02 03:04:05"') AS TIMESTAMP)) AS result | 1704164645000000 |
      | one | false | CAST | SELECT unix_micros(CAST(parse_json('1') AS TIMESTAMP)) AS result | 1000000 |
      | one | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('1') AS TIMESTAMP)) AS result | 1000000 |
      | minus | false | CAST | SELECT unix_micros(CAST(parse_json('-1') AS TIMESTAMP)) AS result | -1000000 |
      | minus | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-1') AS TIMESTAMP)) AS result | -1000000 |
      | zero | false | CAST | SELECT unix_micros(CAST(parse_json('0') AS TIMESTAMP)) AS result | 0 |
      | zero | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('0') AS TIMESTAMP)) AS result | 0 |
      | max_seconds | false | CAST | SELECT unix_micros(CAST(parse_json('9223372036854') AS TIMESTAMP)) AS result | 9223372036854000000 |
      | max_seconds | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036854') AS TIMESTAMP)) AS result | 9223372036854000000 |
      | min_seconds | false | CAST | SELECT unix_micros(CAST(parse_json('-9223372036854') AS TIMESTAMP)) AS result | -9223372036854000000 |
      | min_seconds | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-9223372036854') AS TIMESTAMP)) AS result | -9223372036854000000 |
      | positive_overflow | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036855') AS TIMESTAMP)) AS result | NULL |
      | negative_overflow | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-9223372036855') AS TIMESTAMP)) AS result | NULL |
      | decimal | false | CAST | SELECT unix_micros(CAST(parse_json('1.23456789') AS TIMESTAMP)) AS result | 1234567 |
      | decimal | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('1.23456789') AS TIMESTAMP)) AS result | 1234567 |
      | negative_decimal | false | CAST | SELECT unix_micros(CAST(parse_json('-1.23456789') AS TIMESTAMP)) AS result | -1234567 |
      | negative_decimal | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('-1.23456789') AS TIMESTAMP)) AS result | -1234567 |
      | small_decimal | false | CAST | SELECT unix_micros(CAST(parse_json('0.0000009') AS TIMESTAMP)) AS result | 0 |
      | small_decimal | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('0.0000009') AS TIMESTAMP)) AS result | 0 |
      | decimal_max | false | CAST | SELECT unix_micros(CAST(parse_json('9223372036854.775807') AS TIMESTAMP)) AS result | 9223372036854775807 |
      | decimal_max | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036854.775807') AS TIMESTAMP)) AS result | 9223372036854775807 |
      | decimal_overflow | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('9223372036854.775808') AS TIMESTAMP)) AS result | NULL |
      | double | false | CAST | SELECT unix_micros(CAST(CAST(CAST(1.23456789 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | double | false | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST(1.23456789 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | float | false | CAST | SELECT unix_micros(CAST(CAST(CAST(1.23456789 AS FLOAT) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | float | false | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST(1.23456789 AS FLOAT) AS VARIANT) AS TIMESTAMP)) AS result | 1234567 |
      | double_large | false | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST(1e20 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | nan | false | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST('NaN' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | infinity | false | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(CAST('Infinity' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | boolean | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('true') AS TIMESTAMP)) AS result | NULL |
      | json_null | false | CAST | SELECT unix_micros(CAST(parse_json('null') AS TIMESTAMP)) AS result | NULL |
      | json_null | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('null') AS TIMESTAMP)) AS result | NULL |
      | sql_null | false | CAST | SELECT unix_micros(CAST(CAST(NULL AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | sql_null | false | TRY_CAST | SELECT unix_micros(TRY_CAST(CAST(NULL AS VARIANT) AS TIMESTAMP)) AS result | NULL |
      | timestamp_string | false | CAST | SELECT unix_micros(CAST(parse_json('"2024-01-02 03:04:05"') AS TIMESTAMP)) AS result | 1704164645000000 |
      | timestamp_string | false | TRY_CAST | SELECT unix_micros(TRY_CAST(parse_json('"2024-01-02 03:04:05"') AS TIMESTAMP)) AS result | 1704164645000000 |

  Scenario Outline: numeric variant timestamp errors <name> ANSI <ansi> <mode>
    Given config spark.sql.session.timeZone = UTC
    Given config spark.sql.ansi.enabled = <ansi>
    When query template
      """
      <query>
      """
    Then query error INVALID_VARIANT_CAST.*cannot be cast into.*TIMESTAMP

    Examples:
      | name | ansi | mode | query |
      | positive_overflow | true | CAST | SELECT unix_micros(CAST(parse_json('9223372036855') AS TIMESTAMP)) AS result |
      | negative_overflow | true | CAST | SELECT unix_micros(CAST(parse_json('-9223372036855') AS TIMESTAMP)) AS result |
      | decimal_overflow | true | CAST | SELECT unix_micros(CAST(parse_json('9223372036854.775808') AS TIMESTAMP)) AS result |
      | double_large | true | CAST | SELECT unix_micros(CAST(CAST(CAST(1e20 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result |
      | nan | true | CAST | SELECT unix_micros(CAST(CAST(CAST('NaN' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result |
      | infinity | true | CAST | SELECT unix_micros(CAST(CAST(CAST('Infinity' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result |
      | boolean | true | CAST | SELECT unix_micros(CAST(parse_json('true') AS TIMESTAMP)) AS result |
      | positive_overflow | false | CAST | SELECT unix_micros(CAST(parse_json('9223372036855') AS TIMESTAMP)) AS result |
      | negative_overflow | false | CAST | SELECT unix_micros(CAST(parse_json('-9223372036855') AS TIMESTAMP)) AS result |
      | decimal_overflow | false | CAST | SELECT unix_micros(CAST(parse_json('9223372036854.775808') AS TIMESTAMP)) AS result |
      | double_large | false | CAST | SELECT unix_micros(CAST(CAST(CAST(1e20 AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result |
      | nan | false | CAST | SELECT unix_micros(CAST(CAST(CAST('NaN' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result |
      | infinity | false | CAST | SELECT unix_micros(CAST(CAST(CAST('Infinity' AS DOUBLE) AS VARIANT) AS TIMESTAMP)) AS result |
      | boolean | false | CAST | SELECT unix_micros(CAST(parse_json('true') AS TIMESTAMP)) AS result |

  Scenario: timestamp variant sibling variant_get_seconds
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT unix_micros(variant_get(parse_json('{"v":1}'), '$.v', 'timestamp')) AS result
      """
    Then query result
      | result |
      | 1000000 |

  Scenario: timestamp variant sibling try_variant_get_seconds
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT unix_micros(try_variant_get(parse_json('{"v":-1.25}'), '$.v', 'timestamp')) AS result
      """
    Then query result
      | result |
      | -1250000 |

  Scenario: timestamp variant sibling variant_get_missing
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT unix_micros(variant_get(parse_json('{}'), '$.v', 'timestamp')) AS result
      """
    Then query result
      | result |
      | NULL |

  Scenario: timestamp variant sibling variant_get_array
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT unix_micros(variant_get(parse_json('{"v":[1.25]}'), '$.v[0]', 'timestamp')) AS result
      """
    Then query result
      | result |
      | 1250000 |

  Scenario: timestamp variant sibling variant_get_overflow
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT variant_get(parse_json('{"v":9223372036855}'), '$.v', 'timestamp') AS result
      """
    Then query error INVALID_VARIANT_CAST.*cannot be cast into.*TIMESTAMP

  Scenario: timestamp variant sibling try_variant_get_overflow
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT try_variant_get(parse_json('{"v":9223372036855}'), '$.v', 'timestamp') AS result
      """
    Then query result
      | result |
      | NULL |

  Scenario: timestamp variant sibling cast_ntz
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(parse_json('1') AS TIMESTAMP_NTZ) AS result
      """
    Then query error INVALID_VARIANT_CAST.*cannot be cast into.*TIMESTAMP

  Scenario: timestamp variant sibling try_cast_ntz
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(parse_json('1') AS TIMESTAMP_NTZ) AS result
      """
    Then query result
      | result |
      | NULL |

  Scenario: timestamp variant sibling variant_get_ntz
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT variant_get(parse_json('1'), '$', 'timestamp_ntz') AS result
      """
    Then query error INVALID_VARIANT_CAST.*cannot be cast into.*TIMESTAMP
