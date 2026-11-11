Feature: Interval strings use the target qualifiers in casts and CSV

  Scenario Outline: Qualified interval strings parse in casts and CSV: <case>
    When query
      """
      SELECT CAST(value AS INTERVAL <qualifier>) <=> expected AS cast_matches,
             TRY_CAST(value AS INTERVAL <qualifier>) <=> expected AS try_matches,
             from_csv(value, 'a INTERVAL <qualifier>').a <=> expected AS csv_matches
      FROM VALUES (<input>, <expected>),
                  (CAST(NULL AS STRING), CAST(NULL AS INTERVAL <qualifier>)) AS t(value, expected)
      """
    Then query result
      | cast_matches | try_matches | csv_matches |
      | true         | true        | true        |
      | true         | true        | true        |
    When query
      """
      SELECT CAST(<input> AS INTERVAL <qualifier>) <=> <expected> AS cast_matches,
             TRY_CAST(<input> AS INTERVAL <qualifier>) <=> <expected> AS try_matches,
             from_csv(<input>, 'a INTERVAL <qualifier>').a <=> <expected> AS csv_matches
      """
    Then query result
      | cast_matches | try_matches | csv_matches |
      | true         | true        | true        |

    Examples:
      | case                         | input                          | qualifier        | expected                                    |
      | years                        | '1'                            | YEAR             | INTERVAL '1' YEAR                           |
      | months                       | '1'                            | MONTH            | INTERVAL '1' MONTH                          |
      | years and months             | '1-2'                          | YEAR TO MONTH    | INTERVAL '1-2' YEAR TO MONTH                |
      | days                         | '1'                            | DAY              | INTERVAL '1' DAY                            |
      | hours                        | '1'                            | HOUR             | INTERVAL '1' HOUR                           |
      | minutes                      | '1'                            | MINUTE           | INTERVAL '1' MINUTE                         |
      | seconds                      | '1'                            | SECOND           | INTERVAL '1' SECOND                         |
      | day to hour                  | '1 02'                         | DAY TO HOUR      | INTERVAL '1 02' DAY TO HOUR                 |
      | day to minute                | '1 02:03'                      | DAY TO MINUTE    | INTERVAL '1 02:03' DAY TO MINUTE            |
      | day to second                | '1 02:03:04.000005'            | DAY TO SECOND    | INTERVAL '1 02:03:04.000005' DAY TO SECOND  |
      | hour to minute               | '25:03'                        | HOUR TO MINUTE   | INTERVAL '25:03' HOUR TO MINUTE             |
      | hour to second               | '25:03:04.000005'              | HOUR TO SECOND   | INTERVAL '25:03:04.000005' HOUR TO SECOND   |
      | minute to second             | '61:04.000005'                 | MINUTE TO SECOND | INTERVAL '61:04.000005' MINUTE TO SECOND    |
      | signed hours with whitespace | '  +2  '                       | HOUR             | INTERVAL '2' HOUR                           |
      | negative year to month       | '-1-2'                         | YEAR TO MONTH    | INTERVAL '-1-2' YEAR TO MONTH               |
      | negative day to second       | '-1 02:03:04.000005'           | DAY TO SECOND    | INTERVAL '-1 02:03:04.000005' DAY TO SECOND |
      | fractional seconds           | '0.000001'                     | SECOND           | INTERVAL '0.000001' SECOND                  |
      | negative fractional seconds  | '-1.000001'                    | SECOND           | INTERVAL '-1.000001' SECOND                 |
      | fraction truncation          | '0.123456789'                  | SECOND           | INTERVAL '0.123456' SECOND                  |
      | single fractional digit      | '1.1'                          | SECOND           | INTERVAL '1.100000' SECOND                  |
      | day fraction truncation      | '1 02:03:04.123456789'         | DAY TO SECOND    | INTERVAL '1 02:03:04.123456' DAY TO SECOND  |
      | hour fraction truncation     | '25:03:04.123456789'           | HOUR TO SECOND   | INTERVAL '25:03:04.123456' HOUR TO SECOND   |
      | minute fraction truncation   | '61:04.123456789'              | MINUTE TO SECOND | INTERVAL '61:04.123456' MINUTE TO SECOND    |
      | explicit year literal        | "INTERVAL '2' YEAR"            | YEAR             | INTERVAL '2' YEAR                           |
      | explicit hour literal        | "INTERVAL '2' HOUR"            | HOUR             | INTERVAL '2' HOUR                           |
      | explicit compound literal    | "INTERVAL '1-2' YEAR TO MONTH" | YEAR TO MONTH    | INTERVAL '1-2' YEAR TO MONTH                |

  Scenario Outline: Invalid qualified interval strings become null: <case>
    When query
      """
      SELECT TRY_CAST(value AS INTERVAL <qualifier>) IS NULL AS try_null,
             from_csv(concat(value, ',7'), 'a INTERVAL <qualifier>, b INT').a IS NULL AS csv_null,
             from_csv(concat(value, ',7'), 'a INTERVAL <qualifier>, b INT').b AS unaffected
      FROM VALUES (<input>) AS t(value)
      """
    Then query result
      | try_null | csv_null | unaffected |
      | true     | true     | 7          |

    Examples:
      | case                             | input                            | qualifier        |
      | missing INTERVAL keyword         | "'1' HOUR"                       | HOUR             |
      | unit without quotes              | '1 hour'                         | HOUR             |
      | unquoted literal                 | 'INTERVAL 1 HOUR'                | HOUR             |
      | missing compound fields          | '1'                              | DAY TO SECOND    |
      | month out of range               | '1-12'                           | YEAR TO MONTH    |
      | hour out of range                | '1 24'                           | DAY TO HOUR      |
      | minute out of range              | '1:60'                           | HOUR TO MINUTE   |
      | second out of range              | '1:60'                           | MINUTE TO SECOND |
      | mismatched literal qualifier     | "INTERVAL '1' DAY"               | HOUR             |
      | invalid value                    | 'not an interval'                | DAY              |
      | overflow                         | '999999999999999999999999999'    | DAY              |
      | trailing decimal point           | '1.'                             | SECOND           |
      | day trailing decimal point       | '1 02:03:04.'                    | DAY TO SECOND    |
      | hour trailing decimal point      | '25:03:04.'                      | HOUR TO SECOND   |
      | minute trailing decimal point    | '61:04.'                         | MINUTE TO SECOND |
      | excess fractional digits         | '0.1234567890'                   | SECOND           |
      | day excess fractional digits     | '1 02:03:04.1234567890'          | DAY TO SECOND    |
      | hour excess fractional digits    | '25:03:04.1234567890'            | HOUR TO SECOND   |
      | minute excess fractional digits  | '61:04.1234567890'               | MINUTE TO SECOND |
      | literal trailing decimal point   | "INTERVAL '1.' SECOND"           | SECOND           |
      | literal excess fractional digits | "INTERVAL '0.1234567890' SECOND" | SECOND           |

  Scenario Outline: Invalid qualified interval strings fail in strict modes: <case>
    When query
      """
      SELECT CAST(<input> AS INTERVAL <qualifier>) AS result
      """
    Then query error (?i)interval|invalid argument|SQL parser
    When query
      """
      SELECT from_csv(<input>, 'a INTERVAL <qualifier>', map('mode', 'FAILFAST')) AS result
      """
    Then query error (?i)interval|malformed records|SQL parser

    Examples:
      | case                             | input                            | qualifier        |
      | missing INTERVAL keyword         | "'1' HOUR"                       | HOUR             |
      | unit without quotes              | '1 hour'                         | HOUR             |
      | unquoted literal                 | 'INTERVAL 1 HOUR'                | HOUR             |
      | missing compound fields          | '1'                              | DAY TO SECOND    |
      | month out of range               | '1-12'                           | YEAR TO MONTH    |
      | hour out of range                | '1 24'                           | DAY TO HOUR      |
      | minute out of range              | '1:60'                           | HOUR TO MINUTE   |
      | second out of range              | '1:60'                           | MINUTE TO SECOND |
      | mismatched literal qualifier     | "INTERVAL '1' DAY"               | HOUR             |
      | invalid value                    | 'not an interval'                | DAY              |
      | overflow                         | '999999999999999999999999999'    | DAY              |
      | trailing decimal point           | '1.'                             | SECOND           |
      | day trailing decimal point       | '1 02:03:04.'                    | DAY TO SECOND    |
      | hour trailing decimal point      | '25:03:04.'                      | HOUR TO SECOND   |
      | minute trailing decimal point    | '61:04.'                         | MINUTE TO SECOND |
      | excess fractional digits         | '0.1234567890'                   | SECOND           |
      | day excess fractional digits     | '1 02:03:04.1234567890'          | DAY TO SECOND    |
      | hour excess fractional digits    | '25:03:04.1234567890'            | HOUR TO SECOND   |
      | minute excess fractional digits  | '61:04.1234567890'               | MINUTE TO SECOND |
      | literal trailing decimal point   | "INTERVAL '1.' SECOND"           | SECOND           |
      | literal excess fractional digits | "INTERVAL '0.1234567890' SECOND" | SECOND           |

  Scenario Outline: Non-ANSI interval casts retain strict parsing for <qualifier>
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST('invalid' AS INTERVAL <qualifier>) AS result
      """
    Then query error (?i)interval|invalid argument|SQL parser
    When query
      """
      SELECT TRY_CAST(value AS INTERVAL <qualifier>) IS NULL AS result
      FROM VALUES ('invalid'), (CAST(NULL AS STRING)) AS t(value)
      """
    Then query result
      | result |
      | true   |
      | true   |

    Examples:
      | qualifier      |
      | YEAR TO MONTH  |
      | HOUR TO SECOND |

  Scenario: SQL interval literals still allow a trailing decimal point
    When query
      """
      SELECT INTERVAL '1.' SECOND = INTERVAL '1' SECOND AS matches
      """
    Then query result
      | matches |
      | true    |

  Scenario: Interval casts of the same input keep distinct qualifiers
    When query
      """
      SELECT CAST(value AS INTERVAL HOUR) <=> expected_hour AS hour_matches,
             CAST(value AS INTERVAL MINUTE) <=> expected_minute AS minute_matches,
             CAST(value AS INTERVAL YEAR) <=> expected_year AS year_matches,
             CAST(value AS INTERVAL MONTH) <=> expected_month AS month_matches
      FROM VALUES ('1', INTERVAL '1' HOUR, INTERVAL '1' MINUTE, INTERVAL '1' YEAR, INTERVAL '1' MONTH),
                  ('-2', INTERVAL '-2' HOUR, INTERVAL '-2' MINUTE, INTERVAL '-2' YEAR, INTERVAL '-2' MONTH),
                  (NULL, NULL, NULL, NULL, NULL)
           AS t(value, expected_hour, expected_minute, expected_year, expected_month)
      """
    Then query result
      | hour_matches | minute_matches | year_matches | month_matches |
      | true         | true           | true         | true          |
      | true         | true           | true         | true          |
      | true         | true           | true         | true          |

  Scenario: Strict interval casts still fail beside try casts of the same input
    When query
      """
      SELECT TRY_CAST(value AS INTERVAL HOUR) AS permissive,
             CAST(value AS INTERVAL HOUR) AS strict
      FROM (SELECT concat('invalid', CAST(id AS STRING)) AS value FROM range(2))
      """
    Then query error (?i)interval|invalid argument|SQL parser

  Scenario: CSV interval columns keep distinct qualifiers across rows
    When query
      """
      SELECT parsed.h <=> expected_hour AS hour_matches,
             parsed.n AS unaffected,
             parsed.m <=> expected_month AS month_matches
      FROM (
        SELECT from_csv(value, 'h INTERVAL HOUR, n INT, m INTERVAL MONTH') AS parsed,
               expected_hour, expected_month
        FROM VALUES ('1,7,2', INTERVAL '1' HOUR, INTERVAL '2' MONTH),
                    ('-2,8,3', INTERVAL '-2' HOUR, INTERVAL '3' MONTH),
                    ('invalid,9,4', NULL, INTERVAL '4' MONTH),
                    (NULL, NULL, NULL)
             AS t(value, expected_hour, expected_month)
      )
      """
    Then query result
      | hour_matches | unaffected | month_matches |
      | true         | 7          | true          |
      | true         | 8          | true          |
      | true         | 9          | true          |
      | true         | NULL       | true          |
