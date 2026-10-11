Feature: Casting intervals to numeric types

  Rule: Numeric casts discard interval type metadata

    Scenario Outline: <cast> converts month interval literals and columns to BIGINT
      When query
        """
        SELECT <cast>(INTERVAL '13' MONTH AS BIGINT) AS months
        """
      Then query result
        | months |
        | 13     |
      When query
        """
        SELECT <cast>(m AS BIGINT) AS months,
               CAST(<cast>(m AS BIGINT) AS STRING) AS month_text
        FROM VALUES (INTERVAL '13' MONTH), (INTERVAL '-1' MONTH), (NULL) AS t(m)
        """
      Then query result
        | months | month_text |
        | 13     | 13         |
        | -1     | -1         |
        | NULL   | NULL       |
      And query schema
        """
        root
         |-- months: long (nullable = true)
         |-- month_text: string (nullable = true)
        """

      Examples:
        | cast     |
        | CAST     |
        | TRY_CAST |

    Scenario Outline: <cast> preserves parsed years inside an integer cast
      When query
        """
        SELECT CAST(<cast>(CONCAT('INTERVAL ', CHR(39), id, CHR(39), ' YEAR')
                          AS INTERVAL YEAR) AS BIGINT) AS years
        FROM range(1, 3)
        """
      Then query result
        | years |
        | 1     |
        | 2     |

      Examples:
        | cast     |
        | CAST     |
        | TRY_CAST |

    Scenario: Integer casts use the trailing interval field
      When query
        """
        SELECT CAST(INTERVAL '2' YEAR AS BIGINT) AS years,
               CAST(INTERVAL '-1-1' YEAR TO MONTH AS BIGINT) AS months,
               CAST(INTERVAL '2147483647' MONTH AS INT) AS max_months,
               CAST(INTERVAL '-2147483647' MONTH - INTERVAL '1' MONTH AS INT) AS min_months,
               TRY_CAST(INTERVAL '128' MONTH AS TINYINT) AS overflow
        """
      Then query result
        | years | months | max_months | min_months  | overflow |
        | 2     | -13    | 2147483647 | -2147483648 | NULL     |

  Rule: Day-time intervals cast to DECIMAL keep the microseconds of the SECOND field

    # IntervalUtils.dayTimeIntervalToDecimal (IntervalUtils.scala:930-937): an interval whose
    # end field is SECOND is `Decimal(micros, 20, 6)`; DAY, HOUR and MINUTE divide by the unit.
    Scenario Outline: Interval to DECIMAL: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<interval> AS DECIMAL(<precision>,<scale>)) AS result
        """
      Then query result
        | result   |
        | <result> |
      And query schema
        """
        root
         |-- result: decimal(<precision>,<scale>) (nullable = true)
        """

      Examples:
        | ansi  | case                       | interval                                | precision | scale | result    |
        | true  | SECOND keeps the fraction  | INTERVAL '1.5' SECOND                   | 10        | 2     | 1.50      |
        | true  | negative SECOND            | INTERVAL '-1.5' SECOND                  | 10        | 2     | -1.50     |
        | true  | all six microsecond digits | INTERVAL '1.999999' SECOND              | 10        | 6     | 1.999999  |
        | true  | rounds half up to scale 0  | INTERVAL '1.999999' SECOND              | 10        | 0     | 2         |
        | true  | rounds half up to scale 1  | INTERVAL '1.999999' SECOND              | 10        | 1     | 2.0       |
        | true  | DAY TO SECOND              | INTERVAL '1 02:03:04.5' DAY TO SECOND   | 20        | 3     | 93784.500 |
        | true  | HOUR TO SECOND             | INTERVAL '2:30:15.25' HOUR TO SECOND    | 10        | 3     | 9015.250  |
        | true  | MINUTE TO SECOND           | INTERVAL '1:30.5' MINUTE TO SECOND      | 10        | 2     | 90.50     |
        | true  | MINUTE                     | INTERVAL '90' MINUTE                    | 10        | 2     | 90.00     |
        | true  | HOUR                       | INTERVAL '2' HOUR                       | 10        | 2     | 2.00      |
        | true  | DAY                        | INTERVAL '2' DAY                        | 10        | 2     | 2.00      |
        | true  | DAY TO HOUR                | INTERVAL '2 03' DAY TO HOUR             | 10        | 2     | 51.00     |
        | true  | DAY TO MINUTE              | INTERVAL '2 03:04' DAY TO MINUTE        | 10        | 2     | 3064.00   |
        | false | SECOND keeps the fraction  | INTERVAL '1.5' SECOND                   | 10        | 2     | 1.50      |
        | false | negative SECOND            | INTERVAL '-1.5' SECOND                  | 10        | 2     | -1.50     |
        | false | all six microsecond digits | INTERVAL '1.999999' SECOND              | 10        | 6     | 1.999999  |
        | false | rounds half up to scale 0  | INTERVAL '1.999999' SECOND              | 10        | 0     | 2         |
        | false | rounds half up to scale 1  | INTERVAL '1.999999' SECOND              | 10        | 1     | 2.0       |
        | false | DAY TO SECOND              | INTERVAL '1 02:03:04.5' DAY TO SECOND   | 20        | 3     | 93784.500 |
        | false | HOUR TO SECOND             | INTERVAL '2:30:15.25' HOUR TO SECOND    | 10        | 3     | 9015.250  |
        | false | MINUTE TO SECOND           | INTERVAL '1:30.5' MINUTE TO SECOND      | 10        | 2     | 90.50     |
        | false | MINUTE                     | INTERVAL '90' MINUTE                    | 10        | 2     | 90.00     |
        | false | HOUR                       | INTERVAL '2' HOUR                       | 10        | 2     | 2.00      |
        | false | DAY                        | INTERVAL '2' DAY                        | 10        | 2     | 2.00      |
        | false | DAY TO HOUR                | INTERVAL '2 03' DAY TO HOUR             | 10        | 2     | 51.00     |
        | false | DAY TO MINUTE              | INTERVAL '2 03:04' DAY TO MINUTE        | 10        | 2     | 3064.00   |

    Scenario Outline: Interval column to DECIMAL keeps the fraction and NULL with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(i AS DECIMAL(10,2)) AS result
        FROM VALUES (INTERVAL '1.5' SECOND), (INTERVAL '-0.25' SECOND), (CAST(NULL AS INTERVAL SECOND)) AS t(i)
        """
      Then query result
        | result |
        | 1.50   |
        | -0.25  |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

    # `main` raised Arrow's "too large to store in a Decimal128" instead of this class.
    Scenario Outline: Interval to DECIMAL that does not fit raises with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(INTERVAL '99999999999' SECOND AS DECIMAL(5,0)) AS result
        """
      Then query error \[NUMERIC_VALUE_OUT_OF_RANGE\.WITH_SUGGESTION\]

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: TRY_CAST of an interval that does not fit the DECIMAL is NULL
      When query
        """
        SELECT TRY_CAST(INTERVAL '99999999999' SECOND AS DECIMAL(5,0)) AS result
        """
      Then query result
        | result |
        | NULL   |

  # Cast.scala:1141-1146 routes DECIMAL to an interval through IntervalUtils.decimalToYearMonthInterval
  # and decimalToDayTimeInterval, which round HALF_UP at the interval's unit.
  Rule: DECIMAL casts to an interval rounding half up

    Scenario Outline: ANSI <ansi>, DECIMAL <value> to an interval, read back as <back>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(CAST(CAST(<value> AS DECIMAL(<p>,<s>)) AS INTERVAL <interval>) AS <back>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ansi  | value     | p  | s | interval | back          | result   |
        | true  | 2.5       | 3  | 1 | MONTH    | INT           | 3        |
        | true  | -2.5      | 3  | 1 | MONTH    | INT           | -3       |
        | true  | 0.0000005 | 10 | 7 | SECOND   | DECIMAL(10,6) | 0.000001 |
        | false | 2.5       | 3  | 1 | MONTH    | INT           | 3        |
        | false | -2.5      | 3  | 1 | MONTH    | INT           | -3       |
        | false | 0.0000005 | 10 | 7 | SECOND   | DECIMAL(10,6) | 0.000001 |
