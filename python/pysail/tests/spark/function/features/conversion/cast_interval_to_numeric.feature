Feature: Casting intervals to numeric types

  Rule: Common nested interval types widen their qualifiers

    Scenario Outline: Array <function> uses the common YEAR TO MONTH unit
      When query
        """
        SELECT CAST(<function>(CASE WHEN id = 0
          THEN CAST(array(INTERVAL '1' YEAR) AS ARRAY<INTERVAL YEAR>)
          ELSE CAST(array(INTERVAL '2' MONTH) AS ARRAY<INTERVAL MONTH>)
        END) AS INT) AS result FROM range(2)
        """
      Then query result
        | result |
        | 12     |
        | 2      |

      Examples:
        | function  |
        | array_min |
        | array_max |

    Scenario: Union widens nested map interval units
      When query
        """
        SELECT CAST(map_entries(m)[0].value AS INT) AS result FROM (
          SELECT map_from_entries(array(struct(1, INTERVAL '1' YEAR))) AS m
          UNION ALL
          SELECT map_from_entries(array(struct(1, INTERVAL '2' MONTH))) AS m
        )
        """
      Then query result
        | result |
        | 12     |
        | 2      |

    Scenario Outline: Nested interval units widen when element nullability differs: <case>
      When query
        """
        SELECT CAST(array_max(IF(id = 0, array_repeat(INTERVAL '1' YEAR, 1),
                                array_repeat(<month>, 1))) AS INT) AS result
        FROM range(2)
        """
      Then query result
        | result   |
        | 12       |
        | <result> |

      Examples:
        | case           | month                                          | result |
        | null month     | CAST(NULL AS INTERVAL MONTH)                    | NULL   |
        | nullable month | CASE WHEN id = 1 THEN INTERVAL '2' MONTH END     | 2      |

    Scenario: Union metadata coercion preserves the column count check
      When query
        """
        SELECT map_from_entries(array(struct(1, INTERVAL '1' YEAR))) AS m, 1 AS extra
        UNION ALL
        SELECT map_from_entries(array(struct(1, INTERVAL '2' MONTH))) AS m
        """
      Then query error (?i)columns

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


    Scenario Outline: Day-time integer casts use their trailing unit for <case>
      When query
        """
        SELECT <cast>(value AS <type>) AS result
        FROM VALUES (<value>) AS t(value)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | cast     | value                           | type     | result |
        | days             | CAST     | INTERVAL '2' DAY                | INT      | 2      |
        | hours            | CAST     | INTERVAL '2' HOUR               | BIGINT   | 2      |
        | minutes          | CAST     | INTERVAL '2' MINUTE             | SMALLINT | 2      |
        | seconds          | CAST     | INTERVAL '2.9' SECOND           | TINYINT  | 2      |
        | negative seconds | CAST     | INTERVAL '-2.9' SECOND          | BIGINT   | -2     |
        | day to hour      | CAST     | INTERVAL '2 03' DAY TO HOUR     | BIGINT   | 51     |
        | hour to minute   | CAST     | INTERVAL '2:03' HOUR TO MINUTE  | BIGINT   | 123    |
        | null             | CAST     | CAST(NULL AS INTERVAL DAY)     | INT      | NULL   |
        | try days         | TRY_CAST | INTERVAL '2' DAY                | INT      | 2      |
        | try overflow     | TRY_CAST | INTERVAL '128' DAY              | TINYINT  | NULL   |

    Scenario Outline: Day-time integer overflow raises with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(INTERVAL '128' DAY AS TINYINT) AS result
        """
      Then query error (?i)overflow|out of range|out of bounds|can't cast value 128 to type Int8

      Examples:
        | ansi  |
        | true  |
        | false |
