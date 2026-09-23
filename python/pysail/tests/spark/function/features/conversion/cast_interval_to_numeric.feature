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
