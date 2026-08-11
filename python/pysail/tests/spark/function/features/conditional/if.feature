Feature: if output schema

  Rule: Spark-compatible coercion for mixed string and temporal branches

    Scenario: IF coerces date branches to string when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          if(use_override, '2026-03-31', period_end) AS result,
          typeof(if(use_override, '2026-03-31', period_end)) AS result_type
        FROM VALUES
          (1, true, DATE '2026-02-20'),
          (2, false, DATE '2025-12-01'),
          (3, false, CAST(NULL AS DATE))
        AS t(id, use_override, period_end)
        ORDER BY id
        """
      Then query result
        | id | result     | result_type |
        | 1  | 2026-03-31 | string      |
        | 2  | 2025-12-01 | string      |
        | 3  | NULL       | string      |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to if yields the schema Spark declares
      When query
        """
        SELECT if(1 < 2, 'a', 'b') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to if stays nullable
      When query
        """
        SELECT if(c, 'a', 'b') AS result FROM VALUES (1 < 2), (CAST(NULL AS BOOLEAN)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
