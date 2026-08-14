Feature: if output schema

  Rule: Spark-compatible coercion for mixed string and temporal branches

    Scenario: IF coerces date branches to string and keeps downstream parsing valid when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          if(use_override, '2026-03-31', period_end) AS result,
          to_date(if(use_override, '2026-03-31', period_end)) AS parsed,
          typeof(if(use_override, '2026-03-31', period_end)) AS result_type
        FROM VALUES
          (1, true, DATE '2026-02-20'),
          (2, false, DATE '2025-12-01'),
          (3, false, CAST(NULL AS DATE))
        AS t(id, use_override, period_end)
        ORDER BY id
        """
      Then query result
        | result     | parsed     | result_type |
        | 2026-03-31 | 2026-03-31 | string      |
        | 2025-12-01 | 2025-12-01 | string      |
        | NULL       | NULL       | string      |

    Scenario: IF coerces string branches to date when ANSI is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          if(use_override, '2026-03-31', period_end) AS result,
          to_date(if(use_override, '2026-03-31', period_end)) AS parsed,
          typeof(if(use_override, '2026-03-31', period_end)) AS result_type
        FROM VALUES
          (1, true, DATE '2026-02-20'),
          (2, false, DATE '2025-12-01'),
          (3, false, CAST(NULL AS DATE))
        AS t(id, use_override, period_end)
        ORDER BY id
        """
      Then query result
        | result     | parsed     | result_type |
        | 2026-03-31 | 2026-03-31 | date        |
        | 2025-12-01 | 2025-12-01 | date        |
        | NULL       | NULL       | date        |

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
