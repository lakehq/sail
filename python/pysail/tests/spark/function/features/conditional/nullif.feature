Feature: nullif output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(2, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullif stays nullable
      When query
        """
        SELECT nullif(c, 2) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Temporal argument evaluation

    # Spark 3.5 evaluates the first argument twice; Spark 4.0+ binds it once.
    @spark-4.0
    Scenario Outline: Mixed timestamps evaluate a volatile first argument once
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT count(*) AS retained_equal_values
        FROM range(1000)
        WHERE nullif(
          CAST(timestamp_micros(CAST(rand(1) * 2 AS BIGINT)) AS <left_type>),
          CAST(timestamp_micros(0) AS <right_type>)
        ) = CAST(timestamp_micros(0) AS <left_type>)
        """
      Then query result
        | retained_equal_values |
        | 0                     |
      When query
        """
        SELECT nullif(
          CASE WHEN id = 0 THEN CAST(NULL AS <left_type>)
               ELSE CAST(timestamp_micros(CAST(rand(1) * 2 AS BIGINT)) AS <left_type>) END,
          CAST(timestamp_micros(0) AS <right_type>)
        ) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |
      Then query schema
        """
        root
         |-- result: <schema_type> (nullable = true)
        """

      Examples:
        | left_type     | right_type    | schema_type   |
        | TIMESTAMP     | TIMESTAMP_NTZ | timestamp     |

    # Sail still widens the NTZ-first result to LTZ instead of preserving its type.
    @spark-4.0
    @sail-bug
    Scenario Outline: Mixed timestamps preserve the NTZ first argument type
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT count(*) AS retained_equal_values
        FROM range(1000)
        WHERE nullif(
          CAST(timestamp_micros(CAST(rand(1) * 2 AS BIGINT)) AS <left_type>),
          CAST(timestamp_micros(0) AS <right_type>)
        ) = CAST(timestamp_micros(0) AS <left_type>)
        """
      Then query result
        | retained_equal_values |
        | 0                     |
      When query
        """
        SELECT nullif(
          CASE WHEN id = 0 THEN CAST(NULL AS <left_type>)
               ELSE CAST(timestamp_micros(CAST(rand(1) * 2 AS BIGINT)) AS <left_type>) END,
          CAST(timestamp_micros(0) AS <right_type>)
        ) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |
      Then query schema
        """
        root
         |-- result: <schema_type> (nullable = true)
        """

      Examples:
        | left_type     | right_type    | schema_type   |
        | TIMESTAMP_NTZ | TIMESTAMP     | timestamp_ntz |
