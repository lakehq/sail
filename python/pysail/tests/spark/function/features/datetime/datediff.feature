Feature: datediff output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff('2009-07-31', '2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff(CAST(id AS STRING), '2009-07-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to datediff stays nullable
      When query
        """
        SELECT datediff(c, '2009-07-30') AS result FROM VALUES ('2009-07-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: a CASE, IF or UNION with an INT branch beside a BIGINT one is a BIGINT

    # TODO: a CASE, IF or UNION is typed by its first branch, so an INT branch beside a BIGINT one
    #  reports INT; `datediff` is an INT now and meets it (`TypeCoercion.scala:168`).
    @sail-bug
    Scenario Outline: <expression> is a BIGINT
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | bigint |

      Examples:
        | expression                                                                        |
        | CASE WHEN false THEN datediff(DATE'2024-01-15', DATE'2024-01-01') ELSE 3000000000L END |
        | if(false, regexp_count('aaa', 'a'), 3000000000L)                                  |
