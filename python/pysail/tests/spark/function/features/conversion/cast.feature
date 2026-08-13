Feature: cast output schema

  Rule: Timestamp timezone conversion

    Scenario: casting TIMESTAMP_NTZ to TIMESTAMP resolves the session-zone gap and overlap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT label, unix_micros(CAST(value AS TIMESTAMP)) AS result
        FROM VALUES
          ('gap', TIMESTAMP_NTZ '2021-03-14 02:30:00'),
          ('overlap', TIMESTAMP_NTZ '2021-11-07 01:30:00')
          AS t(label, value)
        ORDER BY label
        """
      Then query result ordered
        | label   | result           |
        | gap     | 1615717800000000 |
        | overlap | 1636273800000000 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to cast yields the schema Spark declares
      When query
        """
        SELECT cast('10' as int) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
