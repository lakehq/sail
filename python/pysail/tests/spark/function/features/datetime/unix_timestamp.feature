Feature: unix_timestamp with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: unix_timestamp — the argument may come from a column

    @function(columnargs)
    Scenario: unix_timestamp with the argument as a literal
      When query
        """
        SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 1460073600 |

    @function(columnargs)
    Scenario: unix_timestamp takes argument 2 from a column
      When query
        """
        SELECT unix_timestamp('2016-04-08', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1460073600 |
        | 1460073600 |

  Rule: Spark parsing, NULL format, and typed input contract

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: Unix timestamp parsing contract uses the one-argument default format
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          unix_timestamp('2024-01-15') AS date_only,
          unix_timestamp('2024-01-15 01:02:03') AS full_timestamp
        """
      Then query result
        | date_only | full_timestamp |
        | NULL      | 1705280523     |
      And query schema
        """
        root
         |-- date_only: long (nullable = true)
         |-- full_timestamp: long (nullable = true)
        """

    Scenario Outline: Unix timestamp parsing contract scalar formats have ANSI <ansi> nullability
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          unix_timestamp('2024-01-15', 'yyyy-MM-dd') AS parsed,
          unix_timestamp('2024-01-15', CAST(NULL AS STRING)) AS null_format
        """
      Then query result
        | parsed     | null_format |
        | 1705276800 | NULL        |
      And query schema
        """
        root
         |-- parsed: long (nullable = <parsed_nullable>)
         |-- null_format: long (nullable = true)
        """

      Examples:
        | ansi  | parsed_nullable |
        | true  | false           |
        | false | true            |

    Scenario: Unix timestamp parsing contract accepts a scalar value and format column
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp('2024-01-15', format) AS result
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING))
        AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1705276800 |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: Unix timestamp parsing contract accepts a value column and scalar format
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp(value, 'yyyy-MM-dd') AS result
        FROM VALUES
          (1, '2024-01-15'),
          (2, CAST(NULL AS STRING))
        AS t(id, value)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1705276800 |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: Unix timestamp parsing contract handles value and format columns row by row with ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp(value, format) AS result
        FROM VALUES
          (1, '2024-01-15', 'yyyy-MM-dd'),
          (2, '15/01/2024', 'dd/MM/yyyy'),
          (3, 'bad-value', 'yyyy-MM-dd'),
          (4, '2024-01-15', CAST(NULL AS STRING)),
          (5, CAST(NULL AS STRING), 'yyyy-MM-dd')
        AS t(id, value, format)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1705276800 |
        | 1705276800 |
        | NULL       |
        | NULL       |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: Unix timestamp parsing contract errors on a mixed value column with ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, unix_timestamp(value, 'yyyy-MM-dd') AS result
        FROM VALUES
          (1, '2024-01-15'),
          (2, 'bad-value')
        AS t(id, value)
        ORDER BY id
        """
      Then query error CANNOT_PARSE_TIMESTAMP

    Scenario Outline: Unix timestamp parsing contract ignores scalar and column formats for <type>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          unix_timestamp(<input>, format) AS column_format,
          unix_timestamp(<input>, CAST(NULL AS STRING)) AS scalar_null_format,
          unix_timestamp(<input>, 'invalid[') AS scalar_invalid_format
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING)),
          (3, 'invalid[')
        AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | column_format | scalar_null_format | scalar_invalid_format |
        | <result>      | <result>            | <result>              |
        | <result>      | <result>            | <result>              |
        | <result>      | <result>            | <result>              |
      And query schema
        """
        root
         |-- column_format: long (nullable = true)
         |-- scalar_null_format: long (nullable = true)
         |-- scalar_invalid_format: long (nullable = false)
        """

      Examples:
        | type          | input                                | result     |
        | DATE          | DATE '2024-01-15'                    | 1705276800 |
        | TIMESTAMP     | TIMESTAMP '2024-01-15 01:02:03'      | 1705280523 |
        | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2024-01-15 01:02:03'  | 1705280523 |

    Scenario Outline: Unix timestamp parsing contract rejects a <case> format for a typed DATE input
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp(
          DATE '2024-01-15',
          <format>
        )
        """
      Then query error (?i)(DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE|expects.*STRING|requires.*STRING|must.*STRING)

      Examples:
        | case       | format             |
        | atomic     | 123                |
        | collection | array('yyyy-MM-dd') |

    Scenario Outline: Unix timestamp parsing contract does not evaluate an ignored format for <case>
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp(
          <input>,
          CAST(raise_error(CAST(id AS STRING)) AS STRING)
        ) AS result
        FROM range(1)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case       | input             | result |
        | typed DATE | DATE '1970-01-01' | 0      |
        | NULL       | NULL              | NULL   |

    Scenario: Unix timestamp parsing contract applies the session zone to typed inputs
      Given config spark.sql.session.timeZone = America/Los_Angeles
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          unix_timestamp(DATE '2024-01-15', CAST(NULL AS STRING)) AS date_null_format,
          unix_timestamp(DATE '2024-01-15', 'invalid[') AS date_invalid_format,
          unix_timestamp(TIMESTAMP '2024-01-15 01:02:03', 'invalid[') AS timestamp_result,
          unix_timestamp(TIMESTAMP_NTZ '2024-01-15 01:02:03', 'invalid[') AS timestamp_ntz_result
        """
      Then query result
        | date_null_format | date_invalid_format | timestamp_result | timestamp_ntz_result |
        | 1705305600       | 1705305600          | 1705309323       | 1705280523           |
      And query schema
        """
        root
         |-- date_null_format: long (nullable = true)
         |-- date_invalid_format: long (nullable = false)
         |-- timestamp_result: long (nullable = false)
         |-- timestamp_ntz_result: long (nullable = false)
        """

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal yields a bigint
      When query
        """
        SELECT unix_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null string column yields a bigint
      When query
        """
        SELECT unix_timestamp(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT unix_timestamp(c, 'yyyy-MM-dd') AS result FROM VALUES ('2024-01-15'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: unix_seconds FLOORS while unix_timestamp TRUNCATES toward zero
    # Spark uses two different divisions for what looks like the same conversion:
    #
    #   unix_timestamp / to_unix_timestamp : `micros / 1000000`   (Java `/`, toward zero)
    #   unix_seconds / unix_millis         : `Math.floorDiv(...)` (toward negative infinity)
    #
    # They therefore disagree by one second for EVERY negative-micros timestamp whose
    # sub-second part is non-zero — and by nothing at all elsewhere. Spark's own doc for
    # unix_seconds says "Truncates higher levels of precision", which is wrong; the code is
    # authoritative. Spark's own test suite never catches this either: it uses
    # `new Timestamp(-1000000)`, an exact second boundary.
    #
    # The pair below is the whole point. A shared helper makes both functions agree, which
    # is exactly the bug: Sail floors nothing and truncates both. Asserting only the
    # unix_timestamp half would look green under either rule.
    # Measured on Spark JVM 4.2.0, session time zone UTC.

    Scenario Outline: unix_timestamp truncates toward zero: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | fn                | input                                     | result      |
        | pre-epoch half second rounds up to 0    | unix_timestamp    | TIMESTAMP '1969-12-31 23:59:59.5'         | 0           |
        | post-epoch half second rounds down to 0 | unix_timestamp    | TIMESTAMP '1970-01-01 00:00:00.5'         | 0           |
        | exact second boundary is unambiguous    | unix_timestamp    | TIMESTAMP '1969-12-31 23:59:59'           | -1          |
        | deep pre-epoch fraction                 | unix_timestamp    | TIMESTAMP '1900-01-01 00:00:00.000001'    | -2208988799 |
        | the twin behaves identically            | to_unix_timestamp | TIMESTAMP '1969-12-31 23:59:59.5'         | 0           |
        | twin, deep pre-epoch fraction           | to_unix_timestamp | TIMESTAMP '1900-01-01 00:00:00.000001'    | -2208988799 |

    @sail-bug
    Scenario Outline: unix_seconds and unix_millis floor instead: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | fn           | input                                  | result         |
        | pre-epoch half second floors to -1    | unix_seconds | TIMESTAMP '1969-12-31 23:59:59.5'      | -1             |
        | deep pre-epoch fraction floors        | unix_seconds | TIMESTAMP '1900-01-01 00:00:00.000001' | -2208988800    |
        | millis floors the same way            | unix_millis  | TIMESTAMP '1900-01-01 00:00:00.000001' | -2208988800000 |

    Scenario Outline: where the two rules agree, so does everything: <case>
      # The guard-rail half: these inputs give the same answer under flooring and under
      # truncation, so they must stay green whichever way the bug above is fixed.
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | fn           | input                             | result           |
        | exact second, unix_seconds  | unix_seconds | TIMESTAMP '1969-12-31 23:59:59'   | -1               |
        | post-epoch, unix_seconds    | unix_seconds | TIMESTAMP '1970-01-01 00:00:00.5' | 0                |
        | micros are the identity     | unix_micros  | TIMESTAMP '1969-12-31 23:59:59.5' | -500000          |

  Rule: unix_seconds / unix_millis / unix_micros accept TIMESTAMP only
    # These three mix in ExpectsInputTypes with inputTypes = Seq(TimestampType) and NO
    # implicit cast, so DATE and TIMESTAMP_NTZ are ANALYSIS errors — even though
    # unix_timestamp accepts both. Sail accepts them, which is the divergence.
    # The unix_timestamp half is asserted alongside because it is what discriminates a
    # genuinely narrower signature from a blanket rejection.

    Scenario Outline: unix_timestamp does accept DATE and TIMESTAMP_NTZ: <case>
      When query
        """
        SELECT unix_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case          | input                                 | result     |
        | DATE          | DATE '2020-01-01'                     | 1577836800 |
        | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2020-01-01 00:00:00'   | 1577836800 |

    @sail-bug
    Scenario Outline: the unix_* trio rejects them: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query error due to data type mismatch

      Examples:
        | case                | fn           | input                               |
        | unix_seconds, DATE  | unix_seconds | DATE '2020-01-01'                   |
        | unix_seconds, NTZ   | unix_seconds | TIMESTAMP_NTZ '2020-01-01 00:00:00' |
        | unix_millis, DATE   | unix_millis  | DATE '2020-01-01'                   |
        | unix_millis, NTZ    | unix_millis  | TIMESTAMP_NTZ '2020-01-01 00:00:00' |
        | unix_micros, DATE   | unix_micros  | DATE '2020-01-01'                   |
        | unix_micros, NTZ    | unix_micros  | TIMESTAMP_NTZ '2020-01-01 00:00:00' |
