Feature: trim and replace implicitly cast atomic arguments to strings

  Rule: Atomic arguments are cast to strings in both ANSI modes

    Scenario Outline: trim functions accept atomic arguments with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT trim(1) AS trim_int,
               ltrim(CAST(12 AS BIGINT)) AS ltrim_bigint,
               rtrim(CAST(1.50 AS DECIMAL(3, 2))) AS rtrim_decimal,
               trim(CAST(1.5 AS DOUBLE)) AS trim_double,
               trim(true) AS trim_boolean,
               trim(DATE'2024-01-02') AS trim_date,
               trim(TIMESTAMP'2024-01-02 03:04:05') AS trim_timestamp,
               trim(TIMESTAMP_NTZ'2024-01-02 03:04:05') AS trim_timestamp_ntz
        """
      Then query result
        | trim_int | ltrim_bigint | rtrim_decimal | trim_double | trim_boolean | trim_date  | trim_timestamp      | trim_timestamp_ntz  |
        | 1        | 12           | 1.50          | 1.5         | true         | 2024-01-02 | 2024-01-02 03:04:05 | 2024-01-02 03:04:05 |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: trim syntax casts the source and the trim string with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT trim(BOTH 1 FROM 121) AS both_value,
               trim(LEADING 1 FROM 112) AS leading_value,
               trim(TRAILING 1 FROM 211) AS trailing_value,
               trim(BOTH '1' FROM 121) AS string_trim,
               btrim(true, 'e') AS btrim_boolean
        """
      Then query result
        | both_value | leading_value | trailing_value | string_trim | btrim_boolean |
        | 2          | 2             | 2              | 2           | tru           |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: replace accepts atomic arguments with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT replace(1, 1, 2) AS replace_int,
               replace(CAST(12.0 AS DOUBLE), '.0', '') AS replace_double,
               replace(DATE'2024-01-02', '-', '') AS replace_date,
               replace(12321, 2) AS replace_two_args,
               replace('a1b', 1, true) AS replace_boolean
        """
      Then query result
        | replace_int | replace_double | replace_date | replace_two_args | replace_boolean |
        | 2           | 12             | 20240102     | 131              | atrueb          |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: binary and interval arguments are cast to strings with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT trim(X'2061') AS trim_binary,
               replace(X'616263', 'b', 'x') AS replace_binary,
               trim(INTERVAL '1' HOUR) AS trim_interval,
               rtrim(INTERVAL '1-2' YEAR TO MONTH) AS rtrim_interval
        """
      Then query result
        | trim_binary | replace_binary | trim_interval      | rtrim_interval               |
        | a           | axc            | INTERVAL '01' HOUR | INTERVAL '1-2' YEAR TO MONTH |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: numeric columns and null rows are cast to strings with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
               trim(n) AS trimmed,
               replace(n, 1, 9) AS replaced,
               typeof(trim(n)) AS result_type
        FROM VALUES (0, 101), (1, CAST(NULL AS INT)) AS t(id, n)
        ORDER BY id
        """
      Then query result ordered
        | id | trimmed | replaced | result_type |
        | 0  | 101     | 909      | string      |
        | 1  | NULL    | NULL     | string      |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: the result of a cast argument is nullable only if the argument is
      When query
        """
        SELECT trim(1) AS a, replace(1, 1, 2) AS b, trim(n) AS c, replace(n, 1, 2) AS d
        FROM VALUES (CAST(NULL AS INT)) AS t(n)
        """
      Then query schema
        """
        root
         |-- a: string (nullable = false)
         |-- b: string (nullable = false)
         |-- c: string (nullable = true)
         |-- d: string (nullable = true)
        """

  Rule: Nested arguments are rejected

    Scenario: trim rejects an array argument
      When query
        """
        SELECT trim(array(1)) AS r
        """
      Then query error (?i)trim

    Scenario: replace rejects a map argument
      When query
        """
        SELECT replace(map(1, 2), 'a', 'b') AS r
        """
      Then query error (?i)replace
