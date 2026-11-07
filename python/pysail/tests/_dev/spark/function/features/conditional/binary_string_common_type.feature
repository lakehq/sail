Feature: BINARY with STRING values has the BINARY common type

  Like Spark in ANSI mode, the common type of BINARY and STRING values is BINARY.

  Background:
    Given config spark.sql.ansi.enabled = true

  Rule: Conditional functions

    Scenario Outline: <function> of BINARY and STRING values is BINARY
      When query
        """
        SELECT id, hex(<expression>) AS result, typeof(<expression>) AS result_type
        FROM VALUES
          (0, X'80', 'x'),
          (1, CAST(NULL AS BINARY), 'yz'),
          (2, X'41', CAST(NULL AS STRING)),
          (3, CAST(NULL AS BINARY), CAST(NULL AS STRING))
        AS t(id, b, s)
        ORDER BY id
        """
      Then query result ordered
        | id | result   | result_type |
        | 0  | <first>  | binary      |
        | 1  | 797A     | binary      |
        | 2  | 41       | binary      |
        | 3  | NULL     | binary      |

      Examples:
        | function              | expression            | first |
        | coalesce              | coalesce(b, s)        | 80    |
        | coalesce (string 1st) | coalesce(s, b)        | 78    |
        | nvl                   | nvl(b, s)             | 80    |
        | ifnull                | ifnull(s, b)          | 78    |
        | coalesce (3 args)     | coalesce(NULL, b, s)  | 80    |

    Scenario: coalesce of a BINARY column and a STRING literal is BINARY
      When query
        """
        SELECT
          id,
          hex(coalesce(b, 'abc')) AS c0,
          hex(coalesce(X'00', s)) AS c1,
          hex(coalesce(b, s)) AS c2
        FROM VALUES
          (0, X'80', 'café'),
          (1, CAST(NULL AS BINARY), 'café')
        AS t(id, b, s)
        ORDER BY id
        """
      Then query result ordered
        | id | c0     | c1 | c2         |
        | 0  | 80     | 00 | 80         |
        | 1  | 616263 | 00 | 636166C3A9 |

    Scenario: coalesce of nested BINARY and STRING values is nested BINARY
      When query
        """
        SELECT
          id,
          typeof(coalesce(array(b), array(s))) AS a_type,
          typeof(coalesce(named_struct('f', b), named_struct('f', s))) AS s_type,
          typeof(coalesce(map('k', b), map('k', s))) AS m_type
        FROM VALUES (0, X'80', 'x') AS t(id, b, s)
        """
      Then query result
        | id | a_type        | s_type           | m_type             |
        | 0  | array<binary> | struct<f:binary> | map<string,binary> |

  Rule: Other types have no common type with BINARY

    Scenario Outline: coalesce of BINARY and <case> is rejected
      When query
        """
        SELECT coalesce(b, <other>) AS result FROM VALUES (X'80') AS t(b)
        """
      Then query error .

      Examples:
        | case    | other                |
        | INT     | 1                    |
        | BOOLEAN | true                 |
        | DATE    | DATE'2020-01-01'     |
        | STRUCT  | named_struct('a', 1) |

  Rule: Spark rejects BINARY with STRING values when ANSI mode is disabled

    Scenario Outline: <function> of BINARY and STRING values without ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expression> AS result FROM VALUES (X'80', 'x') AS t(b, s)
        """
      Then query error .

      Examples:
        | function | expression     |
        | coalesce | coalesce(b, s) |
        | nvl      | nvl(s, b)      |
