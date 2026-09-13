Feature: regexp_instr integer index argument

  Rule: Integer index values do not change the first whole match

    Scenario Outline: Literal integer index <idx>
      When query
        """
        SELECT regexp_instr('abcabc', 'a', <idx>) AS result
        """
      Then query result
        | result |
        | 1      |

      Examples:
        | idx                    |
        | 0                      |
        | 1                      |
        | 2                      |
        | -1                     |
        | 2147483647             |
        | CAST(-2147483648 AS INT) |
        | CAST(2 AS TINYINT)      |
        | CAST(2 AS SMALLINT)     |

    Scenario Outline: Nullable integer indices in ANSI mode <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, regexp_instr(s, p, i) AS result
        FROM VALUES
          (1, 'xabcabc', '(ab)c', 2),
          (2, 'xabcabc', 'z', 0),
          (3, 'xabcabc', 'a', CAST(NULL AS INT)),
          (4, CAST(NULL AS STRING), 'a', -1),
          (5, 'xabcabc', CAST(NULL AS STRING), 2),
          (6, '', '', -1),
          (7, 'éabc', 'a', 2147483647)
        AS t(id, s, p, i)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 2      |
        | 2  | 0      |
        | 3  | NULL   |
        | 4  | NULL   |
        | 5  | NULL   |
        | 6  | 1      |
        | 7  | 2      |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: Null index suppresses invalid pattern matching
      When query
        """
        SELECT regexp_instr('abcabc', '[', CAST(NULL AS INT)) AS result,
               regexp_instr('abcabc', '[', NULL) AS untyped
        """
      Then query result
        | result | untyped |
        | NULL   | NULL    |

    Scenario: Runtime index expressions still raise their errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abcabc', 'a', CAST(concat(CAST(id AS STRING), 'bad') AS INT)) AS result
        FROM range(1)
        """
      Then query error (?i)(cast|parse|convert)

    Scenario: Nonconstant indices remain evaluated without affecting match positions
      When query
        """
        SELECT id, regexp_instr('abcabc', 'a', CAST(id - 2 AS INT)) AS result
        FROM range(5)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 1      |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 1      |
        | 4  | 1      |

    Scenario Outline: Nullable <type> index columns preserve null positions
      When query
        """
        SELECT id, regexp_instr('abcabc', 'a', CAST(i AS <type>)) AS result
        FROM VALUES (0, CAST(NULL AS INT)), (1, -1), (2, 0), (3, 2) AS t(id, i)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | NULL   |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 1      |

      Examples:
        | type     |
        | TINYINT  |
        | SMALLINT |

  Rule: Index coercion to Spark INT remains deferred

    @sail-bug
    Scenario Outline: Coerce <type> index before ignoring its value
      When query
        """
        SELECT regexp_instr('abcabc', 'a', CAST(2 AS <type>)) AS result
        """
      Then query result
        | result |
        | 1      |

      Examples:
        | type   |
        | BIGINT |
        | STRING |

    @sail-bug
    Scenario: ANSI index narrowing rejects integer overflow
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abcabc', 'a', CAST(2147483648 AS BIGINT)) AS result
        """
      Then query error (?i)overflow

  Rule: Existing index expression evaluation gaps remain deferred

    @sail-bug
    Scenario: Null subject suppresses an invalid index cast in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr(CAST(NULL AS STRING), 'a',
                            CAST(concat(CAST(id AS STRING), 'bad') AS INT)) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: Legacy explicit integer overflow wraps before ignoring the index
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT regexp_instr('abcabc', 'a', CAST(id + 2147483648 AS INT)) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | 1      |
