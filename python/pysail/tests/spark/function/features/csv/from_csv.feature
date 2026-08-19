Feature: from_csv column display name matches Spark

  Spark renders `from_csv` as a UnaryExpression: only the input column
  appears in the column display name, and the schema and options arguments
  are dropped. The same convention applies to `any_value`, `first_value`,
  and `last_value`, which share Sail's unary display-name match arm.

    Scenario Outline: from_csv <case>
      When query
        """
        SELECT from_csv(value, <args>)
        FROM VALUES ('1,2,3') AS t(value)
        """
      Then query result
        | from_csv(value) |
        | {1, 2, 3}       |

      Examples:
        | case                                    | args                                   |
        | with a schema string                    | 'a INT, b INT, c INT'                  |
        | with a schema string and an options map | 'a INT, b INT, c INT', map('sep', ',') |

    Scenario: from_csv with schema_of_csv as the schema argument
      When query
        """
        SELECT from_csv(value, schema_of_csv('1,abc')) AS result
        FROM VALUES ('42,hello') AS t(value)
        """
      Then query result
        | result      |
        | {42, hello} |

    Scenario: from_csv with schema_of_csv handles multiple rows
      When query
        """
        SELECT from_csv(value, schema_of_csv('0,x')) AS result
        FROM VALUES ('1,a'), ('2,b'), ('3,c') AS t(value)
        ORDER BY result._c0
        """
      Then query result ordered
        | result |
        | {1, a} |
        | {2, b} |
        | {3, c} |

    Scenario: from_csv with schema_of_csv options as the schema argument
      When query
        """
        SELECT from_csv(value, schema_of_csv('1|abc', map('sep', '|')), map('sep', '|')) AS result
        FROM VALUES ('42|hello') AS t(value)
        """
      Then query result
        | result      |
        | {42, hello} |

    Scenario: from_csv with schema_of_csv delimiter option as the schema argument
      When query
        """
        SELECT from_csv(value, schema_of_csv('1|abc', map('delimiter', '|')), map('delimiter', '|')) AS result
        FROM VALUES ('42|hello') AS t(value)
        """
      Then query result
        | result      |
        | {42, hello} |

    Scenario Outline: <fn> drops the ignoreNulls argument from its display name
      When query
        """
        SELECT <fn>(x, true)
        FROM VALUES (1) AS t(x)
        """
      Then query result
        | <fn>(x) |
        | 1       |

      Examples:
        | fn          |
        | any_value   |
        | first_value |
        | last_value  |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null csv literal yields a struct
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT') AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- a: integer (nullable = true)
         |    |-- b: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null csv column yields a struct
      When query
        """
        SELECT from_csv(CAST(id AS STRING), 'a INT') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- a: integer (nullable = true)
        """

    Scenario: a nullable csv column stays nullable
      When query
        """
        SELECT from_csv(c, 'a INT') AS result FROM VALUES ('1'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = true)
         |    |-- a: integer (nullable = true)
        """

  Rule: Result values (migrated from test_from_csv.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT from_csv(<args>) AS result, typeof(from_csv(<args>)) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | case                         | args                                                                                                                                  | result                          | type                                            |
        | from_csv doctest #1 (result) | '1, 0.8', 'a INT, b DOUBLE'                                                                                                           | {1, 0.8}                        | struct<a:int,b:double>                          |
        | from_csv doctest #2 (result) | '2015-08-26 00:00:00', 'time TIMESTAMP'                                                                                               | {2015-08-26 00:00:00}           | struct<time:timestamp>                          |
        | from_csv doctest #3 (result) | '42;3.14;2024-12-01 10:00:00', 'id INT, value DOUBLE, timestamp TIMESTAMP', map('sep', ';')                                           | {42, 3.14, 2024-12-01 10:00:00} | struct<id:int,value:double,timestamp:timestamp> |
        | from_csv doctest #4 (result) | '42;3.14;2024/12/01 10:00:00', 'id INT, value DOUBLE, timestamp TIMESTAMP', map('sep', ';', 'timestampFormat', 'yyyy/MM/dd HH:mm:ss') | {42, 3.14, 2024-12-01 10:00:00} | struct<id:int,value:double,timestamp:timestamp> |
        | from_csv doctest #5 (result) | '26/08/2015', 'time TIMESTAMP', map('timestampFormat', 'dd/MM/yyyy')                                                                  | {2015-08-26 00:00:00}           | struct<time:timestamp>                          |

  Rule: A NOT NULL in the schema argument is ignored

    # `CsvToStructs` forces the user-supplied schema nullable before using it
    # (`schema.asNullable`, csvExpressions.scala:84 and :97), so a NOT NULL field in the DDL is
    # ignored -- a missing or malformed field still parses to NULL, and honouring the flag would
    # put a real NULL under a non-nullable Arrow field.
    #
    # Asserted by VALUE rather than by `query schema` on purpose: Sail also diverges on the
    # OUTER struct's flag (Spark declares it non-nullable, see the two @sail-bug scenarios in the
    # Output schema rule above), so a schema assertion here would stack two independent bugs
    # behind one tag. The value path isolates this one.

    Scenario: a missing field under a NOT NULL schema yields NULL
      When query
        """
        SELECT from_csv('', 'a INT NOT NULL') AS result
        """
      Then query result
        | result |
        | {NULL} |

    # Control: without NOT NULL the same query must give the same answer. If this one ever
    # diverges from the scenario above, the NOT NULL flag is being honoured again.
    Scenario: a missing field under a plain schema yields NULL
      When query
        """
        SELECT from_csv('', 'a INT') AS result
        """
      Then query result
        | result |
        | {NULL} |

