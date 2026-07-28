@json_tuple
Feature: json_tuple with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: json_tuple — the argument may come from a column

    @column_args
    Scenario: json_tuple with the argument as a literal
      When query
        """
        SELECT json_tuple('{"a":1, "b":2}', 'a', 'b')
        """
      Then query result ordered
        | c0 | c1 |
        | 1  | 2  |

    # Sail rejects the column: Sail errors: invalid argument: json_tuple field names must be string literals
    @column_args @sail-bug
    Scenario Outline: Json_tuple: <case>
      When query
        """
        SELECT json_tuple('{"a":1, "b":2}', <f1>, <f2>) FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | c0     | c1     |
        | 1      | 2      |
        | <r2c0> | <r2c1> |

      Examples:
        | case                                                      | f1  | f2  | v1  | v2   | r2c0 | r2c1 |
        | json_tuple takes argument 2 from a column containing NULL | c   | 'b' | 'a' | NULL | NULL | 2    |
        | json_tuple takes argument 2 from a column                 | c   | 'b' | 'a' | 'a'  | 1    | 2    |
        | json_tuple takes argument 3 from a column containing NULL | 'a' | c   | 'b' | NULL | 1    | NULL |
        | json_tuple takes argument 3 from a column                 | 'a' | c   | 'b' | 'b'  | 1    | 2    |

    @column_args @sail-bug
    Scenario: json_tuple takes argument 2 from a column holding two different values
      When query
        """
        SELECT json_tuple('{"a":1, "b":2}', c, 'b') FROM VALUES (1, 'a'), (2, 'b') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | c0 | c1 |
        | 1  | 2  |
        | 2  | 2  |
