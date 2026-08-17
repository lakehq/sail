Feature: schema_of_csv infers a CSV schema from a literal row

    Scenario Outline: schema_of_csv infers <case>
      When query
        """
        SELECT schema_of_csv(<arg>) AS schema
        """
      Then query result
        | schema   |
        | <schema> |

      Examples:
        | case                       | arg               | schema                          |
        | integer and string columns | '1,abc'           | STRUCT<_c0: INT, _c1: STRING>   |
        | boolean and date columns   | 'true,2024-01-02' | STRUCT<_c0: BOOLEAN, _c1: DATE> |

    # Kept separate: the SQL contains `|`, which a data-table cell would re-escape.
    Scenario: schema_of_csv honors a custom separator
      When query
        """
        SELECT schema_of_csv('1|abc', map('sep', '|')) AS schema
        """
      Then query result
        | schema                        |
        | STRUCT<_c0: INT, _c1: STRING> |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to schema_of_csv yields the schema Spark declares
      When query
        """
        SELECT schema_of_csv('1,abc') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
