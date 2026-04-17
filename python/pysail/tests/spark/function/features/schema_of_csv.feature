Feature: schema_of_csv infers a CSV schema from a literal row

  Scenario: schema_of_csv infers integer and string columns
    When query
    """
    SELECT schema_of_csv('1,abc') AS schema
    """
    Then query result
    | schema                          |
    | STRUCT<_c0: INT, _c1: STRING> |

  Scenario: schema_of_csv honors a custom separator
    When query
    """
    SELECT schema_of_csv('1|abc', map('sep', '|')) AS schema
    """
    Then query result
    | schema                          |
    | STRUCT<_c0: INT, _c1: STRING> |

  Scenario: schema_of_csv infers boolean and date columns
    When query
    """
    SELECT schema_of_csv('true,2024-01-02') AS schema
    """
    Then query result
    | schema                              |
    | STRUCT<_c0: BOOLEAN, _c1: DATE> |
