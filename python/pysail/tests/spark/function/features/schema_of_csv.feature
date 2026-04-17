Feature: schema_of_csv infers a CSV schema from a literal row

  Scenario: schema_of_csv infers integer and string columns
    When query
    """
    SELECT schema_of_csv('1,abc') AS schema
    """
    Then query result
    | schema                          |
    | STRUCT<_c0: INT, _c1: STRING> |

  Scenario: from_csv accepts a schema inferred by schema_of_csv
    When query
    """
    SELECT
      from_csv('1,2,3', schema_of_csv('1,2,3')) AS parsed,
      typeof(from_csv('1,2,3', schema_of_csv('1,2,3'))) AS type
    """
    Then query result
    | parsed     | type                            |
    | {1, 2, 3} | struct<_c0:int,_c1:int,_c2:int> |

  Scenario: schema_of_csv honors a custom separator
    When query
    """
    SELECT schema_of_csv('1|abc', map('sep', '|')) AS schema
    """
    Then query result
    | schema                          |
    | STRUCT<_c0: INT, _c1: STRING> |
