Feature: COUNT(DISTINCT *) function

  Rule: COUNT(DISTINCT *) counts distinct rows

    # The expected header is the column name Spark derives for `COUNT(DISTINCT *)`,
    # so it is spelled out from the same <cols> slot and stays asserted.
    Scenario Outline: count distinct star counts distinct rows <case>
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES <values> AS t(<cols>)
        """
      Then query result
        | count(DISTINCT <cols>) |
        | <n>                    |

      Examples:
        | case                    | values                                      | cols             | n |
        | with duplicates         | (1, 1.0, 'a'), (2, 2.0, 'b'), (1, 1.0, 'a') | a, b, c          | 2 |
        | all same                | (1, 1), (1, 1), (1, 1)                      | a, b             | 1 |
        | all different           | (1, 1), (2, 2), (3, 3)                      | a, b             | 3 |
        | single column           | (1), (2), (1)                               | a                | 2 |
        | mixed case column names | (1, 2), (1, 2), (3, 4)                      | MyCol, UPPER_COL | 2 |

    Scenario: count distinct star with group by
      When query
        """
        SELECT g, COUNT(DISTINCT *) FROM VALUES ('x', 1), ('x', 1), ('x', 2), ('y', 1) AS t(g, v) GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | count(DISTINCT g, v) |
        | x | 2                    |
        | y | 1                    |

  Rule: COUNT(DISTINCT *) with NULLs

    Scenario Outline: count distinct star with NULLs <case>
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES <values> AS t(a, b)
        """
      Then query result
        | count(DISTINCT a, b) |
        | <n>                  |

      Examples:
        | case                                | values                                                | n |
        | skips rows where any column is null | (1, 'a'), (NULL, 'b'), (1, 'a')                       | 1 |
        | all nulls returns zero              | (NULL, NULL), (NULL, NULL)                            | 0 |
        | with some null columns              | (1, NULL), (2, NULL), (1, NULL)                       | 0 |
        | mixed nulls and values              | (1, 'a'), (1, NULL), (2, 'b'), (2, 'b'), (NULL, NULL) | 2 |
