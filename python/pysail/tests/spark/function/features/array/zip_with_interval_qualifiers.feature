@function(lambda)
@sail-bug
Feature: Zip functions preserve supplied interval qualifiers

  Background:
    Given config spark.sql.ansi.enabled = true

  Rule: Qualified interval fields survive zip binding and results

    Scenario: A year interval array parameter casts to years
      When query
        """
        SELECT zip_with(CAST(array(INTERVAL '2' YEAR) AS ARRAY<INTERVAL YEAR>),
                        array(1), (x, y) -> CAST(x AS INT)) AS result
        """
      Then query result
        | result |
        | [2]    |

    Scenario: A zip result retains the year interval element qualifier
      When query
        """
        SELECT zip_with(CAST(array(INTERVAL '2' YEAR) AS ARRAY<INTERVAL YEAR>),
                        array(1), (x, y) -> x) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: interval year (containsNull = true)
        """

    Scenario: A downstream lambda casts the qualified zip result to years
      When query
        """
        SELECT transform(
                 zip_with(CAST(array(INTERVAL '2' YEAR) AS ARRAY<INTERVAL YEAR>),
                          array(id), (x, y) -> x),
                 x -> CAST(x AS INT)) AS result
        FROM range(2)
        """
      Then query result
        | result |
        | [2]    |
        | [2]    |

    Scenario: A year interval map value casts to years
      When query
        """
        SELECT map_values(map_zip_with(
                 CAST(map('a', INTERVAL '2' YEAR) AS MAP<STRING, INTERVAL YEAR>),
                 map('a', 1), (k, x, y) -> CAST(x AS INT))) AS result
        """
      Then query result
        | result |
        | [2]    |

    Scenario: A map result retains the year interval value qualifier
      When query
        """
        SELECT map_zip_with(
                 CAST(map('a', INTERVAL '2' YEAR) AS MAP<STRING, INTERVAL YEAR>),
                 map('a', 1), (k, x, y) -> x) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: interval year (valueContainsNull = true)
        """

    Scenario: A year interval map key casts to years
      When query
        """
        SELECT map_values(map_zip_with(
                 CAST(map(INTERVAL '2' YEAR, 1) AS MAP<INTERVAL YEAR, INT>),
                 CAST(map(INTERVAL '2' YEAR, 2) AS MAP<INTERVAL YEAR, INT>),
                 (k, x, y) -> CAST(k AS INT))) AS result
        """
      Then query result
        | result |
        | [2]    |
