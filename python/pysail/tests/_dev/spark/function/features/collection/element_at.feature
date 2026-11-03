Feature: element_at output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to element_at yields the schema Spark declares
      When query
        """
        SELECT element_at(array(1, 2, 3), 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to element_at stays nullable
      When query
        """
        SELECT element_at(c, 2) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Index out of bounds depends on ANSI mode

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(<collection>, <index>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                 | collection                          | index |
        | index after the last element         | array(1, 2, 3)                      | 4     |
        | negative index before the first item | array(1, 2, 3)                      | -4    |
        | empty array                          | CAST(array() AS ARRAY<INT>)         | 1     |
        | empty filter result                  | filter(array(1, 2, 3), x -> x > 5)  | 1     |

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT element_at(<collection>, <index>) AS result
        """
      Then query error (?i)out of bounds

      Examples:
        | case                                 | collection                          | index |
        | index after the last element         | array(1, 2, 3)                      | 4     |
        | negative index before the first item | array(1, 2, 3)                      | -4    |
        | empty array                          | CAST(array() AS ARRAY<INT>)         | 1     |
        | empty filter result                  | filter(array(1, 2, 3), x -> x > 5)  | 1     |

    Scenario Outline: In-bounds index with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT element_at(array(1, 2, 3), 1) AS first, element_at(array(1, 2, 3), -1) AS last
        """
      Then query result
        | first | last |
        | 1     | 3    |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: Index 0 fails with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT element_at(array(1, 2, 3), 0) AS result
        """
      Then query error (?i)index 0 is invalid

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: NULL array and missing map key return NULL with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT element_at(CAST(NULL AS ARRAY<INT>), 1) AS from_array, element_at(map(1, 'a'), 2) AS from_map
        """
      Then query result
        | from_array | from_map |
        | NULL       | NULL     |

      Examples:
        | ansi  |
        | false |
        | true  |
