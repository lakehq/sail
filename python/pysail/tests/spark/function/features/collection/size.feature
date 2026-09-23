Feature: size output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to size yields the schema Spark declares
      When query
        """
        SELECT size(array('b', 'd', 'c', 'a')) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to size stays nullable
      When query
        """
        SELECT size(c) AS result FROM VALUES (array('b', 'd', 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Non-null collections

    Scenario: size of a non-null array
      When query
        """
        SELECT size(array(1, 2, 3)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: size of a non-null map
      When query
        """
        SELECT size(map('a', 1, 'b', 2)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: size of an empty array is 0
      When query
        """
        SELECT size(array()) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: size(NULL) is -1 under legacy sizeOfNull with ANSI off

    Scenario: legacy sizeOfNull true and ANSI off returns -1 for null array
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.sizeOfNull = true
      When query
        """
        SELECT size(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: legacy sizeOfNull true and ANSI off returns -1 for null map
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.sizeOfNull = true
      When query
        """
        SELECT size(CAST(NULL AS MAP<STRING,INT>)) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: legacy sizeOfNull true and ANSI off returns -1 for null map in a column
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.sizeOfNull = true
      When query
        """
        SELECT size(c) AS result FROM VALUES
          (MAP(1, 2, 3, 4)),
          (CAST(NULL AS MAP<INT,INT>)),
          (MAP(-1, -3))
          AS t(c)
        """
      Then query result
        | result |
        | 2      |
        | -1     |
        | 1      |

  Rule: size(NULL) is NULL when sizeOfNull is not legacy

    Scenario: legacy sizeOfNull false returns NULL even with ANSI off
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.sizeOfNull = false
      When query
        """
        SELECT size(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: ANSI on returns NULL even with legacy sizeOfNull true
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.legacy.sizeOfNull = true
      When query
        """
        SELECT size(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |
