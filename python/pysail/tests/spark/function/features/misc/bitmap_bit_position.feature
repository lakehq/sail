Feature: bitmap_bit_position output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to bitmap_bit_position yields the schema Spark declares
      When query
        """
        SELECT bitmap_bit_position(1) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to bitmap_bit_position yields the schema Spark declares
      When query
        """
        SELECT bitmap_bit_position(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column input to bitmap_bit_position stays nullable
      When query
        """
        SELECT bitmap_bit_position(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: 64-bit arithmetic

    Scenario: negating the smallest 32-bit integer does not overflow
      When query
        """
        SELECT bitmap_bit_position(CAST(-2147483648 AS INT)) AS result
        """
      Then query result
        | result |
        | 0 |

    Scenario: an input beyond the 32-bit range does not overflow
      When query
        """
        SELECT bitmap_bit_position(3000000000) AS result
        """
      Then query result
        | result |
        | 24063  |

    Scenario: a negative input beyond the 32-bit range does not overflow
      When query
        """
        SELECT bitmap_bit_position(-3000000000) AS result
        """
      Then query result
        | result |
        | 24064  |
