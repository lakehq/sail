@bitmap_bucket_number
Feature: bitmap_bucket_number output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to bitmap_bucket_number yields the schema Spark declares
      When query
        """
        SELECT bitmap_bucket_number(123) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to bitmap_bucket_number yields the schema Spark declares
      When query
        """
        SELECT bitmap_bucket_number(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column input to bitmap_bucket_number stays nullable
      When query
        """
        SELECT bitmap_bucket_number(c) AS result FROM VALUES (123), (CAST(NULL AS INT)) AS t(c)
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
        SELECT bitmap_bucket_number(CAST(-2147483648 AS INT)) AS result
        """
      Then query result
        | result |
        | -65536 |

    Scenario: an input beyond the 32-bit range does not overflow
      When query
        """
        SELECT bitmap_bucket_number(3000000000) AS result
        """
      Then query result
        | result |
        | 91553  |

    Scenario: a negative input beyond the 32-bit range does not overflow
      When query
        """
        SELECT bitmap_bucket_number(-3000000000) AS result
        """
      Then query result
        | result  |
        | -91552  |
