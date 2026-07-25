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

    @sail-bug
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
