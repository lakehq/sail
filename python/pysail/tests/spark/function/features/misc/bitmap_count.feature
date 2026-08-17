Feature: bitmap_count output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to bitmap_count yields the schema Spark declares
      When query
        """
        SELECT bitmap_count(X '1010') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column input to bitmap_count stays nullable
      When query
        """
        SELECT bitmap_count(c) AS result FROM VALUES (X '1010'), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
