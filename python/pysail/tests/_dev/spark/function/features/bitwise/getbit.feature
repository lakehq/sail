Feature: getbit output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to getbit yields the schema Spark declares
      When query
        """
        SELECT getbit(11, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = false)
        """

    Scenario: a non-null column input to getbit yields the schema Spark declares
      When query
        """
        SELECT getbit(CAST(id AS INT), 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = false)
        """

    Scenario: a nullable column input to getbit stays nullable
      When query
        """
        SELECT getbit(c, 0) AS result FROM VALUES (11), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = true)
        """
