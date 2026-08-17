Feature: octet_length output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to octet_length yields the schema Spark declares
      When query
        """
        SELECT octet_length('Spark SQL') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to octet_length yields the schema Spark declares
      When query
        """
        SELECT octet_length(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to octet_length stays nullable
      When query
        """
        SELECT octet_length(c) AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
