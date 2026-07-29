@struct
Feature: struct output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to struct yields the schema Spark declares
      When query
        """
        SELECT struct(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- col1: integer (nullable = false)
         |    |-- col2: integer (nullable = false)
         |    |-- col3: integer (nullable = false)
        """

    Scenario: a non-null column input to struct yields the schema Spark declares
      When query
        """
        SELECT struct(CAST(id AS INT), 2, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- col1: integer (nullable = false)
         |    |-- col2: integer (nullable = false)
         |    |-- col3: integer (nullable = false)
        """

    Scenario: a nullable column input to struct stays nullable
      When query
        """
        SELECT struct(c, 2, 3) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- c: integer (nullable = true)
         |    |-- col2: integer (nullable = false)
         |    |-- col3: integer (nullable = false)
        """
