Feature: instr output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to instr yields the schema Spark declares
      When query
        """
        SELECT instr('SparkSQL', 'SQL') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to instr yields the schema Spark declares
      When query
        """
        SELECT instr(CAST(id AS STRING), 'SQL') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to instr stays nullable
      When query
        """
        SELECT instr(c, 'SQL') AS result FROM VALUES ('SparkSQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
