Feature: rlike output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to rlike yields the schema Spark declares
      When query
        """
        SELECT rlike('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to rlike yields the schema Spark declares
      When query
        """
        SELECT rlike(CAST(id AS STRING), '%SystemDrive%\\Users.*') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to rlike stays nullable
      When query
        """
        SELECT rlike(c, '%SystemDrive%\\Users.*') AS result FROM VALUES ('%SystemDrive%\Users\John'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
