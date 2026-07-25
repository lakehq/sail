@regexp_like
Feature: regexp_like output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to regexp_like yields the schema Spark declares
      When query
        """
        SELECT regexp_like('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to regexp_like yields the schema Spark declares
      When query
        """
        SELECT regexp_like(CAST(id AS STRING), '%SystemDrive%\\Users.*') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to regexp_like stays nullable
      When query
        """
        SELECT regexp_like(c, '%SystemDrive%\\Users.*') AS result FROM VALUES ('%SystemDrive%\Users\John'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
