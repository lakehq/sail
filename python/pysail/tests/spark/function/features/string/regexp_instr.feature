@regexp_instr
Feature: regexp_instr output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to regexp_instr yields the schema Spark declares
      When query
        """
        SELECT regexp_instr(r"\abc", r"^\\abc$") AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to regexp_instr yields the schema Spark declares
      When query
        """
        SELECT regexp_instr(CAST(id AS STRING), r"^\\abc$") AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to regexp_instr stays nullable
      When query
        """
        SELECT regexp_instr(c, r"^\\abc$") AS result FROM VALUES (r"\abc"), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
