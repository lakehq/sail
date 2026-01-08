Feature: DuckLake Read Explain

  Rule: EXPLAIN EXTENDED shows DuckLake read plan shape

    Background:
      Given variable metadata for temporary directory metadata.ducklake
      Given variable data for temporary directory data
      Given ducklake test table is created in metadata and data
      Given final statement
        """
        DROP TABLE IF EXISTS ducklake_explain_test
        """
      Given statement template
        """
        CREATE TABLE ducklake_explain_test
        USING ducklake
        OPTIONS (
          url '{{ ducklake_url }}',
          `table` '{{ ducklake_table }}',
          base_path '{{ ducklake_base_path }}'
        )
        """

    Scenario: EXPLAIN EXTENDED includes DuckLake scan operators
      When query
        """
        EXPLAIN EXTENDED
        SELECT *
        FROM ducklake_explain_test
        WHERE score > 90
        """
      Then query plan matches snapshot

