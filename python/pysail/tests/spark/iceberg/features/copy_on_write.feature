Feature: Iceberg copy-on-write row operations

  Rule: Copy-on-write metadata
    Background:
      Given variable location for temporary directory iceberg_cow_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_cow_metadata
        """
      Given statement template
        """
        CREATE TABLE iceberg_cow_metadata (
          id INT,
          value STRING,
          selected BOOLEAN
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.update.mode' = 'copy-on-write'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_cow_metadata VALUES
          (1, 'change', true),
          (2, 'keep', false),
          (3, 'unknown', NULL)
        """

    Scenario: Partial UPDATE snapshots replacement metadata
      Given statement
        """
        UPDATE iceberg_cow_metadata
        SET value = concat(value, '-updated')
        WHERE selected
        """
      Then iceberg metadata matches snapshot
      Then iceberg current manifest list matches snapshot
      Then iceberg current snapshot summary matches snapshot
      Then iceberg snapshot count is 2
      When query
        """
        SELECT id, value, selected
        FROM iceberg_cow_metadata
        ORDER BY id
        """
      Then query result ordered
        | id | value          | selected |
        | 1  | change-updated | true     |
        | 2  | keep           | false    |
        | 3  | unknown        | NULL     |
