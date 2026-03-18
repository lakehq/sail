Feature: Delta Lake checksum read path

  @sail-only
  Rule: Metadata-as-data reads can use the latest checksum directly

    Background:
      Given variable location for temporary directory delta_checksum_read_exact
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checksum_read_exact_test
        """
      Given statement template
        """
        CREATE TABLE delta_checksum_read_exact_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_exact_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_exact_test VALUES
          (3, 'three')
        """

    Scenario: Metadata-as-data SELECT succeeds when latest checksum exists
      When query
        """
        SELECT * FROM delta_checksum_read_exact_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then file tree including checksum files in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        """

  @sail-only
  Rule: Metadata-as-data reads can use an older checksum as a hint

    Background:
      Given variable location for temporary directory delta_checksum_read_hint
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checksum_read_hint_test
        """
      Given statement template
        """
        CREATE TABLE delta_checksum_read_hint_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_hint_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_hint_test VALUES
          (3, 'three')
        """

    Scenario: Metadata-as-data read succeeds when newest checksum is missing
      Given file 00000000000000000001.crc in delta_log is deleted
      When query
        """
        SELECT * FROM delta_checksum_read_hint_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then file tree including checksum files in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.json
        """

  @sail-only
  Rule: Metadata-as-data reads recover from malformed latest checksum files

    Background:
      Given variable location for temporary directory delta_checksum_read_malformed
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checksum_read_malformed_test
        """
      Given statement template
        """
        CREATE TABLE delta_checksum_read_malformed_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_malformed_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_malformed_test VALUES
          (3, 'three')
        """

    Scenario: Metadata-as-data read succeeds when newest checksum is malformed
      Given file 00000000000000000001.crc in delta_log is replaced with
        """
        {not-valid-json
        """
      When query
        """
        SELECT * FROM delta_checksum_read_malformed_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then file tree including checksum files in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        """

  @sail-only
  Rule: Metadata-as-data reads fall back to full replay when checksum files are absent

    Background:
      Given variable location for temporary directory delta_checksum_read_missing
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checksum_read_missing_test
        """
      Given statement template
        """
        CREATE TABLE delta_checksum_read_missing_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_missing_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_checksum_read_missing_test VALUES
          (3, 'three')
        """

    Scenario: Metadata-as-data SELECT succeeds when all checksum files are deleted
      Given file 00000000000000000000.crc in delta_log is deleted
      Given file 00000000000000000001.crc in delta_log is deleted
      When query
        """
        SELECT * FROM delta_checksum_read_missing_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then file tree including checksum files in delta_log matches
        """
        📄 00000000000000000000.json
        📄 00000000000000000001.json
        """
