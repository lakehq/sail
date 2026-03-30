Feature: Delta Lake Version Checksum

  @sail-only
  Rule: Delta log writes version checksum files after successful commits

    Background:
      Given variable location for temporary directory delta_version_checksum
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_version_checksum_test
        """
      Given statement template
        """
        CREATE TABLE delta_version_checksum_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableInCommitTimestamps' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_version_checksum_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_version_checksum_test VALUES
          (3, 'three')
        """

    Scenario: Delta log contains checksum files and latest checksum matches snapshot
      When query
        """
        SELECT * FROM delta_version_checksum_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then delta log first commit protocol and metadata contains
        | path                                               | value  |
        | metaData.configuration['delta.enableInCommitTimestamps'] | "true" |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        """
      Then delta log JSON file 00000000000000000001.crc in delta_log matches snapshot

  @sail-only
  Rule: Delta log checksum writing can be explicitly enabled by table property

    Background:
      Given variable location for temporary directory delta_version_checksum_enabled
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_version_checksum_enabled_test
        """
      Given statement template
        """
        CREATE TABLE delta_version_checksum_enabled_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.writeChecksumFile.enabled' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_version_checksum_enabled_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_version_checksum_enabled_test VALUES
          (3, 'three')
        """

    Scenario: Delta log checksum snapshot records explicit writeChecksumFile enablement
      When query
        """
        SELECT * FROM delta_version_checksum_enabled_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then delta log first commit protocol and metadata contains
        | path                                                  | value  |
        | metaData.configuration['delta.writeChecksumFile.enabled'] | "true" |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        """
      Then delta log JSON file 00000000000000000001.crc in delta_log matches snapshot

  @sail-only
  Rule: Delta log checksum writing can be disabled by table property

    Background:
      Given variable location for temporary directory delta_version_checksum_disabled
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_version_checksum_disabled_test
        """
      Given statement template
        """
        CREATE TABLE delta_version_checksum_disabled_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.writeChecksumFile.enabled' = 'false'
        )
        """
      Given statement
        """
        INSERT INTO delta_version_checksum_disabled_test VALUES
          (1, 'one'),
          (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_version_checksum_disabled_test VALUES
          (3, 'three')
        """

    Scenario: Delta log omits checksum files when writeChecksumFile is disabled
      When query
        """
        SELECT * FROM delta_version_checksum_disabled_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |
      Then delta log first commit protocol and metadata contains
        | path                                                  | value   |
        | metaData.configuration['delta.writeChecksumFile.enabled'] | "false" |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.json
        📄 00000000000000000001.json
        """
