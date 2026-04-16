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

  @sail-only
  Rule: Incremental CRC correctly tracks file counts across multiple commits

    Background:
      Given variable location for temporary directory delta_incremental_crc
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_incremental_crc_test
        """
      Given statement template
        """
        CREATE TABLE delta_incremental_crc_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_incremental_crc_test VALUES (1, 'one'), (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_incremental_crc_test VALUES (3, 'three')
        """
      Given statement
        """
        INSERT INTO delta_incremental_crc_test VALUES (4, 'four')
        """

    Scenario: CRC numFiles accumulates correctly across sequential inserts
      When query
        """
        SELECT COUNT(*) AS cnt FROM delta_incremental_crc_test
        """
      Then query result
        | cnt |
        | 4   |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        """
      Then delta log JSON file 00000000000000000000.crc in delta_log contains
        | path     | value |
        | numFiles | 1     |
      Then delta log JSON file 00000000000000000001.crc in delta_log contains
        | path     | value |
        | numFiles | 2     |
      Then delta log JSON file 00000000000000000002.crc in delta_log contains
        | path     | value |
        | numFiles | 3     |

  @sail-only
  Rule: Incremental CRC correctly tracks file counts when files are replaced

    Background:
      Given variable location for temporary directory delta_incremental_crc_replace
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_incremental_crc_replace_test
        """
      Given statement template
        """
        CREATE TABLE delta_incremental_crc_replace_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_incremental_crc_replace_test VALUES (1, 'one'), (2, 'two'), (3, 'three')
        """

    Scenario: CRC numFiles stays correct after a DELETE that rewrites a file
      Given statement
        """
        DELETE FROM delta_incremental_crc_replace_test WHERE id = 1
        """
      When query
        """
        SELECT COUNT(*) AS cnt FROM delta_incremental_crc_replace_test
        """
      Then query result
        | cnt |
        | 2   |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        """
      Then delta log JSON file 00000000000000000000.crc in delta_log contains
        | path     | value |
        | numFiles | 1     |
      Then delta log JSON file 00000000000000000001.crc in delta_log contains
        | path     | value |
        | numFiles | 1     |

  @sail-only
  Rule: Broken CRC chain is healed via full-snapshot fallback on next commit

    Background:
      Given variable location for temporary directory delta_crc_fallback
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_crc_fallback_test
        """
      Given statement template
        """
        CREATE TABLE delta_crc_fallback_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_crc_fallback_test VALUES (1, 'one'), (2, 'two')
        """

    Scenario: CRC is regenerated via full-snapshot fallback when prev CRC is missing
      Given file 00000000000000000000.crc in delta_log is deleted
      Given statement
        """
        INSERT INTO delta_crc_fallback_test VALUES (3, 'three')
        """
      When query
        """
        SELECT COUNT(*) AS cnt FROM delta_crc_fallback_test
        """
      Then query result
        | cnt |
        | 3   |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        """
      Then delta log JSON file 00000000000000000001.crc in delta_log contains
        | path     | value |
        | numFiles | 2     |
