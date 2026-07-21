Feature: Delta Lake VACUUM protocol check

  Scenario: VACUUM protocol check allows ordinary reads and writes
    Given variable location for temporary directory delta_vacuum_protocol_check
    Given final statement
      """
      DROP TABLE IF EXISTS delta_vacuum_protocol_check
      """
    Given statement template
      """
      CREATE TABLE delta_vacuum_protocol_check (
        id INT,
        value STRING
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.feature.vacuumProtocolCheck' = 'supported')
      """
    Given statement
      """
      INSERT INTO delta_vacuum_protocol_check VALUES (1, 'append')
      """
    Given statement
      """
      INSERT OVERWRITE TABLE delta_vacuum_protocol_check VALUES (2, 'overwrite')
      """
    Then delta log first commit protocol and metadata contains
      | path                      | value                   |
      | protocol.minReaderVersion | 3                       |
      | protocol.minWriterVersion | 7                       |
      | protocol.readerFeatures   | ["vacuumProtocolCheck"] |
      | protocol.writerFeatures   | ["vacuumProtocolCheck"] |
    When query
      """
      SELECT id, value FROM delta_vacuum_protocol_check ORDER BY id
      """
    Then query result ordered
      | id | value     |
      | 2  | overwrite |
