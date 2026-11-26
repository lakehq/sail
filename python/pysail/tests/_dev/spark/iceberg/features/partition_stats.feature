# SPDX-License-Identifier: Apache-2.0

Feature: Iceberg partition statistics

  Scenario: Incremental Avro statistics preserve typed partition values
    Given variable location for temporary directory partition_stats
    And an Iceberg table with two appends and Avro partition statistics for the first snapshot
    And final statement
      """
      DROP TABLE IF EXISTS typed_partition_stats
      """
    And statement template
      """
      CREATE TABLE typed_partition_stats USING iceberg LOCATION {{ location.uri }}
      """
    When query
      """
      CALL system.compute_partition_stats('typed_partition_stats')
      """
    Then the registered Avro partition statistics match the snapshot
