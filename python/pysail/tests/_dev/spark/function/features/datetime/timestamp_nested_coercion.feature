# SPDX-License-Identifier: Apache-2.0

Feature: Coercion of nested timestamps in a session with a seconds offset

  Scenario: Coalesce converts nested wall-clock timestamps to instants
    Given config spark.sql.session.timeZone = +05:30:45
    When query
      """
      SELECT
        unix_micros(coalesce(array(ntz), array(ltz))[0]) AS array_us,
        unix_micros(coalesce(named_struct('t', ntz, 'n', 1), named_struct('t', ltz, 'n', 2L)).t) AS struct_us
      FROM VALUES (TIMESTAMP_NTZ '1970-01-01 05:30:45', timestamp_micros(0L)) AS input(ntz, ltz)
      """
    Then query result
      | array_us | struct_us |
      | 0        | 0         |
