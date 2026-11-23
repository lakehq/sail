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

  Scenario: Scalar and collection functions compare wall-clock timestamps with the same instant
    Given config spark.sql.session.timeZone = +05:30:45
    When query
      """
      SELECT unix_micros(greatest(ntz, ltz)) AS greatest_us,
             unix_micros(array(ntz, ltz)[0]) AS array_us,
             array_contains(array(ltz), ntz) AS has_element,
             size(array_union(array(ntz), array(ltz))) AS distinct_elements,
             unix_micros(element_at(map(1, ntz, 2, ltz), 1)) AS map_us,
             map_contains_key(map(ltz, 1), ntz) AS has_key,
             size(map_zip_with(map(ntz, 1), map(ltz, 2), (k, x, y) -> x + y)) AS distinct_keys
      FROM VALUES (TIMESTAMP_NTZ '1970-01-01 05:30:45', timestamp_micros(0L)) AS t(ntz, ltz)
      """
    Then query result
      | greatest_us | array_us | has_element | distinct_elements | map_us | has_key | distinct_keys |
      | 0           | 0        | true        | 1                 | 0      | true    | 1             |

  Scenario: Union reads wall-clock timestamps in a subminute session time zone
    Given config spark.sql.session.timeZone = +05:30:45
    When query
      """
      SELECT unix_micros(ts) AS epoch_us FROM (
        SELECT TIMESTAMP_NTZ '1970-01-01 05:30:45' AS ts UNION ALL
        SELECT timestamp_micros(0L))
      """
    Then query result
      | epoch_us |
      | 0        |
      | 0        |

  Scenario Outline: Intersect converts temporal inputs using the full session offset
    Given config spark.sql.session.timeZone = +05:30:45
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT unix_micros(ts) AS epoch_us FROM (
        SELECT <value> AS ts INTERSECT SELECT timestamp_micros(<micros>))
      """
    Then query result
      | epoch_us   |
      | <expected> |

    Examples:
      | value                 | micros        | expected     |
      | DATE '1970-01-01'     | -19845000000L | -19845000000 |
      | '1970-01-01 05:30:45' | 0L            | 0            |
