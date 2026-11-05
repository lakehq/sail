Feature: Nested TIMESTAMP_NTZ values compare with TIMESTAMP values in the session time zone
  Spark finds the common type of arrays, structs and maps element by element, so TIMESTAMP_NTZ elements become TIMESTAMP
  in the session time zone, like top-level values, including DST gaps and overlaps.

  Background:
    Given variable location for temporary directory timestamp_ntz_nested
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_nested
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_nested (id INT, ntz TIMESTAMP_NTZ, ltz TIMESTAMP, s STRING) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_nested VALUES
        (0, TIMESTAMP_NTZ '2020-06-14 12:00:00', TIMESTAMP '2020-06-14 19:00:00', '2020-06-14 12:00:00'),
        (1, TIMESTAMP_NTZ '2020-03-08 02:30:00', TIMESTAMP '2020-03-08 10:30:00', '2020-03-08 03:30:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 08:30:00', '2020-11-01 01:30:00'),
        (3, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 09:30:00', '2020-11-01 01:30:00-08:00'),
        (4, TIMESTAMP_NTZ '2020-06-14 19:00:00', TIMESTAMP '2020-06-14 19:00:00', '2020-06-14 19:00:00'),
        (5, TIMESTAMP_NTZ '2020-06-15 03:00:00', TIMESTAMP '2020-06-14 19:00:00', '2020-06-15 03:00:00'),
        (6, NULL, TIMESTAMP '2020-06-14 19:00:00', NULL)
      """

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | true         | false           | true          | true           | true                 | true      |
      | 1  | true         | false           | true          | true           | true                 | true      |
      | 2  | true         | false           | true          | true           | true                 | true      |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | false        | false           | false         | false          | true                 | false     |
      | 5  | false        | false           | false         | false          | true                 | false     |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | false        | true            | false         | true           | true                 | false     |
      | 1  | false        | true            | false         | true           | false                | false     |
      | 2  | false        | true            | false         | true           | true                 | false     |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | false        | true            | false         | true           | true                 | false     |
      | 5  | true         | false           | true          | true           | true                 | true      |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | false        | true            | false         | true           | true                 | false     |
      | 1  | false        | true            | false         | true           | false                | false     |
      | 2  | false        | true            | false         | true           | true                 | false     |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | false        | true            | false         | true           | true                 | false     |
      | 5  | false        | false           | false         | false          | true                 | false     |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | false        | true            | false         | true           | true                 | false     |
      | 1  | false        | true            | false         | true           | false                | false     |
      | 2  | false        | true            | false         | true           | true                 | false     |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | true         | false           | true          | true           | true                 | true      |
      | 5  | false        | false           | false         | false          | true                 | false     |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | true               | true         | true    | 0           | 1          | 1              |
      | 1  | true               | true         | true    | 0           | 1          | 1              |
      | 2  | true               | true         | true    | 0           | 1          | 1              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | false              | false        | false   | 1           | 2          | 0              |
      | 5  | false              | false        | false   | 1           | 2          | 0              |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | false              | false        | false   | 1           | 2          | 0              |
      | 1  | false              | false        | false   | 1           | 2          | 0              |
      | 2  | false              | false        | false   | 1           | 2          | 0              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | false              | false        | false   | 1           | 2          | 0              |
      | 5  | true               | true         | true    | 0           | 1          | 1              |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | false              | false        | false   | 1           | 2          | 0              |
      | 1  | false              | false        | false   | 1           | 2          | 0              |
      | 2  | false              | false        | false   | 1           | 2          | 0              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | false              | false        | false   | 1           | 2          | 0              |
      | 5  | false              | false        | false   | 1           | 2          | 0              |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | false              | false        | false   | 1           | 2          | 0              |
      | 1  | false              | false        | false   | 1           | 2          | 0              |
      | 2  | false              | false        | false   | 1           | 2          | 0              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | true               | true         | true    | 0           | 1          | 1              |
      | 5  | false              | false        | false   | 1           | 2          | 0              |

  Scenario: Nested TIMESTAMP_NTZ values widen to TIMESTAMP in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        transform(array(ntz, ltz), x -> unix_seconds(x) - unix_seconds(ltz)) AS array_offsets,
        unix_seconds(named_struct('t', coalesce(ntz, ltz)).t) - unix_seconds(ltz) AS struct_coalesce_offset,
        array(coalesce(ntz, ltz)) = array(ltz) AS array_coalesce_equals,
        transform(array_union(array(ntz), array(ltz)), x -> unix_seconds(x) - unix_seconds(ltz)) AS union_offsets,
        unix_seconds(map_concat(map('n', ntz), map('l', ltz))['n']) - unix_seconds(ltz) AS map_value_offset,
        map_contains_key(map(coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00'), 1), ltz) AS map_key_contains
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_offsets | struct_coalesce_offset | array_coalesce_equals | union_offsets | map_value_offset | map_key_contains |
      | 0  | [0, 0]        | 0                      | true                  | [0]           | 0                | true             |
      | 1  | [0, 0]        | 0                      | true                  | [0]           | 0                | true             |
      | 2  | [0, 0]        | 0                      | true                  | [0]           | 0                | true             |
      | 3  | [-3600, 0]    | -3600                  | false                 | [-3600, 0]    | -3600            | false            |
      | 4  | [25200, 0]    | 25200                  | false                 | [25200, 0]    | 25200            | false            |
      | 5  | [54000, 0]    | 54000                  | false                 | [54000, 0]    | 54000            | false            |
      | 6  | [NULL, 0]     | 0                      | true                  | [NULL, 0]     | NULL             | false            |

  Scenario: Nested TIMESTAMP_NTZ values widen to TIMESTAMP in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        transform(array(ntz, ltz), x -> unix_seconds(x) - unix_seconds(ltz)) AS array_offsets,
        unix_seconds(named_struct('t', coalesce(ntz, ltz)).t) - unix_seconds(ltz) AS struct_coalesce_offset,
        array(coalesce(ntz, ltz)) = array(ltz) AS array_coalesce_equals,
        transform(array_union(array(ntz), array(ltz)), x -> unix_seconds(x) - unix_seconds(ltz)) AS union_offsets,
        unix_seconds(map_concat(map('n', ntz), map('l', ltz))['n']) - unix_seconds(ltz) AS map_value_offset,
        map_contains_key(map(coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00'), 1), ltz) AS map_key_contains
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_offsets | struct_coalesce_offset | array_coalesce_equals | union_offsets | map_value_offset | map_key_contains |
      | 0  | [-45000, 0]   | -45000                 | false                 | [-45000, 0]   | -45000           | false            |
      | 1  | [-48600, 0]   | -48600                 | false                 | [-48600, 0]   | -48600           | false            |
      | 2  | [-45000, 0]   | -45000                 | false                 | [-45000, 0]   | -45000           | false            |
      | 3  | [-48600, 0]   | -48600                 | false                 | [-48600, 0]   | -48600           | false            |
      | 4  | [-19800, 0]   | -19800                 | false                 | [-19800, 0]   | -19800           | false            |
      | 5  | [9000, 0]     | 9000                   | false                 | [9000, 0]     | 9000             | false            |
      | 6  | [NULL, 0]     | 0                      | true                  | [NULL, 0]     | NULL             | false            |

  Scenario: CASE, COALESCE and IF widen nested TIMESTAMP_NTZ values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        unix_seconds(coalesce(named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS coalesce_struct_offset,
        unix_seconds(if(id >= 0, named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS if_struct_offset,
        unix_seconds((CASE WHEN id >= 0 THEN array(ntz) ELSE array(ltz) END)[0]) - unix_seconds(ltz) AS case_array_offset,
        unix_seconds(array(array(ntz), array(ltz))[0][0]) - unix_seconds(ltz) AS nested_array_offset,
        CAST(coalesce(named_struct('t', ntz), named_struct('t', ltz)) AS STRING) AS coalesce_struct_string
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | coalesce_struct_offset | if_struct_offset | case_array_offset | nested_array_offset | coalesce_struct_string |
      | 0  | 0                      | 0                | 0                 | 0                   | {2020-06-14 12:00:00}  |
      | 1  | 0                      | 0                | 0                 | 0                   | {2020-03-08 03:30:00}  |
      | 2  | 0                      | 0                | 0                 | 0                   | {2020-11-01 01:30:00}  |
      | 3  | -3600                  | -3600            | -3600             | -3600               | {2020-11-01 01:30:00}  |
      | 4  | 25200                  | 25200            | 25200             | 25200               | {2020-06-14 19:00:00}  |
      | 5  | 54000                  | 54000            | 54000             | 54000               | {2020-06-15 03:00:00}  |
      | 6  | NULL                   | NULL             | NULL              | NULL                | {null}                 |

  Scenario: CASE, COALESCE and IF widen nested TIMESTAMP_NTZ values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        unix_seconds(coalesce(named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS coalesce_struct_offset,
        unix_seconds(if(id >= 0, named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS if_struct_offset,
        unix_seconds((CASE WHEN id >= 0 THEN array(ntz) ELSE array(ltz) END)[0]) - unix_seconds(ltz) AS case_array_offset,
        unix_seconds(array(array(ntz), array(ltz))[0][0]) - unix_seconds(ltz) AS nested_array_offset,
        CAST(coalesce(named_struct('t', ntz), named_struct('t', ltz)) AS STRING) AS coalesce_struct_string
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | coalesce_struct_offset | if_struct_offset | case_array_offset | nested_array_offset | coalesce_struct_string |
      | 0  | -54000                 | -54000           | -54000            | -54000              | {2020-06-14 12:00:00}  |
      | 1  | -57600                 | -57600           | -57600            | -57600              | {2020-03-08 02:30:00}  |
      | 2  | -54000                 | -54000           | -54000            | -54000              | {2020-11-01 01:30:00}  |
      | 3  | -57600                 | -57600           | -57600            | -57600              | {2020-11-01 01:30:00}  |
      | 4  | -28800                 | -28800           | -28800            | -28800              | {2020-06-14 19:00:00}  |
      | 5  | 0                      | 0                | 0                 | 0                   | {2020-06-15 03:00:00}  |
      | 6  | NULL                   | NULL             | NULL              | NULL                | {null}                 |

  Scenario: UNION widens nested TIMESTAMP_NTZ values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        u.id,
        unix_seconds(u.s.t) - unix_seconds(n.ltz) AS union_struct_offset,
        u.s.t = n.ltz AS union_struct_equals,
        CAST(u.s AS STRING) AS union_struct_string
      FROM (SELECT id, named_struct('t', ntz) AS s FROM timestamp_ntz_nested UNION ALL SELECT id + 100, named_struct('t', ltz) FROM timestamp_ntz_nested) u
      JOIN timestamp_ntz_nested n ON u.id = n.id
      ORDER BY u.id
      """
    Then query result ordered
      | id | union_struct_offset | union_struct_equals | union_struct_string   |
      | 0  | 0                   | true                | {2020-06-14 12:00:00} |
      | 1  | 0                   | true                | {2020-03-08 03:30:00} |
      | 2  | 0                   | true                | {2020-11-01 01:30:00} |
      | 3  | -3600               | false               | {2020-11-01 01:30:00} |
      | 4  | 25200               | false               | {2020-06-14 19:00:00} |
      | 5  | 54000               | false               | {2020-06-15 03:00:00} |
      | 6  | NULL                | NULL                | {null}                |

  Scenario: UNION widens nested TIMESTAMP_NTZ values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        u.id,
        unix_seconds(u.s.t) - unix_seconds(n.ltz) AS union_struct_offset,
        u.s.t = n.ltz AS union_struct_equals,
        CAST(u.s AS STRING) AS union_struct_string
      FROM (SELECT id, named_struct('t', ntz) AS s FROM timestamp_ntz_nested UNION ALL SELECT id + 100, named_struct('t', ltz) FROM timestamp_ntz_nested) u
      JOIN timestamp_ntz_nested n ON u.id = n.id
      ORDER BY u.id
      """
    Then query result ordered
      | id | union_struct_offset | union_struct_equals | union_struct_string   |
      | 0  | -54000              | false               | {2020-06-14 12:00:00} |
      | 1  | -57600              | false               | {2020-03-08 02:30:00} |
      | 2  | -54000              | false               | {2020-11-01 01:30:00} |
      | 3  | -57600              | false               | {2020-11-01 01:30:00} |
      | 4  | -28800              | false               | {2020-06-14 19:00:00} |
      | 5  | 0                   | true                | {2020-06-15 03:00:00} |
      | 6  | NULL                | NULL                | {null}                |

  # The common type of nested TIMESTAMP_NTZ and TIMESTAMP values keeps NULL children from either side.
  Scenario: Nested arrays keep NULL elements when TIMESTAMP_NTZ and TIMESTAMP widen in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        CAST(array(array(TIMESTAMP_NTZ '2020-06-14 12:00:00'), array(CAST(NULL AS TIMESTAMP))) AS STRING) AS ntz_then_null,
        CAST(array(array(CAST(NULL AS TIMESTAMP)), array(TIMESTAMP_NTZ '2020-06-14 12:00:00')) AS STRING) AS null_then_ntz,
        CAST(array(array(TIMESTAMP '2020-06-14 12:00:00'), array(CAST(NULL AS TIMESTAMP_NTZ))) AS STRING) AS ltz_then_null_ntz,
        CAST(array(array(CAST(NULL AS TIMESTAMP_NTZ)), array(TIMESTAMP '2020-06-14 12:00:00')) AS STRING) AS null_ntz_then_ltz,
        unix_seconds(array(array(TIMESTAMP_NTZ '2020-06-14 12:00:00'), array(CAST(NULL AS TIMESTAMP)))[0][0]) AS seconds
      """
    Then query result
      | ntz_then_null                   | null_then_ntz                   | ltz_then_null_ntz               | null_ntz_then_ltz               | seconds    |
      | [[2020-06-14 12:00:00], [null]] | [[null], [2020-06-14 12:00:00]] | [[2020-06-14 12:00:00], [null]] | [[null], [2020-06-14 12:00:00]] | 1592161200 |

  Scenario: Map values and struct fields keep NULL values when TIMESTAMP_NTZ and TIMESTAMP widen in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        CAST(array(map('k', TIMESTAMP_NTZ '2020-06-14 12:00:00'), map('k', CAST(NULL AS TIMESTAMP))) AS STRING) AS map_null_second,
        CAST(array(map('k', CAST(NULL AS TIMESTAMP)), map('k', TIMESTAMP_NTZ '2020-06-14 12:00:00')) AS STRING) AS map_null_first,
        CAST(array(map('k', array(TIMESTAMP_NTZ '2020-06-14 12:00:00')), map('k', array(CAST(NULL AS TIMESTAMP)))) AS STRING) AS map_array_value,
        CAST(array(named_struct('t', TIMESTAMP_NTZ '2020-06-14 12:00:00'), named_struct('t', CAST(NULL AS TIMESTAMP))) AS STRING) AS struct_null_second,
        CAST(array(named_struct('t', CAST(NULL AS TIMESTAMP)), named_struct('t', TIMESTAMP_NTZ '2020-06-14 12:00:00')) AS STRING) AS struct_null_first,
        unix_seconds(array(map('k', TIMESTAMP_NTZ '2020-06-14 12:00:00'), map('k', CAST(NULL AS TIMESTAMP)))[0]['k']) AS map_seconds,
        unix_seconds(array(named_struct('t', TIMESTAMP_NTZ '2020-06-14 12:00:00'), named_struct('t', CAST(NULL AS TIMESTAMP)))[0].t) AS struct_seconds
      """
    Then query result
      | map_null_second                           | map_null_first                            | map_array_value                               | struct_null_second              | struct_null_first               | map_seconds | struct_seconds |
      | [{k -> 2020-06-14 12:00:00}, {k -> null}] | [{k -> null}, {k -> 2020-06-14 12:00:00}] | [{k -> [2020-06-14 12:00:00]}, {k -> [null]}] | [{2020-06-14 12:00:00}, {null}] | [{null}, {2020-06-14 12:00:00}] | 1592161200  | 1592161200     |

  # A struct field that widens to TIMESTAMP converts through the DST gap while a TIMESTAMP_NTZ sibling is unchanged.
  Scenario: Struct fields widen independently of unchanged TIMESTAMP_NTZ siblings in a DST gap (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        named_struct('a', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00')
          = named_struct('a', TIMESTAMP '2020-03-08 03:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00') AS struct_equals,
        CAST(coalesce(
          named_struct('a', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00'),
          named_struct('a', TIMESTAMP '2020-03-08 03:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00')) AS STRING) AS struct_coalesce,
        unix_seconds(coalesce(
          named_struct('a', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00'),
          named_struct('a', TIMESTAMP '2020-03-08 03:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00')).a) AS struct_seconds,
        CAST(array(
          named_struct('a', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00'),
          named_struct('a', TIMESTAMP '2020-03-08 03:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00')) AS STRING) AS struct_array
      """
    Then query result
      | struct_equals | struct_coalesce                            | struct_seconds | struct_array                                                                             |
      | true          | {2020-03-08 03:30:00, 2020-01-01 00:00:00} | 1583663400     | [{2020-03-08 03:30:00, 2020-01-01 00:00:00}, {2020-03-08 03:30:00, 2020-01-01 00:00:00}] |

  Scenario: Unchanged TIMESTAMP_NTZ struct fields keep DST-gap wall-clock values (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        named_struct('a', TIMESTAMP_NTZ '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00')
          = named_struct('a', TIMESTAMP '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00') AS gap_sibling_equals,
        CAST(coalesce(
          named_struct('a', TIMESTAMP_NTZ '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00'),
          named_struct('a', TIMESTAMP '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00')) AS STRING) AS gap_sibling,
        named_struct('a', TIMESTAMP_NTZ '2020-11-01 01:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00')
          = named_struct('a', TIMESTAMP '2020-11-01 01:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00') AS overlap_equals,
        unix_seconds(coalesce(
          named_struct('a', TIMESTAMP_NTZ '2020-11-01 01:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00'),
          named_struct('a', TIMESTAMP '2020-11-01 01:30:00', 'b', TIMESTAMP_NTZ '2020-01-01 00:00:00')).a) AS overlap_seconds
      """
    Then query result
      | gap_sibling_equals | gap_sibling                                | overlap_equals | overlap_seconds |
      | true               | {2020-06-14 12:00:00, 2020-03-08 02:30:00} | true           | 1604219400      |

  # TRY_CAST of an out-of-range TIMESTAMP to TIMESTAMP_NTZ is NULL. Nullable input agrees with JVM Spark.
  Scenario: TRY_CAST of the largest TIMESTAMP to TIMESTAMP_NTZ is NULL in a positive-offset session (+05:30)
    Given variable location for temporary directory try_cast_timestamp_ntz_range
    Given final statement
      """
      DROP TABLE IF EXISTS try_cast_timestamp_ntz_range
      """
    Given statement template
      """
      CREATE TABLE try_cast_timestamp_ntz_range (id INT, m BIGINT) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO try_cast_timestamp_ntz_range VALUES (0, 0), (1, 9223372036854775807)
      """
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        TRY_CAST(timestamp_micros(m) AS TIMESTAMP_NTZ) IS NULL AS is_null,
        TRY_CAST(timestamp_micros(m) AS TIMESTAMP_NTZ) AS value,
        coalesce(TRY_CAST(timestamp_micros(m) AS TIMESTAMP_NTZ), TIMESTAMP_NTZ '1970-01-01 00:00:00') AS fallback
      FROM try_cast_timestamp_ntz_range
      ORDER BY id
      """
    Then query result ordered
      | id | is_null | value               | fallback            |
      | 0  | false   | 1970-01-01 05:30:00 | 1970-01-01 05:30:00 |
      | 1  | true    | NULL                | 1970-01-01 00:00:00 |

  # TRY_CAST with non-nullable input. JVM Spark 4.2 returns is_null = false and drops the COALESCE fallback here although
  # the value is NULL: Cast.nullable treats TIMESTAMP -> TIMESTAMP_NTZ TRY_CAST as non-nullable (Cast.scala:630-640,
  # UpCastRule.scala:42) and NullPropagation folds IS NULL to false (optimizer/expressions.scala:920). Sail intentionally
  # follows the value.
  @sail-only
  Scenario: TRY_CAST of the largest TIMESTAMP to TIMESTAMP_NTZ is NULL for non-nullable input (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        TRY_CAST(timestamp_micros(9223372036854775807) AS TIMESTAMP_NTZ) IS NULL AS literal_is_null,
        coalesce(TRY_CAST(timestamp_micros(9223372036854775807) AS TIMESTAMP_NTZ), TIMESTAMP_NTZ '1970-01-01 00:00:00') AS literal_fallback,
        TRY_CAST(timestamp_micros(id + 9223372036854775807) AS TIMESTAMP_NTZ) IS NULL AS column_is_null,
        coalesce(TRY_CAST(timestamp_micros(id + 9223372036854775807) AS TIMESTAMP_NTZ), TIMESTAMP_NTZ '1970-01-01 00:00:00') AS column_fallback
      FROM range(1)
      """
    Then query result
      | literal_is_null | literal_fallback    | column_is_null | column_fallback     |
      | true            | 1970-01-01 00:00:00 | true           | 1970-01-01 00:00:00 |

  # A struct field that mixes TIMESTAMP_NTZ with a Parquet TIMESTAMP converts in the session time zone even when a sibling
  # field widens too.
  Scenario: Struct fields mixing TIMESTAMP_NTZ and Parquet TIMESTAMP values convert next to widening siblings (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        unix_micros(array(named_struct('t', ntz, 'n', 1), named_struct('t', ltz, 'n', 1L))[0].t) AS array_ntz,
        unix_micros(coalesce(named_struct('t', ntz, 'n', 1), named_struct('t', ltz, 'n', 1L)).t) AS coalesce_ntz,
        unix_micros(map(1, named_struct('t', ntz, 'n', 1), 2, named_struct('t', ltz, 'n', 1L))[1].t) AS map_ntz,
        array_contains(array(named_struct('t', ntz, 'n', 1)), named_struct('t', ltz, 'n', 1L)) AS contains_ltz
      FROM timestamp_ntz_nested
      WHERE id IN (0, 5)
      ORDER BY id
      """
    Then query result ordered
      | id | array_ntz        | coalesce_ntz     | map_ntz          | contains_ltz |
      | 0  | 1592107200000000 | 1592107200000000 | 1592107200000000 | false        |
      | 5  | 1592161200000000 | 1592161200000000 | 1592161200000000 | true         |

  # A TIMESTAMP_NTZ struct field that is TIMESTAMP_NTZ on both sides stays TIMESTAMP_NTZ next to a mixed field and a
  # widening sibling.
  Scenario: Unchanged TIMESTAMP_NTZ struct fields stay TIMESTAMP_NTZ next to mixed fields and widening siblings (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        CAST(coalesce(
          named_struct('a', TIMESTAMP_NTZ '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'n', 1),
          named_struct('a', TIMESTAMP '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'n', 1L)) AS STRING) AS coalesce_value,
        CAST(array(
          named_struct('a', TIMESTAMP_NTZ '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'n', 1),
          named_struct('a', TIMESTAMP '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'n', 1L)) AS STRING) AS array_value,
        array_contains(
          array(named_struct('a', TIMESTAMP_NTZ '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 02:30:00', 'n', 1)),
          named_struct('a', TIMESTAMP '2020-06-14 12:00:00', 'b', TIMESTAMP_NTZ '2020-03-08 03:30:00', 'n', 1L)) AS contains_other
      """
    Then query result
      | coalesce_value                                 | array_value                                                                                        | contains_other |
      | {2020-06-14 12:00:00, 2020-03-08 02:30:00, 1}  | [{2020-06-14 12:00:00, 2020-03-08 02:30:00, 1}, {2020-06-14 12:00:00, 2020-03-08 02:30:00, 1}]     | false          |
