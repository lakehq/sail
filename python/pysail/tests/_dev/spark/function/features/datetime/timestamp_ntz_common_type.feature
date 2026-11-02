Feature: TIMESTAMP_NTZ mixed with TIMESTAMP uses the session time zone
  Spark casts TIMESTAMP_NTZ values mixed with TIMESTAMP values to TIMESTAMP in the session time zone.
  The Parquet table is written in UTC, so its TIMESTAMP column is labeled with UTC rather than the session time zone.

  Background:
    Given variable location for temporary directory timestamp_ntz_common_type
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_common_type
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_common_type (id INT, ntz TIMESTAMP_NTZ, ltz TIMESTAMP) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_common_type VALUES
        (0, TIMESTAMP_NTZ '2020-03-08 02:30:00', NULL),
        (1, NULL, TIMESTAMP '2020-06-14 04:00:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 08:30:00')
      """

  Scenario: Functions return TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        CAST(coalesce(ntz, ltz) AS STRING) AS coalesce,
        CAST(nvl(ltz, ntz) AS STRING) AS nvl,
        CAST(ifnull(ntz, ltz) AS STRING) AS ifnull,
        CAST(nvl2(ntz, ntz, ltz) AS STRING) AS nvl2,
        CAST(CASE WHEN id = 1 THEN ltz ELSE ntz END AS STRING) AS case_when,
        CAST(if(id = 1, ltz, ntz) AS STRING) AS if_else,
        CAST(greatest(ntz, ltz) AS STRING) AS greatest,
        CAST(least(ntz, ltz) AS STRING) AS least,
        CAST(element_at(array(ntz, ltz), if(id = 1, 2, 1)) AS STRING) AS array,
        CAST(map(0, ntz, 1, ltz)[if(id = 1, 1, 0)] AS STRING) AS map
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | coalesce            | nvl                 | ifnull              | nvl2                | case_when           | if_else             | greatest            | least               | array               | map                 |
      | 0  | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 |
      | 1  | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 | 2020-06-14 12:00:00 |
      | 2  | 2020-11-01 01:30:00 | 2020-11-01 16:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 16:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 |

  Scenario: Functions return TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(coalesce(ntz, ltz) AS STRING) AS coalesce,
        CAST(nvl(ltz, ntz) AS STRING) AS nvl,
        CAST(ifnull(ntz, ltz) AS STRING) AS ifnull,
        CAST(nvl2(ntz, ntz, ltz) AS STRING) AS nvl2,
        CAST(CASE WHEN id = 1 THEN ltz ELSE ntz END AS STRING) AS case_when,
        CAST(if(id = 1, ltz, ntz) AS STRING) AS if_else,
        CAST(greatest(ntz, ltz) AS STRING) AS greatest,
        CAST(least(ntz, ltz) AS STRING) AS least,
        CAST(element_at(array(ntz, ltz), if(id = 1, 2, 1)) AS STRING) AS array,
        CAST(map(0, ntz, 1, ltz)[if(id = 1, 1, 0)] AS STRING) AS map
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | coalesce            | nvl                 | ifnull              | nvl2                | case_when           | if_else             | greatest            | least               | array               | map                 |
      | 0  | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 |
      | 1  | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 | 2020-06-13 21:00:00 |
      | 2  | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 |

  Scenario: Functions return TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        CAST(coalesce(ntz, ltz) AS STRING) AS coalesce,
        CAST(nvl(ltz, ntz) AS STRING) AS nvl,
        CAST(ifnull(ntz, ltz) AS STRING) AS ifnull,
        CAST(nvl2(ntz, ntz, ltz) AS STRING) AS nvl2,
        CAST(CASE WHEN id = 1 THEN ltz ELSE ntz END AS STRING) AS case_when,
        CAST(if(id = 1, ltz, ntz) AS STRING) AS if_else,
        CAST(greatest(ntz, ltz) AS STRING) AS greatest,
        CAST(least(ntz, ltz) AS STRING) AS least,
        CAST(element_at(array(ntz, ltz), if(id = 1, 2, 1)) AS STRING) AS array,
        CAST(map(0, ntz, 1, ltz)[if(id = 1, 1, 0)] AS STRING) AS map
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | coalesce            | nvl                 | ifnull              | nvl2                | case_when           | if_else             | greatest            | least               | array               | map                 |
      | 0  | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 |
      | 1  | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 | 2020-06-14 09:30:00 |
      | 2  | 2020-11-01 01:30:00 | 2020-11-01 14:00:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 14:00:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 |

  Scenario: Functions return TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        CAST(coalesce(ntz, ltz) AS STRING) AS coalesce,
        CAST(nvl(ltz, ntz) AS STRING) AS nvl,
        CAST(ifnull(ntz, ltz) AS STRING) AS ifnull,
        CAST(nvl2(ntz, ntz, ltz) AS STRING) AS nvl2,
        CAST(CASE WHEN id = 1 THEN ltz ELSE ntz END AS STRING) AS case_when,
        CAST(if(id = 1, ltz, ntz) AS STRING) AS if_else,
        CAST(greatest(ntz, ltz) AS STRING) AS greatest,
        CAST(least(ntz, ltz) AS STRING) AS least,
        CAST(element_at(array(ntz, ltz), if(id = 1, 2, 1)) AS STRING) AS array,
        CAST(map(0, ntz, 1, ltz)[if(id = 1, 1, 0)] AS STRING) AS map
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | coalesce            | nvl                 | ifnull              | nvl2                | case_when           | if_else             | greatest            | least               | array               | map                 |
      | 0  | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 | 2020-03-08 02:30:00 |
      | 1  | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 |
      | 2  | 2020-11-01 01:30:00 | 2020-11-01 08:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 08:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 | 2020-11-01 01:30:00 |

  Scenario: Functions read TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        typeof(coalesce(ntz, ltz)) AS type,
        hour(coalesce(ntz, ltz)) AS hour,
        CAST(CAST(coalesce(ntz, ltz) AS TIMESTAMP_NTZ) AS STRING) AS ntz,
        CAST(coalesce(ntz, ltz) AS DATE) AS date,
        date_format(coalesce(ntz, ltz), 'yyyy-MM-dd HH:mm') AS formatted,
        unix_micros(coalesce(ntz, ltz)) AS micros,
        timestampdiff(MONTH, coalesce(ntz, ltz), TIMESTAMP '2021-06-01 00:00:00') AS months,
        coalesce(ntz, ltz) = ntz AS equals_ntz,
        ntz IN (coalesce(ntz, ltz)) AS in_list,
        array_contains(array(coalesce(ntz, ltz)), coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00')) AS array_contains
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | type      | hour | ntz                 | date       | formatted        | micros           | months | equals_ntz | in_list | array_contains |
      | 0  | timestamp | 2    | 2020-03-08 02:30:00 | 2020-03-08 | 2020-03-08 02:30 | 1583605800000000 | 14     | true       | true    | true           |
      | 1  | timestamp | 12   | 2020-06-14 12:00:00 | 2020-06-14 | 2020-06-14 12:00 | 1592107200000000 | 11     | NULL       | NULL    | false          |
      | 2  | timestamp | 1    | 2020-11-01 01:30:00 | 2020-11-01 | 2020-11-01 01:30 | 1604165400000000 | 6      | true       | true    | true           |

  Scenario: Functions read TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        typeof(coalesce(ntz, ltz)) AS type,
        hour(coalesce(ntz, ltz)) AS hour,
        CAST(CAST(coalesce(ntz, ltz) AS TIMESTAMP_NTZ) AS STRING) AS ntz,
        CAST(coalesce(ntz, ltz) AS DATE) AS date,
        date_format(coalesce(ntz, ltz), 'yyyy-MM-dd HH:mm') AS formatted,
        unix_micros(coalesce(ntz, ltz)) AS micros,
        timestampdiff(MONTH, coalesce(ntz, ltz), TIMESTAMP '2021-06-01 00:00:00') AS months,
        coalesce(ntz, ltz) = ntz AS equals_ntz,
        ntz IN (coalesce(ntz, ltz)) AS in_list,
        array_contains(array(coalesce(ntz, ltz)), coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00')) AS array_contains
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | type      | hour | ntz                 | date       | formatted        | micros           | months | equals_ntz | in_list | array_contains |
      | 0  | timestamp | 3    | 2020-03-08 03:30:00 | 2020-03-08 | 2020-03-08 03:30 | 1583663400000000 | 14     | true       | true    | true           |
      | 1  | timestamp | 21   | 2020-06-13 21:00:00 | 2020-06-13 | 2020-06-13 21:00 | 1592107200000000 | 11     | NULL       | NULL    | false          |
      | 2  | timestamp | 1    | 2020-11-01 01:30:00 | 2020-11-01 | 2020-11-01 01:30 | 1604219400000000 | 6      | true       | true    | true           |

  Scenario: Functions read TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        typeof(coalesce(ntz, ltz)) AS type,
        hour(coalesce(ntz, ltz)) AS hour,
        CAST(CAST(coalesce(ntz, ltz) AS TIMESTAMP_NTZ) AS STRING) AS ntz,
        CAST(coalesce(ntz, ltz) AS DATE) AS date,
        date_format(coalesce(ntz, ltz), 'yyyy-MM-dd HH:mm') AS formatted,
        unix_micros(coalesce(ntz, ltz)) AS micros,
        timestampdiff(MONTH, coalesce(ntz, ltz), TIMESTAMP '2021-06-01 00:00:00') AS months,
        coalesce(ntz, ltz) = ntz AS equals_ntz,
        ntz IN (coalesce(ntz, ltz)) AS in_list,
        array_contains(array(coalesce(ntz, ltz)), coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00')) AS array_contains
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | type      | hour | ntz                 | date       | formatted        | micros           | months | equals_ntz | in_list | array_contains |
      | 0  | timestamp | 2    | 2020-03-08 02:30:00 | 2020-03-08 | 2020-03-08 02:30 | 1583614800000000 | 14     | true       | true    | true           |
      | 1  | timestamp | 9    | 2020-06-14 09:30:00 | 2020-06-14 | 2020-06-14 09:30 | 1592107200000000 | 11     | NULL       | NULL    | false          |
      | 2  | timestamp | 1    | 2020-11-01 01:30:00 | 2020-11-01 | 2020-11-01 01:30 | 1604174400000000 | 6      | true       | true    | true           |

  Scenario: Functions read TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        typeof(coalesce(ntz, ltz)) AS type,
        hour(coalesce(ntz, ltz)) AS hour,
        CAST(CAST(coalesce(ntz, ltz) AS TIMESTAMP_NTZ) AS STRING) AS ntz,
        CAST(coalesce(ntz, ltz) AS DATE) AS date,
        date_format(coalesce(ntz, ltz), 'yyyy-MM-dd HH:mm') AS formatted,
        unix_micros(coalesce(ntz, ltz)) AS micros,
        timestampdiff(MONTH, coalesce(ntz, ltz), TIMESTAMP '2021-06-01 00:00:00') AS months,
        coalesce(ntz, ltz) = ntz AS equals_ntz,
        ntz IN (coalesce(ntz, ltz)) AS in_list,
        array_contains(array(coalesce(ntz, ltz)), coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00')) AS array_contains
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | type      | hour | ntz                 | date       | formatted        | micros           | months | equals_ntz | in_list | array_contains |
      | 0  | timestamp | 2    | 2020-03-08 02:30:00 | 2020-03-08 | 2020-03-08 02:30 | 1583634600000000 | 14     | true       | true    | true           |
      | 1  | timestamp | 4    | 2020-06-14 04:00:00 | 2020-06-14 | 2020-06-14 04:00 | 1592107200000000 | 11     | NULL       | NULL    | false          |
      | 2  | timestamp | 1    | 2020-11-01 01:30:00 | 2020-11-01 | 2020-11-01 01:30 | 1604194200000000 | 6      | true       | true    | true           |

  Scenario: Set operations cast TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT 'union' AS op, CAST(ts AS STRING) AS ts FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 0
        UNION ALL SELECT ltz FROM timestamp_ntz_common_type WHERE id = 1)
      UNION ALL
      SELECT 'intersect', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        INTERSECT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      UNION ALL
      SELECT 'except', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        EXCEPT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      ORDER BY op, ts
      """
    Then query result
      | op     | ts                  |
      | except | 2020-11-01 01:30:00 |
      | union  | 2020-03-08 02:30:00 |
      | union  | 2020-06-14 12:00:00 |

  Scenario: Set operations cast TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT 'union' AS op, CAST(ts AS STRING) AS ts FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 0
        UNION ALL SELECT ltz FROM timestamp_ntz_common_type WHERE id = 1)
      UNION ALL
      SELECT 'intersect', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        INTERSECT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      UNION ALL
      SELECT 'except', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        EXCEPT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      ORDER BY op, ts
      """
    Then query result
      | op        | ts                  |
      | intersect | 2020-11-01 01:30:00 |
      | union     | 2020-03-08 03:30:00 |
      | union     | 2020-06-13 21:00:00 |

  Scenario: Set operations cast TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT 'union' AS op, CAST(ts AS STRING) AS ts FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 0
        UNION ALL SELECT ltz FROM timestamp_ntz_common_type WHERE id = 1)
      UNION ALL
      SELECT 'intersect', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        INTERSECT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      UNION ALL
      SELECT 'except', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        EXCEPT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      ORDER BY op, ts
      """
    Then query result
      | op     | ts                  |
      | except | 2020-11-01 01:30:00 |
      | union  | 2020-03-08 02:30:00 |
      | union  | 2020-06-14 09:30:00 |

  Scenario: Set operations cast TIMESTAMP_NTZ mixed with TIMESTAMP in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT 'union' AS op, CAST(ts AS STRING) AS ts FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 0
        UNION ALL SELECT ltz FROM timestamp_ntz_common_type WHERE id = 1)
      UNION ALL
      SELECT 'intersect', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        INTERSECT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      UNION ALL
      SELECT 'except', CAST(ts AS STRING) FROM (
        SELECT ntz AS ts FROM timestamp_ntz_common_type WHERE id = 2
        EXCEPT SELECT ltz FROM timestamp_ntz_common_type WHERE id = 2)
      ORDER BY op, ts
      """
    Then query result
      | op     | ts                  |
      | except | 2020-11-01 01:30:00 |
      | union  | 2020-03-08 02:30:00 |
      | union  | 2020-06-14 04:00:00 |

  Scenario: NULLIF returns the type of its first argument (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        typeof(nullif(ntz, ltz)) AS ntz_first,
        typeof(nullif(ltz, ntz)) AS ltz_first,
        CAST(nullif(ntz, ltz) AS STRING) AS value
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | ntz_first     | ltz_first | value               |
      | 0  | timestamp_ntz | timestamp | 2020-03-08 02:30:00 |
      | 1  | timestamp_ntz | timestamp | NULL                |
      | 2  | timestamp_ntz | timestamp | 2020-11-01 01:30:00 |

  Scenario: NULLIF returns the type of its first argument (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        typeof(nullif(ntz, ltz)) AS ntz_first,
        typeof(nullif(ltz, ntz)) AS ltz_first,
        CAST(nullif(ntz, ltz) AS STRING) AS value
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | ntz_first     | ltz_first | value               |
      | 0  | timestamp_ntz | timestamp | 2020-03-08 02:30:00 |
      | 1  | timestamp_ntz | timestamp | NULL                |
      | 2  | timestamp_ntz | timestamp | NULL                |

  Scenario: NULLIF returns the type of its first argument (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        typeof(nullif(ntz, ltz)) AS ntz_first,
        typeof(nullif(ltz, ntz)) AS ltz_first,
        CAST(nullif(ntz, ltz) AS STRING) AS value
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | ntz_first     | ltz_first | value               |
      | 0  | timestamp_ntz | timestamp | 2020-03-08 02:30:00 |
      | 1  | timestamp_ntz | timestamp | NULL                |
      | 2  | timestamp_ntz | timestamp | 2020-11-01 01:30:00 |

  Scenario: NULLIF returns the type of its first argument (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        typeof(nullif(ntz, ltz)) AS ntz_first,
        typeof(nullif(ltz, ntz)) AS ltz_first,
        CAST(nullif(ntz, ltz) AS STRING) AS value
      FROM timestamp_ntz_common_type
      ORDER BY id
      """
    Then query result
      | id | ntz_first     | ltz_first | value               |
      | 0  | timestamp_ntz | timestamp | 2020-03-08 02:30:00 |
      | 1  | timestamp_ntz | timestamp | NULL                |
      | 2  | timestamp_ntz | timestamp | 2020-11-01 01:30:00 |

  Scenario: NVL2 returns the common type of other values (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        typeof(nvl2(id, 1, CAST(2 AS BIGINT))) AS bigint,
        typeof(nvl2(id, 1, 2.5)) AS decimal
      FROM timestamp_ntz_common_type
      WHERE id = 0
      """
    Then query result
      | bigint | decimal       |
      | bigint | decimal(11,1) |
