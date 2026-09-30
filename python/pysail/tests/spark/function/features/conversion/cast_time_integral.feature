@spark-4.2
Feature: Exact TIME to integral casts

  Scenario: Temporal integral time TINYINT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time TINYINT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS TINYINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIME\ '09:05:03\.5'\ of\ the\ type\ "TIME\(6\)"\ cannot\ be\ cast\ to\ "TINYINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral time SMALLINT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time SMALLINT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS SMALLINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIME\ '23:59:59\.999999'\ of\ the\ type\ "TIME\(6\)"\ cannot\ be\ cast\ to\ "SMALLINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral time INT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = false)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time INT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS INT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time BIGINT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time BIGINT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS BIGINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time TINYINT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time TINYINT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS TINYINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |NULL  |
      |NULL  |
      |NULL  |

  Scenario: Temporal integral time SMALLINT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time SMALLINT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS SMALLINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |NULL  |
      |NULL  |

  Scenario: Temporal integral time INT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time INT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS INT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time BIGINT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time BIGINT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS BIGINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time boundary 127 TINYINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:02:07.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral time boundary 127 TINYINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:02:07.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral time boundary 128 TINYINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:02:08.999999' AS TINYINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIME\ '00:02:08\.999999'\ of\ the\ type\ "TIME\(6\)"\ cannot\ be\ cast\ to\ "TINYINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral time boundary 128 TINYINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:02:08.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral time boundary 32767 SMALLINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '09:06:07.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral time boundary 32767 SMALLINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '09:06:07.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral time boundary 32768 SMALLINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '09:06:08.999999' AS SMALLINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIME\ '09:06:08\.999999'\ of\ the\ type\ "TIME\(6\)"\ cannot\ be\ cast\ to\ "SMALLINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral time boundary 32768 SMALLINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '09:06:08.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral time precision 0 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(CAST(TIME '12:34:56.987654' AS TIME(0)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |45296 |

  Scenario: Temporal integral time precision 3 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(CAST(TIME '12:34:56.987654' AS TIME(3)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |45296 |

  Scenario: Temporal integral time precision 6 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(CAST(TIME '12:34:56.987654' AS TIME(6)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |45296 |

  Scenario: Temporal integral time TINYINT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time TINYINT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS TINYINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |NULL  |
      |NULL  |
      |NULL  |

  Scenario: Temporal integral time SMALLINT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time SMALLINT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS SMALLINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |NULL  |
      |NULL  |

  Scenario: Temporal integral time INT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = false)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time INT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS INT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time BIGINT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:00:01.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time BIGINT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(v AS BIGINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time TINYINT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time TINYINT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS TINYINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |NULL  |
      |NULL  |
      |NULL  |

  Scenario: Temporal integral time SMALLINT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time SMALLINT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS SMALLINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |NULL  |
      |NULL  |

  Scenario: Temporal integral time INT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time INT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS INT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time BIGINT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:00:01.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral time BIGINT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(v AS BIGINT) AS result FROM VALUES (TIME '00:00:00'), (TIME '00:00:01.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (CAST(NULL AS TIME)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |0     |
      |1     |
      |32703 |
      |86399 |
      |NULL  |

  Scenario: Temporal integral time boundary 127 TINYINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:02:07.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral time boundary 127 TINYINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:02:07.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral time boundary 128 TINYINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '00:02:08.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral time boundary 128 TINYINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '00:02:08.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral time boundary 32767 SMALLINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '09:06:07.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral time boundary 32767 SMALLINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '09:06:07.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral time boundary 32768 SMALLINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIME '09:06:08.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral time boundary 32768 SMALLINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TRY_CAST(TIME '09:06:08.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral time precision 0 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(CAST(TIME '12:34:56.987654' AS TIME(0)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |45296 |

  Scenario: Temporal integral time precision 3 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(CAST(TIME '12:34:56.987654' AS TIME(3)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |45296 |

  Scenario: Temporal integral time precision 6 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(CAST(TIME '12:34:56.987654' AS TIME(6)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |45296 |
