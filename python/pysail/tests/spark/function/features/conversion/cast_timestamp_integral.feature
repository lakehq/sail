Feature: Exact TIMESTAMP to integral casts

  Scenario: Temporal integral timestamp TINYINT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp TINYINT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS TINYINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp SMALLINT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp SMALLINT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS SMALLINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp INT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp INT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS INT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp BIGINT CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp BIGINT CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS BIGINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp TINYINT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp TINYINT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS TINYINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp SMALLINT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp SMALLINT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS SMALLINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp INT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp INT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS INT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp BIGINT TRY_CAST literal ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp BIGINT TRY_CAST column ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS BIGINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp boundary -129 TINYINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-128000001L) AS TINYINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1969\-12\-31\ 23:57:51\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "TINYINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral timestamp boundary -129 TINYINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-128000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -128 TINYINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-127000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-128  |

  Scenario: Temporal integral timestamp boundary -128 TINYINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-127000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-128  |

  Scenario: Temporal integral timestamp boundary 127 TINYINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(127999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral timestamp boundary 127 TINYINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(127999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral timestamp boundary 128 TINYINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(128999999L) AS TINYINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1970\-01\-01\ 00:02:08\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "TINYINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral timestamp boundary 128 TINYINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(128999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -32769 SMALLINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-32768000001L) AS SMALLINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1969\-12\-31\ 14:53:51\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "SMALLINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral timestamp boundary -32769 SMALLINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-32768000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -32768 SMALLINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-32767000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-32768|

  Scenario: Temporal integral timestamp boundary -32768 SMALLINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-32767000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-32768|

  Scenario: Temporal integral timestamp boundary 32767 SMALLINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(32767999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral timestamp boundary 32767 SMALLINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(32767999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral timestamp boundary 32768 SMALLINT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(32768999999L) AS SMALLINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1970\-01\-01\ 09:06:08\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "SMALLINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral timestamp boundary 32768 SMALLINT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(32768999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -2147483649 INT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-2147483648000001L) AS INT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1901\-12\-13\ 20:45:51\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "INT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral timestamp boundary -2147483649 INT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-2147483648000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -2147483648 INT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-2147483647000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result     |
      |-2147483648|

  Scenario: Temporal integral timestamp boundary -2147483648 INT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-2147483647000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result     |
      |-2147483648|

  Scenario: Temporal integral timestamp boundary 2147483647 INT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(2147483647999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result    |
      |2147483647|

  Scenario: Temporal integral timestamp boundary 2147483647 INT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(2147483647999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result    |
      |2147483647|

  Scenario: Temporal integral timestamp boundary 2147483648 INT CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(2147483648999999L) AS INT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '2038\-01\-19\ 03:14:08\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "INT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral timestamp boundary 2147483648 INT TRY_CAST ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(2147483648999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp exact micros -9223372036854775808 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros((-9223372036854775807L - 1L)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result        |
      |-9223372036855|

  Scenario: Temporal integral timestamp exact micros -9007199254000001 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-9007199254000001L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result     |
      |-9007199255|

  Scenario: Temporal integral timestamp exact micros -1000001 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-1000001L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp exact micros -1 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-1L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-1    |

  Scenario: Temporal integral timestamp exact micros 0 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(0L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |0     |

  Scenario: Temporal integral timestamp exact micros 1 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(1L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |0     |

  Scenario: Temporal integral timestamp exact micros 9007199254999999 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(9007199254999999L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result    |
      |9007199254|

  Scenario: Temporal integral timestamp exact micros 9223372036854775807 ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(9223372036854775807L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result       |
      |9223372036854|

  Scenario: Temporal integral timestamp TINYINT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp TINYINT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS TINYINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp SMALLINT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp SMALLINT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS SMALLINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp INT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp INT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS INT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp BIGINT CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp BIGINT CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(v AS BIGINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp TINYINT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp TINYINT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS TINYINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp SMALLINT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp SMALLINT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS SMALLINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp INT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp INT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS INT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp BIGINT TRY_CAST literal ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp BIGINT TRY_CAST column ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(v AS BIGINT) AS result FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999'), (TIMESTAMP '1969-12-31 23:59:58.999999'), (TIMESTAMP '1970-01-01 00:00:00.999999'), (TIMESTAMP '1970-01-01 00:00:01.999999'), (CAST(NULL AS TIMESTAMP)) AS t(v)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = true)
      """
    Then query result ordered
      |result|
      |-1    |
      |-2    |
      |0     |
      |1     |
      |NULL  |

  Scenario: Temporal integral timestamp boundary -129 TINYINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-128000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -129 TINYINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-128000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -128 TINYINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-127000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-128  |

  Scenario: Temporal integral timestamp boundary -128 TINYINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-127000001L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |-128  |

  Scenario: Temporal integral timestamp boundary 127 TINYINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(127999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral timestamp boundary 127 TINYINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(127999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |127   |

  Scenario: Temporal integral timestamp boundary 128 TINYINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(128999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary 128 TINYINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(128999999L) AS TINYINT) AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -32769 SMALLINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-32768000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -32769 SMALLINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-32768000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -32768 SMALLINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-32767000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-32768|

  Scenario: Temporal integral timestamp boundary -32768 SMALLINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-32767000001L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |-32768|

  Scenario: Temporal integral timestamp boundary 32767 SMALLINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(32767999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral timestamp boundary 32767 SMALLINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(32767999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |32767 |

  Scenario: Temporal integral timestamp boundary 32768 SMALLINT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(32768999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary 32768 SMALLINT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(32768999999L) AS SMALLINT) AS result
      """
    Then query schema
      """
      root
       |-- result: short (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -2147483649 INT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-2147483648000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -2147483649 INT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-2147483648000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary -2147483648 INT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-2147483647000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result     |
      |-2147483648|

  Scenario: Temporal integral timestamp boundary -2147483648 INT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(-2147483647000001L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result     |
      |-2147483648|

  Scenario: Temporal integral timestamp boundary 2147483647 INT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(2147483647999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result    |
      |2147483647|

  Scenario: Temporal integral timestamp boundary 2147483647 INT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(2147483647999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result    |
      |2147483647|

  Scenario: Temporal integral timestamp boundary 2147483648 INT CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(2147483648999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp boundary 2147483648 INT TRY_CAST ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT TRY_CAST(timestamp_micros(2147483648999999L) AS INT) AS result
      """
    Then query schema
      """
      root
       |-- result: integer (nullable = true)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Temporal integral timestamp exact micros -9223372036854775808 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros((-9223372036854775807L - 1L)) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result        |
      |-9223372036855|

  Scenario: Temporal integral timestamp exact micros -9007199254000001 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-9007199254000001L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result     |
      |-9007199255|

  Scenario: Temporal integral timestamp exact micros -1000001 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-1000001L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-2    |

  Scenario: Temporal integral timestamp exact micros -1 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(-1L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-1    |

  Scenario: Temporal integral timestamp exact micros 0 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(0L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |0     |

  Scenario: Temporal integral timestamp exact micros 1 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(1L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |0     |

  Scenario: Temporal integral timestamp exact micros 9007199254999999 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(9007199254999999L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result    |
      |9007199254|

  Scenario: Temporal integral timestamp exact micros 9223372036854775807 ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(timestamp_micros(9223372036854775807L) AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result       |
      |9223372036854|
