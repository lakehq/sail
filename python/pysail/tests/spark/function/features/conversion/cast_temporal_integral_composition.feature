@spark-4.2
Feature: Temporal integral casts in conditional, nested and zoned expressions

  Scenario: Temporal integral composition time unselected overflow ANSI true schema
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIME '23:59:59' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """

  Scenario: Temporal integral composition time unselected overflow ANSI true value
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIME '23:59:59' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query result ordered
      |result|
      |7     |

  Scenario: Temporal integral composition time row selection ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN flag THEN CAST(v AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result FROM VALUES (false,TIME '23:59:59'), (true,TIME '00:00:01.5') AS t(flag,v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |7     |
      |1     |

  Scenario: Temporal integral composition time unselected overflow ANSI false schema
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIME '23:59:59' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """

  Scenario: Temporal integral composition time unselected overflow ANSI false value
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIME '23:59:59' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query result ordered
      |result|
      |7     |

  Scenario: Temporal integral composition time row selection ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN flag THEN CAST(v AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result FROM VALUES (false,TIME '23:59:59'), (true,TIME '00:00:01.5') AS t(flag,v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |7     |
      |1     |

  Scenario: Temporal integral composition time nested struct
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(named_struct('x',named_struct('y',TIME '00:00:01.5')).x.y AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |1     |

  Scenario: Temporal integral composition time JOIN duplicate names
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(l.v AS BIGINT) AS a, CAST(r.v AS BIGINT) AS b FROM VALUES (TIME '00:00:01.5') AS l(v) CROSS JOIN VALUES (TIME '23:59:59') AS r(v)
      """
    Then query schema
      """
      root
       |-- a: long (nullable = false)
       |-- b: long (nullable = false)
      """
    Then query result ordered
      |a  |b    |
      |1  |86399|

  Scenario: Temporal integral composition timestamp unselected overflow ANSI true schema
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIMESTAMP '2024-01-01 00:00:00' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """

  Scenario: Temporal integral composition timestamp unselected overflow ANSI true value
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIMESTAMP '2024-01-01 00:00:00' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query result ordered
      |result|
      |7     |

  Scenario: Temporal integral composition timestamp row selection ANSI true
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN flag THEN CAST(v AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result FROM VALUES (false,TIMESTAMP '2024-01-01 00:00:00'), (true,TIMESTAMP '1969-12-31 23:59:59.999999') AS t(flag,v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |7     |
      |-1    |

  Scenario: Temporal integral composition timestamp unselected overflow ANSI false schema
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIMESTAMP '2024-01-01 00:00:00' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """

  Scenario: Temporal integral composition timestamp unselected overflow ANSI false value
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN false THEN CAST(TIMESTAMP '2024-01-01 00:00:00' AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result
      """
    Then query result ordered
      |result|
      |7     |

  Scenario: Temporal integral composition timestamp row selection ANSI false
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CASE WHEN flag THEN CAST(v AS TINYINT) ELSE CAST(7 AS TINYINT) END AS result FROM VALUES (false,TIMESTAMP '2024-01-01 00:00:00'), (true,TIMESTAMP '1969-12-31 23:59:59.999999') AS t(flag,v)
      """
    Then query schema
      """
      root
       |-- result: byte (nullable = true)
      """
    Then query result ordered
      |result|
      |7     |
      |-1    |

  Scenario: Temporal integral composition timestamp nested struct
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(named_struct('x',named_struct('y',TIMESTAMP '1969-12-31 23:59:59.999999')).x.y AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-1    |

  Scenario: Temporal integral composition timestamp JOIN duplicate names
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(l.v AS BIGINT) AS a, CAST(r.v AS BIGINT) AS b FROM VALUES (TIMESTAMP '1969-12-31 23:59:59.999999') AS l(v) CROSS JOIN VALUES (TIMESTAMP '2024-01-01 00:00:00') AS r(v)
      """
    Then query schema
      """
      root
       |-- a: long (nullable = false)
       |-- b: long (nullable = false)
      """
    Then query result ordered
      |a  |b         |
      |-1 |1704067200|

  Scenario: Temporal integral composition timestamp local wall clock America/Los_Angeles
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = America/Los_Angeles
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIMESTAMP '1970-01-01 00:00:00.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |28800 |

  Scenario: Temporal integral composition timestamp epoch invariant America/Los_Angeles
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = America/Los_Angeles
    And config spark.sql.timeType.enabled = true
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

  Scenario: Temporal integral composition timestamp overflow message America/Los_Angeles
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = America/Los_Angeles
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIMESTAMP '1970-01-01 00:00:00.999999' AS TINYINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1970\-01\-01\ 00:00:00\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "TINYINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.

  Scenario: Temporal integral composition timestamp local wall clock Asia/Kolkata
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = Asia/Kolkata
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIMESTAMP '1970-01-01 00:00:00.999999' AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |-19800|

  Scenario: Temporal integral composition timestamp epoch invariant Asia/Kolkata
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = Asia/Kolkata
    And config spark.sql.timeType.enabled = true
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

  Scenario: Temporal integral composition timestamp overflow message Asia/Kolkata
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = Asia/Kolkata
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(TIMESTAMP '1970-01-01 00:00:00.999999' AS TINYINT) AS result
      """
    Then query error \[CAST_OVERFLOW\]\ The\ value\ TIMESTAMP\ '1970\-01\-01\ 00:00:00\.999999'\ of\ the\ type\ "TIMESTAMP"\ cannot\ be\ cast\ to\ "TINYINT"\ due\ to\ an\ overflow\.\ Use\ `try_cast`\ to\ tolerate\ overflow\ and\ return\ NULL\ instead\.
