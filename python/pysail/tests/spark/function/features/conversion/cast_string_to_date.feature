Feature: Casting a STRING to DATE follows Spark's stringToDate

  # SparkDateTimeUtils.stringToDate (SparkDateTimeUtils.scala:395-458) trims whitespace and ISO
  # control characters (bytes 0x00-0x20 and 0x7F) at both ends, reads `[+-]yyyy*[-[m]m[-[d]d]]`
  # and, once the day has been read, ends the date at the first ' ' or 'T' and ignores the rest.
  # After only the year or the year and month the whole input must be consumed. Any other
  # character (a tab or a newline included, 'Z', a lowercase 't') makes it NULL. CAST, TRY_CAST
  # and to_date(col) without a format share this one rule.

  Rule: Strings stringToDate accepts or rejects, ANSI off

    Scenario Outline: STRING to DATE with ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(<input> AS DATE) AS cast_result,
               TRY_CAST(<input> AS DATE) AS try_cast_result,
               to_date(<input>) AS to_date_result
        """
      Then query result
        | cast_result | try_cast_result | to_date_result |
        | <result>    | <result>        | <result>       |

      Examples:
        | case                          | input                                                            | result     |
        | day                           | '2023-01-31'                                                     | 2023-01-31 |
        | 1-digit month/day             | '2023-1-5'                                                       | 2023-01-05 |
        | year-month                    | '2023-01'                                                        | 2023-01-01 |
        | year only                     | '2023'                                                           | 2023-01-01 |
        | plus sign                     | '+2023-01-31'                                                    | 2023-01-31 |
        | year 8 digits                 | '12345678-01-31'                                                 | NULL       |
        | year 3 digits                 | '123-01-31'                                                      | NULL       |
        | day + space + text            | '2023-01-31 x'                                                   | 2023-01-31 |
        | day + space                   | '2023-01-31 '                                                    | 2023-01-31 |
        | day + two spaces + text       | '2023-01-31  x'                                                  | 2023-01-31 |
        | day + T                       | '2023-01-31T'                                                    | 2023-01-31 |
        | day + T + text                | '2023-01-31Tx'                                                   | 2023-01-31 |
        | day + T + time                | '2023-01-31T13:45:00.123'                                        | 2023-01-31 |
        | day + T + offset              | '2023-01-31T01:30:00+02:00'                                      | 2023-01-31 |
        | 1-digit + T + time            | '2023-1-5T10:00:00'                                              | 2023-01-05 |
        | day + space + time            | '2023-01-31 13:45:00'                                            | 2023-01-31 |
        | day + lowercase t             | '2023-01-31t10:00:00'                                            | NULL       |
        | day + Z                       | '2023-01-31Z'                                                    | NULL       |
        | day + x                       | '2023-01-31x'                                                    | NULL       |
        | day + dash                    | '2023-01-31-05'                                                  | NULL       |
        | day + dot                     | '2023-01-31.5'                                                   | NULL       |
        | day + colon                   | '2023-01-31:00'                                                  | NULL       |
        | day + tab + time              | concat('2023-01-31', chr(9), '10:00:00')                         | NULL       |
        | day + newline + time          | concat('2023-01-31', chr(10), '10:00:00')                        | NULL       |
        | day + CR + time               | concat('2023-01-31', chr(13), '10:00:00')                        | NULL       |
        | day + formfeed + time         | concat('2023-01-31', chr(12), '10:00:00')                        | NULL       |
        | day + NBSP + time             | concat('2023-01-31', chr(160), '10:00:00')                       | NULL       |
        | year-month + space + text     | '2023-01 x'                                                      | NULL       |
        | year-month + T                | '2023-01T'                                                       | NULL       |
        | year-month + T + text         | '2023-01T10'                                                     | NULL       |
        | year + space + text           | '2023 x'                                                         | NULL       |
        | year + T                      | '2023T'                                                          | NULL       |
        | year + dash (empty month)     | '2023-'                                                          | NULL       |
        | year-month + dash (empty day) | '2023-01-'                                                       | NULL       |
        | empty month                   | '2023--01'                                                       | NULL       |
        | 3-digit day                   | '2023-01-310'                                                    | NULL       |
        | 3-digit month                 | '2023-001-01'                                                    | NULL       |
        | leading space                 | ' 2023-01-31'                                                    | 2023-01-31 |
        | leading space + T             | ' 2023-01-31T10:00:00'                                           | 2023-01-31 |
        | leading tab                   | concat(chr(9), '2023-01-31')                                     | 2023-01-31 |
        | leading control 0x01          | concat(chr(1), '2023-01-31')                                     | 2023-01-31 |
        | trailing control 0x01         | concat('2023-01-31', chr(1))                                     | 2023-01-31 |
        | trailing tab                  | concat('2023-01-31', chr(9))                                     | 2023-01-31 |
        | trailing newline              | concat('2023-01-31', chr(10))                                    | 2023-01-31 |
        | trailing T + space            | '2023-01-31T '                                                   | 2023-01-31 |
        | leading NBSP                  | concat(chr(160), '2023-01-31')                                   | NULL       |
        | trailing NBSP                 | concat('2023-01-31', chr(160))                                   | NULL       |
        | NUL inside                    | concat('2023-01-3', chr(0), '1')                                 | NULL       |
        | month 00                      | '2023-00-10'                                                     | NULL       |
        | month 13                      | '2023-13-01'                                                     | NULL       |
        | day 00                        | '2023-01-00'                                                     | NULL       |
        | day 32                        | '2023-01-32'                                                     | NULL       |
        | feb 29 non-leap               | '2023-02-29'                                                     | NULL       |
        | feb 29 leap                   | '2024-02-29'                                                     | 2024-02-29 |
        | apr 31                        | '2023-04-31'                                                     | NULL       |
        | invalid date + T              | '2023-02-30T00:00:00'                                            | NULL       |
        | fullwidth digits              | concat(chr(65298), chr(65296), chr(65298), chr(65299), '-01-31') | NULL       |
        | arabic-indic digits           | concat(chr(1634), chr(1632), chr(1634), chr(1635), '-01-31')     | NULL       |
        | empty                         | ''                                                               | NULL       |
        | space only                    | ' '                                                              | NULL       |
        | tab only                      | concat(chr(9))                                                   | NULL       |
        | null                          | CAST(NULL AS STRING)                                             | NULL       |

  Rule: Strings stringToDate accepts return the same date with ANSI on

    Scenario Outline: STRING to DATE with ANSI on, accepted: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<input> AS DATE) AS cast_result,
               TRY_CAST(<input> AS DATE) AS try_cast_result,
               to_date(<input>) AS to_date_result
        """
      Then query result
        | cast_result | try_cast_result | to_date_result |
        | <result>    | <result>        | <result>       |

      Examples:
        | case                    | input                         | result     |
        | day                     | '2023-01-31'                  | 2023-01-31 |
        | 1-digit month/day       | '2023-1-5'                    | 2023-01-05 |
        | year-month              | '2023-01'                     | 2023-01-01 |
        | year only               | '2023'                        | 2023-01-01 |
        | plus sign               | '+2023-01-31'                 | 2023-01-31 |
        | day + space + text      | '2023-01-31 x'                | 2023-01-31 |
        | day + space             | '2023-01-31 '                 | 2023-01-31 |
        | day + two spaces + text | '2023-01-31  x'               | 2023-01-31 |
        | day + T                 | '2023-01-31T'                 | 2023-01-31 |
        | day + T + text          | '2023-01-31Tx'                | 2023-01-31 |
        | day + T + time          | '2023-01-31T13:45:00.123'     | 2023-01-31 |
        | day + T + offset        | '2023-01-31T01:30:00+02:00'   | 2023-01-31 |
        | 1-digit + T + time      | '2023-1-5T10:00:00'           | 2023-01-05 |
        | day + space + time      | '2023-01-31 13:45:00'         | 2023-01-31 |
        | leading space           | ' 2023-01-31'                 | 2023-01-31 |
        | leading space + T       | ' 2023-01-31T10:00:00'        | 2023-01-31 |
        | leading tab             | concat(chr(9), '2023-01-31')  | 2023-01-31 |
        | leading control 0x01    | concat(chr(1), '2023-01-31')  | 2023-01-31 |
        | trailing control 0x01   | concat('2023-01-31', chr(1))  | 2023-01-31 |
        | trailing tab            | concat('2023-01-31', chr(9))  | 2023-01-31 |
        | trailing newline        | concat('2023-01-31', chr(10)) | 2023-01-31 |
        | trailing T + space      | '2023-01-31T '                | 2023-01-31 |
        | feb 29 leap             | '2024-02-29'                  | 2024-02-29 |
        | null                    | CAST(NULL AS STRING)          | NULL       |

  Rule: Strings stringToDate rejects raise with ANSI on and are NULL for TRY_CAST

    Scenario Outline: STRING to DATE with ANSI on, CAST raises: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<input> AS DATE) AS result
        """
      Then query error \[CAST_INVALID_INPUT\]

      Examples:
        | case                          | input                                                            |
        | year 8 digits                 | '12345678-01-31'                                                 |
        | year 3 digits                 | '123-01-31'                                                      |
        | day + lowercase t             | '2023-01-31t10:00:00'                                            |
        | day + Z                       | '2023-01-31Z'                                                    |
        | day + x                       | '2023-01-31x'                                                    |
        | day + dash                    | '2023-01-31-05'                                                  |
        | day + dot                     | '2023-01-31.5'                                                   |
        | day + colon                   | '2023-01-31:00'                                                  |
        | day + tab + time              | concat('2023-01-31', chr(9), '10:00:00')                         |
        | day + newline + time          | concat('2023-01-31', chr(10), '10:00:00')                        |
        | day + CR + time               | concat('2023-01-31', chr(13), '10:00:00')                        |
        | day + formfeed + time         | concat('2023-01-31', chr(12), '10:00:00')                        |
        | day + NBSP + time             | concat('2023-01-31', chr(160), '10:00:00')                       |
        | year-month + space + text     | '2023-01 x'                                                      |
        | year-month + T                | '2023-01T'                                                       |
        | year-month + T + text         | '2023-01T10'                                                     |
        | year + space + text           | '2023 x'                                                         |
        | year + T                      | '2023T'                                                          |
        | year + dash (empty month)     | '2023-'                                                          |
        | year-month + dash (empty day) | '2023-01-'                                                       |
        | empty month                   | '2023--01'                                                       |
        | 3-digit day                   | '2023-01-310'                                                    |
        | 3-digit month                 | '2023-001-01'                                                    |
        | leading NBSP                  | concat(chr(160), '2023-01-31')                                   |
        | trailing NBSP                 | concat('2023-01-31', chr(160))                                   |
        | NUL inside                    | concat('2023-01-3', chr(0), '1')                                 |
        | month 00                      | '2023-00-10'                                                     |
        | month 13                      | '2023-13-01'                                                     |
        | day 00                        | '2023-01-00'                                                     |
        | day 32                        | '2023-01-32'                                                     |
        | feb 29 non-leap               | '2023-02-29'                                                     |
        | apr 31                        | '2023-04-31'                                                     |
        | invalid date + T              | '2023-02-30T00:00:00'                                            |
        | fullwidth digits              | concat(chr(65298), chr(65296), chr(65298), chr(65299), '-01-31') |
        | arabic-indic digits           | concat(chr(1634), chr(1632), chr(1634), chr(1635), '-01-31')     |
        | empty                         | ''                                                               |
        | space only                    | ' '                                                              |
        | tab only                      | concat(chr(9))                                                   |

    Scenario Outline: STRING to DATE with ANSI on, to_date raises: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date(<input>) AS result
        """
      Then query error \[CAST_INVALID_INPUT\]

      Examples:
        | case                          | input                                                            |
        | year 8 digits                 | '12345678-01-31'                                                 |
        | year 3 digits                 | '123-01-31'                                                      |
        | day + lowercase t             | '2023-01-31t10:00:00'                                            |
        | day + Z                       | '2023-01-31Z'                                                    |
        | day + x                       | '2023-01-31x'                                                    |
        | day + dash                    | '2023-01-31-05'                                                  |
        | day + dot                     | '2023-01-31.5'                                                   |
        | day + colon                   | '2023-01-31:00'                                                  |
        | day + tab + time              | concat('2023-01-31', chr(9), '10:00:00')                         |
        | day + newline + time          | concat('2023-01-31', chr(10), '10:00:00')                        |
        | day + CR + time               | concat('2023-01-31', chr(13), '10:00:00')                        |
        | day + formfeed + time         | concat('2023-01-31', chr(12), '10:00:00')                        |
        | day + NBSP + time             | concat('2023-01-31', chr(160), '10:00:00')                       |
        | year-month + space + text     | '2023-01 x'                                                      |
        | year-month + T                | '2023-01T'                                                       |
        | year-month + T + text         | '2023-01T10'                                                     |
        | year + space + text           | '2023 x'                                                         |
        | year + T                      | '2023T'                                                          |
        | year + dash (empty month)     | '2023-'                                                          |
        | year-month + dash (empty day) | '2023-01-'                                                       |
        | empty month                   | '2023--01'                                                       |
        | 3-digit day                   | '2023-01-310'                                                    |
        | 3-digit month                 | '2023-001-01'                                                    |
        | leading NBSP                  | concat(chr(160), '2023-01-31')                                   |
        | trailing NBSP                 | concat('2023-01-31', chr(160))                                   |
        | NUL inside                    | concat('2023-01-3', chr(0), '1')                                 |
        | month 00                      | '2023-00-10'                                                     |
        | month 13                      | '2023-13-01'                                                     |
        | day 00                        | '2023-01-00'                                                     |
        | day 32                        | '2023-01-32'                                                     |
        | feb 29 non-leap               | '2023-02-29'                                                     |
        | apr 31                        | '2023-04-31'                                                     |
        | invalid date + T              | '2023-02-30T00:00:00'                                            |
        | fullwidth digits              | concat(chr(65298), chr(65296), chr(65298), chr(65299), '-01-31') |
        | arabic-indic digits           | concat(chr(1634), chr(1632), chr(1634), chr(1635), '-01-31')     |
        | empty                         | ''                                                               |
        | space only                    | ' '                                                              |
        | tab only                      | concat(chr(9))                                                   |

    Scenario Outline: STRING to DATE with ANSI on, TRY_CAST is NULL: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT TRY_CAST(<input> AS DATE) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                          | input                                                            |
        | year 8 digits                 | '12345678-01-31'                                                 |
        | year 3 digits                 | '123-01-31'                                                      |
        | day + lowercase t             | '2023-01-31t10:00:00'                                            |
        | day + Z                       | '2023-01-31Z'                                                    |
        | day + x                       | '2023-01-31x'                                                    |
        | day + dash                    | '2023-01-31-05'                                                  |
        | day + dot                     | '2023-01-31.5'                                                   |
        | day + colon                   | '2023-01-31:00'                                                  |
        | day + tab + time              | concat('2023-01-31', chr(9), '10:00:00')                         |
        | day + newline + time          | concat('2023-01-31', chr(10), '10:00:00')                        |
        | day + CR + time               | concat('2023-01-31', chr(13), '10:00:00')                        |
        | day + formfeed + time         | concat('2023-01-31', chr(12), '10:00:00')                        |
        | day + NBSP + time             | concat('2023-01-31', chr(160), '10:00:00')                       |
        | year-month + space + text     | '2023-01 x'                                                      |
        | year-month + T                | '2023-01T'                                                       |
        | year-month + T + text         | '2023-01T10'                                                     |
        | year + space + text           | '2023 x'                                                         |
        | year + T                      | '2023T'                                                          |
        | year + dash (empty month)     | '2023-'                                                          |
        | year-month + dash (empty day) | '2023-01-'                                                       |
        | empty month                   | '2023--01'                                                       |
        | 3-digit day                   | '2023-01-310'                                                    |
        | 3-digit month                 | '2023-001-01'                                                    |
        | leading NBSP                  | concat(chr(160), '2023-01-31')                                   |
        | trailing NBSP                 | concat('2023-01-31', chr(160))                                   |
        | NUL inside                    | concat('2023-01-3', chr(0), '1')                                 |
        | month 00                      | '2023-00-10'                                                     |
        | month 13                      | '2023-13-01'                                                     |
        | day 00                        | '2023-01-00'                                                     |
        | day 32                        | '2023-01-32'                                                     |
        | feb 29 non-leap               | '2023-02-29'                                                     |
        | apr 31                        | '2023-04-31'                                                     |
        | invalid date + T              | '2023-02-30T00:00:00'                                            |
        | fullwidth digits              | concat(chr(65298), chr(65296), chr(65298), chr(65299), '-01-31') |
        | arabic-indic digits           | concat(chr(1634), chr(1632), chr(1634), chr(1635), '-01-31')     |
        | empty                         | ''                                                               |
        | space only                    | ' '                                                              |
        | tab only                      | concat(chr(9))                                                   |

  Rule: The year may have up to seven digits, a sign, and any value a DATE holds

    # The client cannot hold these years, so the day count from 2000-01-01 is compared.
    Scenario Outline: STRING to DATE year range with ANSI off: <input>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT datediff(CAST('<input>' AS DATE), DATE '2000-01-01') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input          | result     |
        | 2023-01-31     | 8431       |
        | -2023-01-31    | -1469340   |
        | +2023-01-31    | 8431       |
        | 0000-01-31     | -730455    |
        | 0001-01-01     | -730119    |
        | 9999-12-31     | 2921939    |
        | 10000-01-01    | 2921940    |
        | 12345-01-31    | 3778464    |
        | +12345-01-31   | 3778464    |
        | -12345-01-31   | -5239373   |
        | 262143-12-31   | 95015644   |
        | 262144-01-01   | 95015645   |
        | 1234567-01-31  | 450185883  |
        | -1234567-01-31 | -451646792 |
        | 5874897-12-31  | 2145031948 |
        | 5874898-01-01  | 2145031949 |
        | 12345678-01-31 | NULL       |
