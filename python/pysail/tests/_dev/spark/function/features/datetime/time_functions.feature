# SPDX-License-Identifier: Apache-2.0

Feature: TIME functions (make_time, time_diff, time_trunc, time_from_*, time_to_*)

  # `spark.sql.timeType.enabled` is an internal Spark configuration whose default is
  # `Utils.isTesting`, so it is on only inside Spark's own test suite and off in every real
  # session. Without it, `TimeExpression.checkInputDataTypes()` rejects these functions with
  # `UNSUPPORTED_TIME_TYPE` and the scenarios below cannot be validated against JVM Spark.
  Background:
      Given config spark.sql.timeType.enabled = true

  Rule: make_time

    Scenario Outline: make_time: <case>
      When query
        """
        SELECT make_time(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | args              | result          |
        | basic make_time            | 6, 30, 45.887     | 06:30:45.887    |
        | make_time midnight         | 0, 0, 0           | 00:00:00        |
        | make_time max precision    | 23, 59, 59.999999 | 23:59:59.999999 |
        | make_time integer seconds  | 12, 0, 30         | 12:00:30        |
        | make_time NULL propagation | NULL, 30, 0       | NULL            |

    Scenario Outline: make_time invalid: <case>
      When query
        """
        SELECT CAST(make_time(<args>) AS STRING)
        """
      Then query error <error>

      Examples:
        | case                            | args     | error          |
        | make_time invalid hour errors   | 25, 0, 0 | HourOfDay      |
        | make_time invalid minute errors | 0, 60, 0 | MinuteOfHour   |
        | make_time invalid second errors | 0, 0, 60 | SecondOfMinute |

  Rule: time_diff

    Scenario Outline: time_diff: <case>
      When query
        """
        SELECT time_diff(<unit>, <start>, <end>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | unit          | start           | end                 | result  |
        | time_diff hours exact                   | 'HOUR'        | TIME '20:30:29' | TIME '21:30:29'     | 1       |
        | time_diff hours truncation              | 'HOUR'        | TIME '20:30:29' | TIME '21:30:28'     | 0       |
        | time_diff negative                      | 'HOUR'        | TIME '20:30:29' | TIME '12:00:00'     | -8      |
        | time_diff minutes                       | 'MINUTE'      | TIME '10:00:00' | TIME '10:45:30'     | 45      |
        | time_diff seconds                       | 'SECOND'      | TIME '10:00:00' | TIME '10:00:30'     | 30      |
        | time_diff microseconds                  | 'MICROSECOND' | TIME '00:00:00' | TIME '00:00:01'     | 1000000 |
        | time_diff milliseconds                  | 'MILLISECOND' | TIME '00:00:00' | TIME '00:00:01.500' | 1500    |
        | time_diff NULL start propagates to NULL | 'HOUR'        | NULL            | TIME '01:00:00'     | NULL    |
        | time_diff NULL end propagates to NULL   | 'MINUTE'      | TIME '10:00:00' | NULL                | NULL    |
        | time_diff NULL unit propagates to NULL  | NULL          | TIME '10:00:00' | TIME '11:00:00'     | NULL    |

    Scenario: time_diff invalid unit errors
      When query
        """
        SELECT time_diff('MS', TIME '10:00:00', TIME '11:00:00')
        """
      Then query error expects one of the units

    Scenario: time_diff with unit from column
      When query
        """
        SELECT time_diff(unit, TIME '08:00:00', TIME '10:30:00') AS result
        FROM (VALUES ('HOUR'), ('MINUTE')) AS t(unit)
        """
      Then query result
        | result |
        | 2      |
        | 150    |

  Rule: time_trunc

    Scenario Outline: time_trunc: <case>
      When query
        """
        SELECT time_trunc(<unit>, <time>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | unit          | time                   | result          |
        | time_trunc hour                         | 'HOUR'        | TIME '09:32:05.359'    | 09:00:00        |
        | time_trunc minute                       | 'MINUTE'      | TIME '09:32:05.359'    | 09:32:00        |
        | time_trunc second                       | 'SECOND'      | TIME '09:32:05.359'    | 09:32:05        |
        | time_trunc millisecond                  | 'MILLISECOND' | TIME '09:32:05.123456' | 09:32:05.123    |
        | time_trunc microsecond passthrough      | 'MICROSECOND' | TIME '09:32:05.123456' | 09:32:05.123456 |
        | time_trunc NULL unit propagates to NULL | NULL          | TIME '09:32:05.123456' | NULL            |
        | time_trunc NULL time propagates to NULL | 'HOUR'        | NULL                   | NULL            |

    Scenario: time_trunc invalid unit errors
      When query
        """
        SELECT CAST(time_trunc('MS', TIME '09:32:05.123456') AS STRING)
        """
      Then query error expects one of the units

    Scenario: time_trunc with unit from column
      When query
        """
        SELECT time_trunc(unit, TIME '09:32:05.359') AS result
        FROM (VALUES ('HOUR'), ('MINUTE'), ('SECOND')) AS t(unit)
        """
      Then query result
        | result   |
        | 09:00:00 |
        | 09:32:00 |
        | 09:32:05 |

  Rule: time_to_micros / time_to_millis / time_to_seconds

    Scenario Outline: time_to_*: <case>
      When query
        """
        SELECT <function>(<time>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | function        | time                   | result        |
        | micros midnight            | time_to_micros  | TIME '00:00:00'        | 0             |
        | micros fractional          | time_to_micros  | TIME '14:30:00.5'      | 52200500000   |
        | micros max                 | time_to_micros  | TIME '23:59:59.999999' | 86399999999   |
        | millis midnight            | time_to_millis  | TIME '00:00:00'        | 0             |
        | millis whole               | time_to_millis  | TIME '14:30:00'        | 52200000      |
        | millis fractional          | time_to_millis  | TIME '14:30:00.5'      | 52200500      |
        | millis truncates           | time_to_millis  | TIME '23:59:59.999999' | 86399999      |
        | seconds midnight           | time_to_seconds | TIME '00:00:00'        | 0.000000      |
        | seconds whole              | time_to_seconds | TIME '14:30:00'        | 52200.000000  |
        | seconds fractional         | time_to_seconds | TIME '14:30:00.5'      | 52200.500000  |
        | seconds max                | time_to_seconds | TIME '23:59:59.999999' | 86399.999999  |
        | micros NULL propagates     | time_to_micros  | NULL                   | NULL          |
        | millis NULL propagates     | time_to_millis  | NULL                   | NULL          |
        | seconds NULL propagates    | time_to_seconds | NULL                   | NULL          |

  Rule: time_from_micros / time_from_millis / time_from_seconds

    Scenario Outline: time_from_*: <case>
      When query
        """
        SELECT <function>(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | function          | value          | result          |
        | micros midnight          | time_from_micros  | 0              | 00:00:00        |
        | micros fractional        | time_from_micros  | 52200500000    | 14:30:00.5      |
        | micros max               | time_from_micros  | 86399999999    | 23:59:59.999999 |
        | millis midnight          | time_from_millis  | 0              | 00:00:00        |
        | millis whole             | time_from_millis  | 52200000       | 14:30:00        |
        | millis fractional        | time_from_millis  | 52200500       | 14:30:00.5      |
        | seconds midnight         | time_from_seconds | 0              | 00:00:00        |
        | seconds whole            | time_from_seconds | 52200          | 14:30:00        |
        | seconds fractional       | time_from_seconds | 52200.5        | 14:30:00.5      |
        | seconds max              | time_from_seconds | 86399.999999   | 23:59:59.999999 |
        | micros NULL propagates   | time_from_micros  | NULL           | NULL            |
        | millis NULL propagates   | time_from_millis  | NULL           | NULL            |
        | seconds NULL propagates  | time_from_seconds | NULL           | NULL            |

  Rule: time_to_* and time_from_* round-trip

    Scenario: round trip through micros
      When query
        """
        SELECT time_from_micros(time_to_micros(TIME '09:32:05.123456')) AS result
        """
      Then query result
        | result          |
        | 09:32:05.123456 |
