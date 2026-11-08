Feature: Interval literal bounds

  Rule: The minimum and maximum interval values are accepted

    Scenario Outline: interval literal within range: <literal>
      When query
        """
        SELECT CAST(<literal> AS STRING) AS s
        """
      Then query result
        | s        |
        | <string> |

      Examples:
        | literal                                                    | string                                              |
        | INTERVAL '-178956970' YEAR                                 | INTERVAL '-178956970' YEAR                          |
        | INTERVAL -'178956970' YEAR                                 | INTERVAL '-178956970' YEAR                          |
        | INTERVAL '178956970' YEAR                                  | INTERVAL '178956970' YEAR                           |
        | -INTERVAL '178956970' YEAR                                 | INTERVAL '-178956970' YEAR                          |
        | INTERVAL -'-178956970' YEAR                                | INTERVAL '178956970' YEAR                           |
        | INTERVAL '-178956970-8' YEAR TO MONTH                      | INTERVAL '-178956970-8' YEAR TO MONTH               |
        | INTERVAL -'178956970-8' YEAR TO MONTH                      | INTERVAL '-178956970-8' YEAR TO MONTH               |
        | INTERVAL '178956970-7' YEAR TO MONTH                       | INTERVAL '178956970-7' YEAR TO MONTH                |
        | INTERVAL '-2147483648' MONTH                               | INTERVAL '-2147483648' MONTH                        |
        | INTERVAL -'2147483648' MONTH                               | INTERVAL '-2147483648' MONTH                        |
        | INTERVAL '2147483647' MONTH                                | INTERVAL '2147483647' MONTH                         |
        | INTERVAL '-106751991' DAY                                  | INTERVAL '-106751991' DAY                           |
        | INTERVAL -'106751991' DAY                                  | INTERVAL '-106751991' DAY                           |
        | INTERVAL '106751991' DAY                                   | INTERVAL '106751991' DAY                            |
        | -INTERVAL '106751991' DAY                                  | INTERVAL '-106751991' DAY                           |
        | INTERVAL -'-106751991' DAY                                 | INTERVAL '106751991' DAY                            |
        | INTERVAL '-106751991 04' DAY TO HOUR                       | INTERVAL '-106751991 04' DAY TO HOUR                |
        | INTERVAL -'106751991 04' DAY TO HOUR                       | INTERVAL '-106751991 04' DAY TO HOUR                |
        | INTERVAL '106751991 04' DAY TO HOUR                        | INTERVAL '106751991 04' DAY TO HOUR                 |
        | -INTERVAL '106751991 04' DAY TO HOUR                       | INTERVAL '-106751991 04' DAY TO HOUR                |
        | INTERVAL -'-106751991 04' DAY TO HOUR                      | INTERVAL '106751991 04' DAY TO HOUR                 |
        | INTERVAL '-106751991 04:00' DAY TO MINUTE                  | INTERVAL '-106751991 04:00' DAY TO MINUTE           |
        | INTERVAL -'106751991 04:00' DAY TO MINUTE                  | INTERVAL '-106751991 04:00' DAY TO MINUTE           |
        | INTERVAL '106751991 04:00' DAY TO MINUTE                   | INTERVAL '106751991 04:00' DAY TO MINUTE            |
        | -INTERVAL '106751991 04:00' DAY TO MINUTE                  | INTERVAL '-106751991 04:00' DAY TO MINUTE           |
        | INTERVAL -'-106751991 04:00' DAY TO MINUTE                 | INTERVAL '106751991 04:00' DAY TO MINUTE            |
        | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND        | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND |
        | INTERVAL -'106751991 04:00:54.775808' DAY TO SECOND        | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND |
        | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND         | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND  |
        | INTERVAL '-2562047788' HOUR                                | INTERVAL '-2562047788' HOUR                         |
        | INTERVAL -'2562047788' HOUR                                | INTERVAL '-2562047788' HOUR                         |
        | INTERVAL '2562047788' HOUR                                 | INTERVAL '2562047788' HOUR                          |
        | -INTERVAL '2562047788' HOUR                                | INTERVAL '-2562047788' HOUR                         |
        | INTERVAL -'-2562047788' HOUR                               | INTERVAL '2562047788' HOUR                          |
        | INTERVAL '-2562047788:00' HOUR TO MINUTE                   | INTERVAL '-2562047788:00' HOUR TO MINUTE            |
        | INTERVAL -'2562047788:00' HOUR TO MINUTE                   | INTERVAL '-2562047788:00' HOUR TO MINUTE            |
        | INTERVAL '2562047788:00' HOUR TO MINUTE                    | INTERVAL '2562047788:00' HOUR TO MINUTE             |
        | -INTERVAL '2562047788:00' HOUR TO MINUTE                   | INTERVAL '-2562047788:00' HOUR TO MINUTE            |
        | INTERVAL -'-2562047788:00' HOUR TO MINUTE                  | INTERVAL '2562047788:00' HOUR TO MINUTE             |
        | INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND         | INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND  |
        | INTERVAL -'2562047788:00:54.775808' HOUR TO SECOND         | INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND  |
        | INTERVAL '2562047788:00:54.775807' HOUR TO SECOND          | INTERVAL '2562047788:00:54.775807' HOUR TO SECOND   |
        | INTERVAL '-153722867280' MINUTE                            | INTERVAL '-153722867280' MINUTE                     |
        | INTERVAL -'153722867280' MINUTE                            | INTERVAL '-153722867280' MINUTE                     |
        | INTERVAL '153722867280' MINUTE                             | INTERVAL '153722867280' MINUTE                      |
        | -INTERVAL '153722867280' MINUTE                            | INTERVAL '-153722867280' MINUTE                     |
        | INTERVAL -'-153722867280' MINUTE                           | INTERVAL '153722867280' MINUTE                      |
        | INTERVAL '-153722867280:54.775808' MINUTE TO SECOND        | INTERVAL '-153722867280:54.775808' MINUTE TO SECOND |
        | INTERVAL -'153722867280:54.775808' MINUTE TO SECOND        | INTERVAL '-153722867280:54.775808' MINUTE TO SECOND |
        | INTERVAL '153722867280:54.775807' MINUTE TO SECOND         | INTERVAL '153722867280:54.775807' MINUTE TO SECOND  |
        | INTERVAL '-9223372036854.775808' SECOND                    | INTERVAL '-9223372036854.775808' SECOND             |
        | INTERVAL -'9223372036854.775808' SECOND                    | INTERVAL '-9223372036854.775808' SECOND             |
        | INTERVAL '9223372036854.775807' SECOND                     | INTERVAL '9223372036854.775807' SECOND              |
        | INTERVAL -178956970 YEAR -8 MONTH                          | INTERVAL '-178956970-8' YEAR TO MONTH               |
        | INTERVAL '-178956970' YEAR '-8' MONTH                      | INTERVAL '-178956970-8' YEAR TO MONTH               |
        | INTERVAL -2147483648 MONTH                                 | INTERVAL '-2147483648' MONTH                        |
        | INTERVAL '-2147483648' MONTHS                              | INTERVAL '-2147483648' MONTH                        |
        | INTERVAL -106751991 DAY -14454775808 MICROSECOND           | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND |
        | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND        | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND |
        | INTERVAL '-9223372036854.775808' SECOND                    | INTERVAL '-9223372036854.775808' SECOND             |
        | INTERVAL '-9223372036854.775808' SECONDS                   | INTERVAL '-9223372036854.775808' SECOND             |
        | INTERVAL -106751991 DAYS                                   | INTERVAL '-106751991' DAY                           |
        | INTERVAL '-178956970-8' YEAR TO MONTH + INTERVAL '1' MONTH | INTERVAL '-178956970-7' YEAR TO MONTH               |
        | INTERVAL '-178956970 years -8 months'                      | INTERVAL '-178956970-8' YEAR TO MONTH               |
        | INTERVAL '-2147483648 months'                              | INTERVAL '-2147483648' MONTH                        |

  Rule: Interval literals out of range are rejected

    Scenario Outline: interval literal out of range: <literal>
      When query
        """
        SELECT CAST(<literal> AS STRING) AS s
        """
      Then query error (?i)interval|overflow

      Examples:
        | literal                                              |
        | INTERVAL '-178956971' YEAR                           |
        | INTERVAL -'178956971' YEAR                           |
        | INTERVAL '178956971' YEAR                            |
        | INTERVAL '-178956970-9' YEAR TO MONTH                |
        | INTERVAL -'178956970-9' YEAR TO MONTH                |
        | INTERVAL '178956970-8' YEAR TO MONTH                 |
        | -INTERVAL '178956970-8' YEAR TO MONTH                |
        | INTERVAL -'-178956970-8' YEAR TO MONTH               |
        | INTERVAL '-2147483649' MONTH                         |
        | INTERVAL -'2147483649' MONTH                         |
        | INTERVAL '2147483648' MONTH                          |
        | -INTERVAL '2147483648' MONTH                         |
        | INTERVAL -'-2147483648' MONTH                        |
        | INTERVAL '-106751992' DAY                            |
        | INTERVAL -'106751992' DAY                            |
        | INTERVAL '106751992' DAY                             |
        | INTERVAL '-106751991 05' DAY TO HOUR                 |
        | INTERVAL -'106751991 05' DAY TO HOUR                 |
        | INTERVAL '106751991 05' DAY TO HOUR                  |
        | INTERVAL '-106751991 04:01' DAY TO MINUTE            |
        | INTERVAL -'106751991 04:01' DAY TO MINUTE            |
        | INTERVAL '106751991 04:01' DAY TO MINUTE             |
        | INTERVAL '-106751991 04:00:54.775809' DAY TO SECOND  |
        | INTERVAL -'106751991 04:00:54.775809' DAY TO SECOND  |
        | INTERVAL '106751991 04:00:54.775808' DAY TO SECOND   |
        | -INTERVAL '106751991 04:00:54.775808' DAY TO SECOND  |
        | INTERVAL -'-106751991 04:00:54.775808' DAY TO SECOND |
        | INTERVAL '-2562047789' HOUR                          |
        | INTERVAL -'2562047789' HOUR                          |
        | INTERVAL '2562047789' HOUR                           |
        | INTERVAL '-2562047788:01' HOUR TO MINUTE             |
        | INTERVAL -'2562047788:01' HOUR TO MINUTE             |
        | INTERVAL '2562047788:01' HOUR TO MINUTE              |
        | INTERVAL '-2562047788:00:54.775809' HOUR TO SECOND   |
        | INTERVAL -'2562047788:00:54.775809' HOUR TO SECOND   |
        | INTERVAL '2562047788:00:54.775808' HOUR TO SECOND    |
        | -INTERVAL '2562047788:00:54.775808' HOUR TO SECOND   |
        | INTERVAL -'-2562047788:00:54.775808' HOUR TO SECOND  |
        | INTERVAL '-153722867281' MINUTE                      |
        | INTERVAL -'153722867281' MINUTE                      |
        | INTERVAL '153722867281' MINUTE                       |
        | INTERVAL '-153722867280:54.775809' MINUTE TO SECOND  |
        | INTERVAL -'153722867280:54.775809' MINUTE TO SECOND  |
        | INTERVAL '153722867280:54.775808' MINUTE TO SECOND   |
        | -INTERVAL '153722867280:54.775808' MINUTE TO SECOND  |
        | INTERVAL -'-153722867280:54.775808' MINUTE TO SECOND |
        | INTERVAL '-9223372036854.775809' SECOND              |
        | INTERVAL -'9223372036854.775809' SECOND              |
        | INTERVAL '9223372036854.775808' SECOND               |
        | -INTERVAL '9223372036854.775808' SECOND              |
        | INTERVAL -'-9223372036854.775808' SECOND             |
