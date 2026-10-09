Feature: Generator placement in withColumn and withColumns
  # Spark 4.2 Analyzer.ExtractGenerator checks placement before evaluating expressions.

  Scenario Outline: A generator nested in an expression is rejected
    When <api> adds column x using <expression>
    Then the column query rejects a nested generator

    Examples:
      | api         | expression                                        |
      | withColumn  | explode(a) + 1                                    |
      | withColumns | explode(a) + 1                                    |
      | withColumn  | CASE WHEN false THEN explode(a) ELSE 0 END        |
      | withColumns | CASE WHEN false THEN explode(a) ELSE 0 END        |
      | withColumn  | explode(array(explode(a)))                        |
      | withColumns | explode(array(explode(a)))                        |
      | withColumn  | cast(explode(a) as bigint)                        |
      | withColumns | cast(explode(a) as bigint)                        |
      | withColumn  | explode_outer(a) + 1                              |
      | withColumns | explode_outer(a) + 1                              |
      | withColumn  | inline(array(named_struct('v', 1))) + 1           |
      | withColumns | inline(array(named_struct('v', 1))) + 1           |
      | withColumn  | sum(explode(a))                                   |
      | withColumns | sum(explode(a))                                   |
      | withColumn  | sum(explode(a)) OVER ()                           |
      | withColumns | sum(explode(a)) OVER ()                           |
      | withColumn  | (explode(a) + 1) AS ignored                       |
      | withColumns | (explode(a) + 1) AS ignored                       |

  Scenario Outline: A top-level generator remains supported
    When <api> adds column x using <expression>
    Then the column query returns its elements

    Examples:
      | api         | expression                                        |
      | withColumn  | explode(a)                                        |
      | withColumns | explode(a)                                        |
      | withColumn  | explode_outer(a)                                  |
      | withColumns | explode_outer(a)                                  |
      | withColumn  | explode(a) AS ignored                             |
      | withColumns | explode(a) AS ignored                             |
