Feature: Approximate percentile parameters require Spark-foldable expressions

  Scenario Outline: Higher-order parameters are rejected regardless of grouped input size
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM (SELECT 1 AS v WHERE <keep>) AS t
      GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage                     | accuracy                                      | keep  |
      | approx_percentile | transform(array(0.5D), x -> x) | 10000                                         | true  |
      | approx_percentile | transform(array(0.5D), x -> x) | 10000                                         | false |
      | percentile_approx | 0.5D                           | aggregate(array(100), 0, (acc, x) -> acc + x) | true  |
      | percentile_approx | 0.5D                           | aggregate(array(100), 0, (acc, x) -> acc + x) | false |
      | percentile_approx | array_sort(array(0.5D))        | 10000                                         | true  |
      | percentile_approx | array_sort(array(0.5D))        | 10000                                         | false |

  Scenario Outline: Higher-order parameters are rejected in window aggregates
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) OVER () AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                     | accuracy                                      |
      | transform(array(0.5D), x -> x) | 10000                                         |
      | 0.5D                           | aggregate(array(100), 0, (acc, x) -> acc + x) |

  Scenario: Foldable sequence and regexp expressions remain valid parameters
    When query
      """
      SELECT percentile_approx(v, sequence(0, 1), 100 * regexp_instr('abc', 'a')) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p      |
      | [1, 2] |

  Scenario Outline: Sort array remains foldable in either direction
    When query
      """
      SELECT percentile_approx(v, sort_array(array(0D, 1D), <ascending>)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | ascending | result |
      | true      | [1, 2] |
      | false     | [2, 1] |

  Scenario Outline: Approximate percentile parameters simplify foldable coalesce expressions
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | function          | percentage                                           | accuracy                           | result |
      | approx_percentile | coalesce(CAST(NULL AS DOUBLE), 0.5D)                 | 10000                              | 2      |
      | percentile_approx | coalesce(CAST(NULL AS ARRAY<DOUBLE>), array(0D, 1D)) | 10000                              | [1, 4] |
      | approx_percentile | 0.5D                                                 | coalesce(CAST(NULL AS INT), 10000) | 2      |

  Scenario: Foldable coalesce parameters work in window aggregates
    When query
      """
      SELECT v, percentile_approx(v, coalesce(CAST(NULL AS DOUBLE), 0.5D),
                 coalesce(CAST(NULL AS INT), 10000)) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p |
      | 1 | 1 |
      | 2 | 1 |

  Scenario: Foldable coalesce parameters retain empty grouped results
    When query
      """
      SELECT approx_percentile(v, coalesce(CAST(NULL AS DOUBLE), 0.5D)) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query result
      | p |

  Scenario Outline: Simplification does not make NVL wrappers foldable percentile parameters
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                         | accuracy                          |
      | ifnull(CAST(NULL AS DOUBLE), 0.5D) | 10000                             |
      | 0.5D                               | nvl2(CAST(NULL AS INT), 0, 10000) |

  @sail-bug
  Scenario: Nullif parameters retain Spark's non-foldable wrapper
    When query
      """
      SELECT percentile_approx(v, nullif(0.5D, 1D)) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query error (?i)foldable

  Scenario Outline: Approximate percentile parameters use the current query time
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query result
      | p |
      | 2 |

    Examples:
      | function          | percentage                                                     | accuracy                           |
      | percentile_approx | dayofmonth(current_date()) / (2D * dayofmonth(current_date())) | 10000                              |
      | approx_percentile | 0.5D                                                           | dayofmonth(current_date()) * 10000 |
      | percentile_approx | 0.5D                                                           | year(current_timestamp()) - 1970   |

  Scenario: Foldable current-date parameters work in window aggregates
    When query
      """
      SELECT v, percentile_approx(v,
                 dayofmonth(current_date()) / (2D * dayofmonth(current_date())),
                 dayofmonth(current_date()) * 10000) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p |
      | 1 | 1 |
      | 2 | 1 |

  Scenario: Foldable current-date parameters retain empty grouped results
    When query
      """
      SELECT percentile_approx(v,
                 dayofmonth(current_date()) / (2D * dayofmonth(current_date()))) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query result
      | p |

  Scenario Outline: TRY arithmetic parameters remain non-foldable for grouped aggregates
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM (SELECT 1 AS v WHERE <keep>) AS t
      GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage           | accuracy               | keep  |
      | percentile_approx | try_add(0, 1) * 0.5D | 100                    | true  |
      | approx_percentile | try_divide(1, 2)     | 100                    | true  |
      | percentile_approx | 0.5D                 | try_multiply(10, 10)   | true  |
      | approx_percentile | 0.5D                 | try_subtract(200, 100) | true  |
      | approx_percentile | try_add(0, 1) * 0.5D | 100                    | false |
      | percentile_approx | 0.5D                 | try_multiply(10, 10)   | false |

  Scenario Outline: TRY arithmetic parameters remain non-foldable for window aggregates
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) OVER () AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage           | accuracy               |
      | percentile_approx | try_add(0, 1) * 0.5D | 100                    |
      | approx_percentile | 0.5D                 | try_subtract(200, 100) |

  @spark-4
  Scenario Outline: New TRY wrappers remain non-foldable percentile parameters
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                                     | accuracy          |
      | try_url_decode('0.5')                          | 100               |
      | try_parse_url('http://x/?p=0.5', 'QUERY', 'p') | 100               |
      | 0.5D                                           | try_mod(100, 101) |

  Scenario: TRY binary conversion remains non-foldable inside an accuracy expression
    When query
      """
      SELECT percentile_approx(v, 0.5D, length(try_to_binary('abc', 'utf-8'))) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  Scenario: TRY AES decryption remains non-foldable inside an accuracy expression
    When query
      """
      SELECT approx_percentile(v, 0.5D,
               length(try_aes_decrypt(
                 unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'),
                 '0000111122223333', 'GCM'))) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  Scenario: Foldable TRY expressions remain valid percentile parameters
    When query
      """
      SELECT percentile_approx(v, try_to_number('0.5', '9.9'),
               width_bucket(3, 0, 10, 100)) AS numeric_parameters,
             approx_percentile(v, 0.5D,
               year(TRY_CAST('2020-01-01' AS TIMESTAMP))) AS cast_parameter
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | numeric_parameters | cast_parameter |
      | 1                  | 1              |

  @spark-4
  Scenario: Foldable TRY timestamp expressions remain valid percentile parameters
    When query
      """
      SELECT percentile_approx(v, 0.5D,
               year(try_make_timestamp_ntz(2020, 1, 1, 0, 0, 0))) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p |
      | 1 |

  Scenario Outline: Non-foldable URL and binary wrappers remain invalid percentile parameters
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                                 | accuracy                          |
      | url_encode('0.5')                          | 100                               |
      | url_decode('0.5')                          | 100                               |
      | parse_url('http://x/?p=0.5', 'QUERY', 'p') | 100                               |
      | 0.5D                                       | length(to_binary('abc', 'utf-8')) |
      | 0.5D                                       | regexp_count('abc', 'a')          |

  Scenario: Foldable array functions remain valid percentile parameters
    When query
      """
      SELECT percentile_approx(v, array_insert(array(0.5D), 1, 1D)) AS percentages,
             approx_percentile(v, 0.5D,
               IF(arrays_overlap(array(1), array(1)), 100, 200)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | percentages | p |
      | [2, 1]      | 1 |

  Scenario Outline: String wrappers remain non-foldable percentile parameters
    When query
      """
      SELECT percentile_approx(v, <percentage>, 100) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                                                   |
      | CAST(left('0.5', 3) AS DOUBLE)                               |
      | CAST(right('0.5', 3) AS DOUBLE)                              |
      | CAST(split_part('0.5,1', ',', 1) AS DOUBLE)                  |
      | CAST(decode(unhex('302e35'), 'UTF-8') AS DOUBLE)             |
      | CASE WHEN luhn_check('79927398713') THEN 0.5D ELSE 0.75D END |

  Scenario: AES decryption remains non-foldable inside an accuracy expression
    When query
      """
      SELECT approx_percentile(v, 0.5D,
               length(aes_decrypt(
                 unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='),
                 '1234567890abcdef', 'CBC'))) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  @spark-4
  Scenario: UTF8 repair remains non-foldable inside a percentage expression
    When query
      """
      SELECT percentile_approx(v, CAST(make_valid_utf8('0.5') AS DOUBLE)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  @spark-4
  @sail-bug
  Scenario: Encoding is non-foldable in Spark 4 percentile parameters
    When query
      """
      SELECT percentile_approx(v, 0.5D, length(encode('abcd', 'UTF-8'))) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  Scenario Outline: Source collection wrappers remain non-foldable percentile parameters
    When query
      """
      SELECT <function>(v, <percentage>) AS p
      FROM (SELECT 1 AS v WHERE <keep>) AS t GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage                         | keep  |
      | percentile_approx | array_compact(array(0D, NULL, 1D)) | true  |
      | percentile_approx | array_prepend(array(0.5D), 1D)     | true  |
      | approx_percentile | array_compact(array(0D, NULL, 1D)) | false |
      | approx_percentile | array_prepend(array(0.5D), 1D)     | false |

  Scenario: Source collection wrappers remain non-foldable with all-null observations
    When query
      """
      SELECT percentile_approx(CAST(NULL AS INT), array_prepend(array(0.5D), 1D)) AS p
      """
    Then query error (?i)foldable

  Scenario Outline: Source functions remain non-foldable inside percentile accuracy
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT approx_percentile(v, 0.5D, <accuracy>) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | accuracy                                  |
      | length(current_timezone()) * 100          |
      | size(array_compact(array(1, NULL))) * 100 |

  Scenario Outline: Source function foldability is checked in percentile windows
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) OVER () AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                         | accuracy                         |
      | array_compact(array(0D, NULL, 1D)) | 100                              |
      | 0.5D                               | length(current_timezone()) * 100 |

  Scenario: Percentile source foldability checks do not leak to observations or siblings
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT percentile_approx(size(array_compact(array(v, NULL))), 0.5D) AS p,
             length(current_timezone()) AS timezone_length,
             array_append(array(1), 2) AS appended
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p | timezone_length | appended |
      | 1 | 3               | [1, 2]   |

  Scenario: Foldable functions sharing collection implementations remain valid in windows
    When query
      """
      SELECT v, approx_percentile(v, array_insert(array(0.5D), 1, 1D),
                 IF(arrays_overlap(array(1), array(1)), 100, 200)) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p      |
      | 1 | [2, 1] |
      | 2 | [2, 1] |

  @spark-4
  @sail-bug
  Scenario: Array append retains its non-foldable Spark 4 wrapper in percentile parameters
    When query
      """
      SELECT percentile_approx(v, array_append(array(0.5D), 1D)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  Scenario Outline: Current context expressions remain non-foldable percentile parameters
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM (SELECT 1 AS v WHERE <keep>) AS t GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage                                        | accuracy                         | keep  |
      | percentile_approx | array(IF(current_database() IS NULL, 0.5D, 0.5D)) | 100                              | true  |
      | approx_percentile | IF(current_schema() IS NULL, 0.5D, 0.5D)          | 100                              | true  |
      | percentile_approx | IF(current_catalog() IS NULL, 0.5D, 0.5D)         | 100                              | true  |
      | approx_percentile | IF(current_user() IS NULL, 0.5D, 0.5D)            | 100                              | true  |
      | percentile_approx | IF(user() IS NULL, 0.5D, 0.5D)                    | 100                              | true  |
      | approx_percentile | 0.5D                                              | length(current_database()) + 100 | true  |
      | percentile_approx | 0.5D                                              | length(current_catalog()) + 100  | false |
      | approx_percentile | 0.5D                                              | length(current_user()) + 100     | false |

  Scenario Outline: Current context expressions remain non-foldable percentile window parameters
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) OVER () AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                               | accuracy             |
      | IF(current_schema() IS NULL, 0.5D, 0.5D) | 100                  |
      | 0.5D                                     | length(user()) + 100 |

  @spark-4
  Scenario: Session user remains a non-foldable percentile parameter
    When query
      """
      SELECT percentile_approx(v, IF(session_user() IS NULL, 0.5D, 0.5D)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  Scenario Outline: Try element lookup remains non-foldable for array percentile parameters
    When query
      """
      SELECT percentile_approx(v, try_element_at(array(array(0D, 1D)), 1)) AS p
      FROM (SELECT 1 AS v WHERE <keep>) AS t GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | keep  |
      | true  |
      | false |

  Scenario: Try array element lookup remains non-foldable in percentile windows
    When query
      """
      SELECT v, percentile_approx(v, try_element_at(array(0.25D, 0.5D), 2)) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query error (?i)foldable

  Scenario: Try map element lookup remains non-foldable inside percentile accuracy
    When query
      """
      SELECT approx_percentile(v, 0.5D, try_element_at(map('accuracy', 100), 'accuracy')) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

  Scenario: Percentile foldability checks preserve ordinary element lookup and observation expressions
    When query
      """
      SELECT percentile_approx(try_element_at(array(v), 1), element_at(array(0.5D), 1)) AS p,
             try_element_at(array(7, 9), 1) AS picked
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p | picked |
      | 1 | 7      |

  Scenario: Typeof remains foldable over non-foldable percentile source functions
    When query
      """
      SELECT percentile_approx(v,
               array(IF(typeof(current_database()) = 'string', 0D, 0.5D), 1D),
               length(typeof(try_element_at(array(1), 1))) + 100) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p      |
      | [1, 2] |

  Scenario: Typeof remains foldable in percentile window parameters
    When query
      """
      SELECT v, approx_percentile(v,
               IF(typeof(current_user()) = 'string', 0.5D, 0.75D),
               length(typeof(try_element_at(map('x', 1), 'x'))) + 100) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p |
      | 1 | 1 |
      | 2 | 1 |


  Scenario Outline: Remaining non-foldable source wrappers are rejected before simplification
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage                                            | accuracy                                             | window  |
      | percentile_approx | IF(true, 0.5D, CAST(raise_error('unused') AS DOUBLE)) | 100                                                  |         |
      | approx_percentile | 0.5D                                                  | coalesce(100, CAST(raise_error('unused') AS BIGINT)) | OVER () |
      | percentile_approx | IF(assert_true(true) IS NULL, 0.5D, 1D)               | 100                                                  |         |
      | approx_percentile | 0.5D                                                  | IF(assert_true(true) IS NULL, 100, 200)              | OVER () |
      | percentile_approx | CAST(btrim(' 0.5 ') AS DOUBLE)                        | 100                                                  |         |
      | approx_percentile | 0.5D                                                  | length(BTRIM(' abc '))                               | OVER () |
      | percentile_approx | CAST(elt(1, '0.5', '1.0') AS DOUBLE)                  | 100                                                  |         |
      | approx_percentile | 0.5D                                                  | length(elt(1, 'abc', 'def'))                         | OVER () |

  @spark-4
  Scenario Outline: UTF8 validation remains non-foldable in percentile parameters
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage                         | accuracy                           | window  |
      | percentile_approx | IF(is_valid_utf8('abc'), 0.5D, 1D) | 100                                |         |
      | approx_percentile | 0.5D                               | IF(is_valid_utf8('abc'), 100, 200) | OVER () |

  Scenario: Foldable expressions sharing wrapper implementations remain valid
    When query
      """
      SELECT percentile_approx(v, 1D / 2D) AS division,
             percentile_approx(v, CAST(trim(' 0.5 ') AS DOUBLE)) AS trimmed,
             percentile_approx(v, element_at(array(0.5D), 1)) AS lookup,
             percentile_approx(v, IF(typeof(assert_true(true)) = 'void', 0.5D, 1D)) AS type_only
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | division | trimmed | lookup | type_only |
      | 1        | 1       | 1      | 1         |

  @sail-bug
  Scenario Outline: Unreachable literal division errors do not invalidate foldable percentile parameters
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT percentile_approx(v, <percentage>) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p |
      | 1 |

    Examples:
      | percentage              |
      | IF(true, 0.5D, 1D / 0D) |
      | coalesce(0.5D, 1D / 0D) |
