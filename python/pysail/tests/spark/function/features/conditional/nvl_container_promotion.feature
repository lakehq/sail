Feature: nvl over containers whose leaves need a promotion

  Rule: nvl of two containers never fails where Spark types them

    # `Nvl` is `Coalesce(Seq(left, right))` (`nullExpressions.scala:246`), and `coalesce` widens two
    # containers leaf by leaf (`TypeCoercionHelper.scala:141`, string promotion included,
    # `TypeCoercion.scala:168`), so every pair below is answered by Spark.
    Scenario Outline: nvl of <case> is answered with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | case                     | ansi  | fn     | left                                  | right                                 |
        | an int and a string list | false | nvl    | array(1)                              | array('a')                            |
        | an int and a string list | true  | ifnull | array(1)                              | array('a')                            |
        | a date and a string list | false | nvl    | array(DATE'2024-01-01')               | array('2024-01-02')                   |
        | a date and a string list | true  | ifnull | array(DATE'2024-01-01')               | array('2024-01-02')                   |

    # TODO: neither `coalesce` nor DataFusion's `nvl` types two maps whose values need a promotion
    #  (`Cannot automatically convert Map to Utf8View`), on `main` as well; Spark widens the values.
    @sail-bug
    Scenario Outline: nvl of <case> is answered with ANSI <ansi> despite the map values
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | case                     | ansi  | fn     | left                                  | right                                 |
        | a date and a string map  | false | nvl    | map('k', DATE'2024-01-01')            | map('k', '2024-01-02')                |
        | a date and a string map  | true  | ifnull | map('k', DATE'2024-01-01')            | map('k', '2024-01-02')                |

    # `findWiderTypeForTwo` recurses into a list and a struct, widening leaf by leaf and keeping the
    # left side's field names (`TypeCoercionHelper.scala:141`), so a list of structs whose leaves
    # widen and two structs whose names differ only by case both type.
    Scenario Outline: nvl of <case> is answered with ANSI <ansi> despite the structs
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | case                           | ansi  | fn     | left                        | right                        |
        | a list of widening structs     | false | nvl    | array(named_struct('a', 1)) | array(named_struct('a', 2L)) |
        | a list of widening structs     | true  | ifnull | array(named_struct('a', 1)) | array(named_struct('a', 2L)) |
        | structs differing only by case | false | nvl    | named_struct('a', 1)        | named_struct('A', 2)         |
        | structs differing only by case | true  | ifnull | named_struct('a', 1)        | named_struct('A', 2)         |

    # TODO: `coalesce` cannot widen these leaves inside a container, so `nvl` keeps DataFusion's
    #  STRING for them: the type is not Spark's and a STRING is an arithmetic operand.
    @sail-bug
    Scenario: nvl of an int and a string list is an ARRAY, refused as an arithmetic operand
      When query
        """
        SELECT 2 / nvl(array(1), array('a')) AS result
        """
      Then query error (?i)cannot resolve

  Rule: two structs type only when their field names match

    # `findTypeForComplex` pairs struct fields through `SQLConf.get.resolver` and returns None when a
    # pair of names does not match (`TypeCoercionHelper.scala:164-176`), so Spark refuses the pair
    # instead of renaming it; the resolver is case-insensitive by default, and an extra field is a
    # mismatch too.
    Scenario Outline: nvl of <case> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                            | ansi  | fn       | left                        | right                                |
        | structs with different names    | false | nvl      | named_struct('a', 1)        | named_struct('b', 2L)                |
        | structs with different names    | true  | ifnull   | named_struct('a', 1)        | named_struct('b', 2L)                |
        | a struct with an extra field    | false | nvl      | named_struct('a', 1)        | named_struct('a', 2L, 'b', 3)        |
        | lists of structs with different names | false | nvl | array(named_struct('a', 1))  | array(named_struct('b', 2L))         |
        | lists of structs with different names | true  | nvl | array(named_struct('a', 1))  | array(named_struct('b', 2L))         |
        | a struct beside an int          | false | nvl      | named_struct('a', 1)        | 2                                    |
