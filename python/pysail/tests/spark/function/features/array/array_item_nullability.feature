Feature: array(...)[index] is as nullable as the element it picks

  # Spark's `SimplifyExtractValueOps` rewrites `array(a, b, c)[1]` to `b` when the index is a
  # literal inside the list, so the other elements are never evaluated and the result is as
  # nullable as `b` alone. Values and nullability measured on the Spark JVM, ANSI on and off.
  Rule: A literal in-range index takes the nullability of the element it picks

    Scenario Outline: array element nullability with ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS r
        """
      Then query schema
        """
        root
         |-- r: <type> (nullable = <nullable>)
        """

      Examples:
        | ansi  | case              | expr                 | type    | nullable |
        | true  | non-null first    | array(1, 2, 3)[0]    | integer | false    |
        | true  | null before       | array(1, NULL, 3)[2] | integer | false    |
        | true  | the null itself   | array(1, NULL, 3)[1] | integer | true     |
        | true  | widened to bigint | array(1, 2L)[0]      | long    | false    |
        | true  | widened to double | array(1, 2.5D)[0]    | double  | false    |
        | true  | strings           | array('a', 'b')[1]   | string  | false    |
        | false | non-null first    | array(1, 2, 3)[0]    | integer | false    |
        | false | null before       | array(1, NULL, 3)[2] | integer | false    |
        | false | the null itself   | array(1, NULL, 3)[1] | integer | true     |
        | false | widened to bigint | array(1, 2L)[0]      | long    | false    |
        | false | widened to double | array(1, 2.5D)[0]    | double  | false    |
        | false | strings           | array('a', 'b')[1]   | string  | false    |

    Scenario Outline: array element with a nullable column with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT array(c, 2)[<index>] AS r FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: integer (nullable = <nullable>)
        """

      Examples:
        | ansi  | index | nullable |
        | true  | 0     | true     |
        | true  | 1     | false    |
        | false | 0     | true     |
        | false | 1     | false    |
