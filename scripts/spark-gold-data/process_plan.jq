def classify:
    # This function classifies SQL statements so that we can better organize test data.
    # The patterns here are not exhaustive. New patterns can be added when more test cases are added in Spark,
    # or when a single test data file becomes too large.
    if test("^\\s*CREATE\\s+TABLE\\b"; "im") then "create_table"
    elif test("^\\s*ALTER\\s+TABLE\\b"; "im") then "alter_table"
    elif test("^\\s*ANALYZE\\s+TABLES?\\b"; "im") then "analyze_table"
    elif test("^\\s*REPLACE\\s+TABLE\\b"; "im") then "replace_table"
    elif test("^\\s*CREATE\\s+VIEW\\b"; "im") then "create_view"
    elif test("^\\s*ALTER\\s+VIEW\\b"; "im") then "alter_view"
    elif test("^\\s*DROP\\s+VIEW\\b"; "im") then "drop_view"
    elif test("^\\s*SHOW\\s+VIEWS\\b"; "im") then "show_views"
    elif test("^\\s*CACHE\\b"; "im") then "cache"
    elif test("^\\s*UNCACHE\\b"; "im") then "uncache"
    elif test("^\\s*CREATE\\s+INDEX\\b"; "im") then "create_index"
    elif test("^\\s*DROP\\s+INDEX\\b"; "im") then "drop_index"
    elif test("^\\s*DELETE\\s+FROM\\b"; "im") then "delete_from"
    elif test("^\\s*(DESC|DESCRIBE)\\b"; "im") then "describe"
    elif test("^\\s*INSERT\\s+INTO\\b"; "im") then "insert_into"
    elif test("^\\s*INSERT\\s+OVERWRITE\\b"; "im") then "insert_overwrite"
    elif test("^\\s*LOAD\\s+DATA\\b"; "im") then "load_data"
    elif test("^\\s*MERGE\\s+INTO\\b"; "im") then "merge_into"
    elif test("^\\s*UPDATE\\b"; "im") then "update"
    elif test("^\\s*EXPLAIN\\b"; "im") then "explain"
    elif test("^\\s*WITH\\b.*\\bSELECT\\b"; "im") then "with"
    elif test("^\\s*SELECT\\b.*/\\*\\+"; "im") then "hint"
    elif test("^\\s*SELECT\\b.*\\bJOIN\\b"; "im") then "join"
    elif test("^\\s*SELECT\\b.*\\b(ORDER|SORT)\\s+BY\\b"; "im") then "order_by"
    elif test("^\\s*SELECT\\b.*\\bGROUP\\s+BY\\b"; "im") then "group_by"
    elif test("^\\s*SELECT\\b.*\\b(UNION|EXCEPT|MINUS|INTERSECT)\\b"; "im") then "set_operation"
    elif test("^\\s*SELECT\\b"; "im") then "select"
    else "misc"
    end;

if $group == "" then
    [
        inputs
        | select(.kind == "plan")
        | . += {"group": (.data | classify)}
    ]
    | map(.group) | unique | .[]
else
    {
        "tests": [
            inputs
            | select(.kind == "plan")
            | . += {"group": (.data | classify)}
            | select(.group == $group)
            | {"input": .data, "exception": .exception}
        ] | unique_by(.input)
    }
end
