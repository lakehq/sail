def classify:
    # This function classifies SQL expressions so that we can better organize test data.
    # New patterns can be added when a single test data file becomes too large.
    if test("^\\s*CASE\\b"; "im") then "case"
    elif test("^\\s*CAST\\b"; "im") then "cast"
    elif test("^\\s*CURRENT_[A-Z]+"; "im") then "current"
    elif test("^\\s*DATE\\b"; "im") then "date"
    elif test("^\\s*TIMESTAMP(_NTZ|_LTZ)?\\b"; "im") then "timestamp"
    elif test("^\\s*-?INTERVAL\\b"; "im") then "interval"
    elif test("^\\s*[0-9.+-][0-9A-Z.+-]+\\s*$"; "im") then "numeric"
    elif test("^['\"]"; "im") then "string"
    elif test("\\b[IR]?LIKE\\b"; "im") then "like"
    elif test("\\bOVER\\b"; "im") then "window"
    elif test("\\b1\\s+==\\s+1\\s+(AND|OR)\\s+2\\s+==\\s+2\\b"; "im") then "large"
    else "misc"
    end;

if $group == "" then
    [
        inputs
        | select(.kind == "expression")
        | . += {"group": (.data | classify)}
    ]
    | map(.group) | unique | .[]
else
    {
        "tests": [
            inputs
            | select(.kind == "expression")
            | . += {"group": (.data | classify)}
            | select(.group == $group)
            | {"input": .data, "exception": .exception}
        ] | unique_by(.input)
    }
end
