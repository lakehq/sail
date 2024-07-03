{
    "tests": [
        inputs
        | select(.kind == "table-schema")
        | {"input": .data, "exception": .exception}
    ] | unique_by(.input)
}
