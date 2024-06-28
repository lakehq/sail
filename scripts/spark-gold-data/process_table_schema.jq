{
    "tests": [
        inputs
        | select(.kind == "table-schema")
        | {"input": .data}
    ] | unique
}
