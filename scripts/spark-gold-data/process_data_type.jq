{
    "tests": [
        inputs
        | select(.kind == "data-type")
        | {"input": .data}
    ] | unique
}
