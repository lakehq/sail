{
    "tests": [
        inputs
        | select(.kind == "data-type")
        | {"input": .data, "exception": .exception}
    ] | unique_by(.input)
}
