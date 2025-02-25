#!/usr/bin/env python
import glob
import json
import re
from collections import Counter


def analyze_not_implemented_functions():
    directory_pattern = "crates/sail-spark-connect/tests/gold_data/function/*.json"
    json_files = glob.glob(directory_pattern)

    not_implemented_functions = []
    for json_file in json_files:
        with open(json_file) as file:
            data = json.load(file)

        for test in data.get("tests", []):
            output = test.get("output", {})

            if "failure" in output:
                failure_msg = output["failure"]
                match = re.search(r"not implemented: function: (\w+)", failure_msg)
                if match:
                    function_name = match[1]
                    not_implemented_functions.append(function_name)

    function_counts = Counter(not_implemented_functions)
    return sorted(function_counts.items(), key=lambda x: x[1], reverse=True)


def main():
    results = analyze_not_implemented_functions()
    print("Not implemented functions total count:")  # noqa: T201
    print("============================================")  # noqa: T201
    print(sum([count for _, count in results]))  # noqa: T201
    print("Not implemented functions (ordered by count):")  # noqa: T201
    print("============================================")  # noqa: T201
    for function, count in results:
        print(f"{function}: {count} occurrences")  # noqa: T201


if __name__ == "__main__":
    main()
