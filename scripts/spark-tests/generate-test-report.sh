#!/bin/bash

set -euo 'pipefail'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <current> <baseline>"
    exit 1
fi

current="$1"
baseline="$2"

project_path="$(dirname "$0")/../.."
tmp_dir="$(mktemp -d)"

trap clean_up EXIT

function clean_up() {
  echo "Cleaning up temporary files..." >&2
  rm -rvf "${tmp_dir}" >&2
}

function show_commit_info() {
  name="$1"
  dir="$2"
  # shellcheck disable=SC2016
  printf '* **%s**: `%s` (`%s`)\n' \
    "${name}" \
    "$(head -c 7 "${dir}/commit")" \
    "$(tr -d '\n' < "${dir}/ref")"
}

function show_test_summary() {
  name="$1"
  dir="$2"
  printf '* **%s**: %s\n' \
    "${name}" \
    "$(tail -n 1 "${dir}/test.log" | sed -e 's/^=* *//' -e 's/ *=*$//' | tr -d '\n')"
}

function show_raw_text() {
  # A GitHub comment has a maximum length of 65536 characters.
  # So we need to truncate the text if it is too long.
  file="$1"
  limit="$2"
  # We remove '```' to avoid issue in the Markdown code block.
  head -c "${limit}" "${file}" | sed -e 's/```//g'
  if [ "$(wc -c < "${file}")" -gt "${limit}" ]; then
      printf '[truncated]\n'
  fi
}

printf '### Spark Test Report\n\n'

printf '#### Commit Information\n\n'

show_commit_info 'Current' "$current"
show_commit_info 'Baseline' "$baseline"

printf '\n'
printf '#### Test Summary\n\n'

show_test_summary 'Current' "$current"
show_test_summary 'Baseline' "$baseline"

printf '\n'
printf '#### Test Details\n\n'

jq -r -f "${project_path}/scripts/spark-tests/count-errors.jq" \
  --slurpfile baseline "$baseline/test.jsonl" \
  "$current/test.jsonl" > "${tmp_dir}/errors.txt"

printf '<details>\n'
printf '<summary>Error Counts</summary>\n\n'
printf '```text\n'
show_raw_text "${tmp_dir}/errors.txt" 40000
printf '```\n\n'
printf '</details>\n'

mkdir "${tmp_dir}/passed-tests"
jq -r -f "${project_path}/scripts/spark-tests/show-passed-tests.jq" \
  "$baseline/test.jsonl" > "${tmp_dir}/passed-tests/baseline"
jq -r -f "${project_path}/scripts/spark-tests/show-passed-tests.jq" \
  "$current/test.jsonl" > "${tmp_dir}/passed-tests/current"

pushd "${tmp_dir}/passed-tests" > /dev/null
diff -u baseline current > ../passed-tests.diff || true
popd > /dev/null

printf '\n'
printf '<details>\n'
printf '<summary>Passed Test Changes</summary>\n\n'
printf '```diff\n'
show_raw_text "${tmp_dir}/passed-tests.diff" 10000
printf '```\n\n'
printf '</details>\n'
