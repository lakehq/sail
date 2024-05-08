#!/bin/bash

set -euo 'pipefail'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <head-test-logs> <base-test-logs>"
    exit 1
fi

head_dir="$1"
base_dir="$2"

project_path="$(dirname "$0")/../.."
tmp_dir="$(mktemp -d)"

trap clean_up EXIT

function clean_up() {
  echo "Cleaning up temporary files..." >&2
  rm -rvf "${tmp_dir}" >&2
}

function show_commit_info() {
  local name="$1"
  local dir="$2"
  # shellcheck disable=SC2016
  printf '* **%s**: `%s` (`%s`)\n' \
    "${name}" \
    "$(head -c 7 "${dir}/commit")" \
    "$(tr -d '\n' < "${dir}/ref")"
}

function show_test_summary() {
  local name="$1"
  local dir="$2"
  printf '* **%s**: %s\n' \
    "${name}" \
    "$(tail -n 1 "${dir}/test.log" | sed -e 's/^=* *//' -e 's/ *=*$//' | tr -d '\n')"
}

function show_code_block() {
  # A GitHub comment has a maximum length of 65536 characters.
  # So we need to truncate the content if it is too long.
  local file="$1"
  local language="$2"
  local limit="$3"
  if [ "$(wc -c < "${file}")" -eq 0 ]; then
      printf '(empty)\n\n'
      return
  fi
  printf '```%s\n' "${language}"
  # We remove '```' from the raw content to avoid issue in the Markdown code block.
  head -c "${limit}" "${file}" | sed -e 's/```//g'
  if [ "$(wc -c < "${file}")" -le "${limit}" ]; then
    printf '```\n\n'
  else
    printf '\n```\n\n(truncated)\n\n'
  fi
}

printf '### Spark Test Report\n\n'

printf '#### Commit Information\n\n'

show_commit_info 'Head' "$head_dir"
show_commit_info 'Base' "$base_dir"

printf '\n'
printf '#### Test Summary\n\n'

show_test_summary 'Head' "$head_dir"
show_test_summary 'Base' "$base_dir"

printf '\n'
printf '#### Test Details\n\n'

jq -r -f "${project_path}/scripts/spark-tests/count-errors.jq" \
  --slurpfile baseline "$base_dir/test.jsonl" \
  "$head_dir/test.jsonl" > "${tmp_dir}/errors.txt"

printf '<details>\n'
printf '<summary>Error Counts</summary>\n\n'
show_code_block "${tmp_dir}/errors.txt" "text" 40000
printf '</details>\n\n'

mkdir "${tmp_dir}/passed-tests"
jq -r -f "${project_path}/scripts/spark-tests/show-passed-tests.jq" \
  "$base_dir/test.jsonl" > "${tmp_dir}/passed-tests/base"
jq -r -f "${project_path}/scripts/spark-tests/show-passed-tests.jq" \
  "$head_dir/test.jsonl" > "${tmp_dir}/passed-tests/head"

pushd "${tmp_dir}/passed-tests" > /dev/null
diff -u base head > ../passed-tests.diff || true
popd > /dev/null

printf '<details>\n'
printf '<summary>Passed Tests Diff</summary>\n\n'
show_code_block "${tmp_dir}/passed-tests.diff" "diff" 10000
printf '</details>\n'
