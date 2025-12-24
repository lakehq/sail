#!/bin/bash

set -euo 'pipefail'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <test-logs-after> <test-logs-before>"
    exit 1
fi

if [ -z "${TEST_REPORT_NAME:-}" ]; then
  echo "Missing environment variable: TEST_REPORT_NAME"
  exit 1
fi

after_dir="$1"
before_dir="$2"

project_path="$(git rev-parse --show-toplevel)"
tmp_dir="$(mktemp -d)"

trap clean_up EXIT

cd "${project_path}"

function clean_up() {
  echo "Cleaning up temporary files..." >&2
  rm -rvf "${tmp_dir}" >&2
}

function show_commit_info() {
  local name="$1"
  local dir="$2"
  # shellcheck disable=SC2016
  printf '| **%s** | `%s` | `%s` |\n' \
    "${name}" \
    "$(head -c 7 "${dir}/commit")" \
    "$(tr -d '\n' < "${dir}/ref")"
}

function write_test_summary() {
  local name="$1"
  local dir="$2"
  local output="$3"
  for f in "${dir}"/*.log; do
    printf '%s\t%s\t%s\n' \
      "${name}" \
      "$(basename "${f}" .log)" \
      "$(tail -n 1 "${f}" | tr -d '\n')" \
      >> "${output}"
  done
}

function show_test_summary() {
  local file="$1"
  sort -t$'\t' -k2,2 -k1,1 < "${file}" | awk -F$'\t' '
    BEGIN {
      printf "| Suite | Commit | Failed | Passed | Skipped | Warnings | Time (s) |\n"
      printf "| :--- | :--- | ---: | ---: | ---: | ---: | ---: |\n"
      suite = ""
    }
    {
      match($3, /[0-9]+ failed/)
      failed = substr($3, RSTART, RLENGTH - 7)
      match($3, /[0-9]+ passed/)
      passed = substr($3, RSTART, RLENGTH - 7)
      match($3, /[0-9]+ skipped/)
      skipped = substr($3, RSTART, RLENGTH - 8)
      match($3, /[0-9]+ warnings/)
      warnings = substr($3, RSTART, RLENGTH - 9)
      match($3, /in [0-9.]+s/)
      time = substr($3, RSTART + 3, RLENGTH - 4)

      printf "| %s | **%s** | %s | %s | %s | %s | %s |\n", ($2 == suite ? "" : sprintf("`%s`", $2)), $1, failed, passed, skipped, warnings, time
      suite = $2
    }
    END {
      printf "\n"
    }
  '
}

function show_code_block() {
  # Need to truncate the content if it's too long since a GitHub comment has a maximum length of 65536 characters.
  # Although, the max character limit may be based off of the gzipped size:
  #   https://github.com/orgs/community/discussions/41331#discussioncomment-9276173
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

printf '### %s\n\n' "${TEST_REPORT_NAME}"

printf '#### Commit Information\n\n'

printf "| Commit | Revision | Branch |\n"
printf "| :--- | :--- | :--- |\n"
show_commit_info 'After' "${after_dir}"
show_commit_info 'Before' "${before_dir}"

printf '\n'
printf '#### Test Summary\n\n'

write_test_summary 'After' "${after_dir}" "${tmp_dir}/summary.tsv"
write_test_summary 'Before' "${before_dir}" "${tmp_dir}/summary.tsv"

show_test_summary "${tmp_dir}/summary.tsv"

printf '\n'
printf '#### Test Details\n\n'

cat "${after_dir}"/*.jsonl > "${tmp_dir}/after.jsonl"
cat "${before_dir}"/*.jsonl > "${tmp_dir}/before.jsonl"

jq -r -f "${project_path}/scripts/spark-tests/count-errors.jq" \
  --slurpfile baseline "${tmp_dir}/before.jsonl" \
  "${tmp_dir}/after.jsonl" > "${tmp_dir}/errors.txt"

printf '<details>\n'
printf '<summary>Error Counts</summary>\n\n'
show_code_block "${tmp_dir}/errors.txt" "text" 40000
printf '</details>\n\n'

mkdir "${tmp_dir}/passed-tests"
jq -r -f "${project_path}/scripts/spark-tests/show-passed-tests.jq" \
  "${tmp_dir}/before.jsonl" > "${tmp_dir}/passed-tests/before.txt"
jq -r -f "${project_path}/scripts/spark-tests/show-passed-tests.jq" \
  "${tmp_dir}/after.jsonl" > "${tmp_dir}/passed-tests/after.txt"

pushd "${tmp_dir}/passed-tests" > /dev/null
diff -U 0 before.txt after.txt > ../passed-tests.diff || true
popd > /dev/null

printf '<details>\n'
printf '<summary>Passed Tests Diff</summary>\n\n'
show_code_block "${tmp_dir}/passed-tests.diff" "diff" 7500
printf '</details>\n'

jq -r -f "${project_path}/scripts/spark-tests/show-failed-tests.jq" \
  "${tmp_dir}/after.jsonl" > "${tmp_dir}/failed-tests.txt"

printf '<details>\n'
printf '<summary>Failed Tests</summary>\n\n'
show_code_block "${tmp_dir}/failed-tests.txt" "text" 40000
printf '</details>\n\n'
