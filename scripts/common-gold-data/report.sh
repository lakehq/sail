#!/bin/bash

set -euo 'pipefail'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <repository-after> <repository-before>"
    exit 1
fi

after_dir="$1"
before_dir="$2"

project_path="$(git rev-parse --show-toplevel)"

cd "${project_path}"

function show_commit_info() {
  local name="$1"
  local dir="$2"
  local ref="$3"
  # shellcheck disable=SC2016
  printf '* **%s**: `%s` (`%s`)\n' \
    "${name}" \
    "$(git -C "${dir}" rev-parse --short=7 HEAD)" \
    "${ref}"
}

function collect_metrics() {
  local name="$1"
  local dir="$2"

  find \
    "${dir}/crates/sail-spark-connect/tests/gold_data" \
    -name '*.json' \
    -exec \
    jq -f "${project_path}/scripts/common-gold-data/metrics.jq" '{}' '+' \
    | jq -r --arg name "${name}" \
    '"\($name)\t\(.group)\t\(.file)\t\(.tp)\t\(.tn)\t\(.fp)\t\(.fn)"'
}

function show_summary_header() {
  printf "| Commit | TP | TN | FP | FN | Total |\n"
  printf "| :--- | ---: | ---: | ---: | ---: | ---: |\n"
}

function show_summary() {
  local name="$1"

  awk -v name="${name}" -F$'\t' '
    BEGIN {
      tp = 0
      tn = 0
      fp = 0
      fn = 0
    }
    $1 == name {
      tp += $4
      tn += $5
      fp += $6
      fn += $7
    }
    END {
      total = tp + tn + fp + fn
      printf "| **%s** | %d | %d | %d | %d | %d |\n", name, tp, tn, fp, fn, total
    }
  '
}

function show_details() {
  sort -t$'\t' -k2,3 -k1,1 | awk -F$'\t' '
    BEGIN {
      printf "| Group | File | Commit | TP | TN | FP | FN | Total |\n"
      printf "| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: |\n"
      group = ""
      file = ""
    }
    {
      printf "| %s ", (group == $2 ? "" : sprintf("`%s`", $2))
      printf "| %s ", (file == $3 ? "" : sprintf("`%s`", $3))
      printf "| **%s** | %d | %d | %d | %d ", $1, $4, $5, $6, $7
      printf "| %d |\n", $4 + $5 + $6 + $7
      group = $2
      file = $3
    }
    END {
      printf "\n"
    }
  '
}

printf '### Gold Data Report\n\n'

printf '<details>\n'
printf '<summary>Notes</summary>\n\n'

printf '1. The tables below show the number of true positives (TP), true negatives (TN), false positives (FP), and false negatives (FN) in gold data input processing.\n'
printf '2. A positive input is a valid test case, while a negative input is a test case that is expected to fail.\n'

printf '\n'
printf '</details>\n\n'

printf '#### Commit Information\n\n'

show_commit_info 'After' "${after_dir}" "${REPORT_GIT_REF_AFTER-HEAD}"
show_commit_info 'Before' "${before_dir}" "${REPORT_GIT_REF_BEFORE-HEAD}"

printf '\n'
printf '#### Summary\n\n'

show_summary_header
collect_metrics 'After' "${after_dir}" | show_summary 'After'
collect_metrics 'Before' "${before_dir}" | show_summary 'Before'

printf '\n'
printf '#### Details\n\n'

printf '<details>\n'
printf '<summary>Gold Data Metrics</summary>\n\n'

cat \
  <(collect_metrics 'After' "${after_dir}") \
  <(collect_metrics 'Before' "${before_dir}") \
  | show_details

printf '</details>\n'
