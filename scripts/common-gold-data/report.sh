#!/bin/bash

set -euo 'pipefail'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <head-repository> <base-repository>"
    exit 1
fi

head_dir="$1"
base_dir="$2"

project_path="$(git rev-parse --show-toplevel)"

function show_commit_info() {
  local name="$1"
  local dir="$2"
  # shellcheck disable=SC2016
  printf '* **%s**: `%s` (`%s`)\n' \
    "${name}" \
    "$(git -C "${dir}" rev-parse --abbrev-ref HEAD)" \
    "$(git -C "${dir}" rev-parse HEAD)"
}

function collect_metrics() {
  local name="$1"
  local dir="$2"

  find \
    "${dir}/crates/framework-spark-connect/tests/gold_data" \
    -name '*.json' \
    -exec \
    jq -f "${project_path}/scripts/common-gold-data/metrics.jq" '{}' '+' \
    | jq --arg name "${name}" '"\($name)\t\(.file)\t\(.tp)\t\(.tn)\t\(.fp)\t\(.fn)"' -r
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
      tp += $3
      tn += $4
      fp += $5
      fn += $6
    }
    END {
      total = tp + tn + fp + fn
      printf "| **%s** | %d | %d | %d | %d | %d |\n", name, tp, tn, fp, fn, total
    }
  '
}

function show_details() {
  sort -t$'\t' -k2,2 -k1,1r | awk -F$'\t' '
    BEGIN {
      printf "| Group | File | Commit | TP | TN | FP | FN | Total |\n"
      printf "| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: |\n"
      previous_group = ""
      previous_file = ""
    }
    {
      if (match($2, "\/crates\/framework-spark-connect\/tests\/gold_data\/")) {
        group = "spark"
        file = substr($2, RSTART + RLENGTH)
      } else {
        group = "unknown"
        file = "unknown"
      }
      printf "| %s ", (previous_group == group ? "" : sprintf("`%s`", group))
      printf "| %s ", (previous_file == file ? "" : sprintf("`%s`", file))
      printf "| **%s** | %d | %d | %d | %d ", $1, $3, $4, $5, $6
      printf "| %d |\n", $3 + $4 + $5 + $6
      previous_group = group
      previous_file = file
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

show_commit_info 'Head' "${head_dir}"
show_commit_info 'Base' "${base_dir}"

printf '\n'
printf '#### Summary\n\n'

show_summary_header
collect_metrics 'Head' "${head_dir}" | show_summary 'Head'
collect_metrics 'Base' "${base_dir}" | show_summary 'Base'

printf '\n'
printf '#### Details\n\n'

printf '<details>\n'
printf '<summary>Gold Data Metrics</summary>\n\n'

cat \
  <(collect_metrics 'Head' "${head_dir}") \
  <(collect_metrics 'Base' "${base_dir}") \
  | show_details

printf '</details>\n'
