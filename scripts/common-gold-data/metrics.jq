def file_group:
    if test("/crates/framework-spark-connect/") then "spark"
    else "unknown"
    end;

def file_simple_name:
    gsub("\t"; " ")
    | gsub("^.*/tests(/[^/]*)*/gold_data/"; "");

.tests
| (map(if .exception == null and .output.success != null then 1 else 0 end) | add) as $tp
| (map(if .exception != null and .output.failure != null then 1 else 0 end) | add) as $tn
| (map(if .exception != null and .output.success != null then 1 else 0 end) | add) as $fp
| (map(if .exception == null and .output.failure != null then 1 else 0 end) | add) as $fn
| {
    "group": (input_filename | file_group),
    "file": (input_filename | file_simple_name),
    "tp": $tp,
    "tn": $tn,
    "fp": $fp,
    "fn": $fn
}
