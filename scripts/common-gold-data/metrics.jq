.tests
| (map(if .exception == null and .output.success != null then 1 else 0 end) | add) as $tp
| (map(if .exception != null and .output.failure != null then 1 else 0 end) | add) as $tn
| (map(if .exception != null and .output.success != null then 1 else 0 end) | add) as $fp
| (map(if .exception == null and .output.failure != null then 1 else 0 end) | add) as $fn
| {"file": input_filename, "tp": $tp, "tn": $tn, "fp": $fp, "fn": $fn}
