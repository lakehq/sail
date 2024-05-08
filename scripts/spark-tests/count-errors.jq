def truncate(n):
    if length > n then (.[:n] + "...") else . end;

def pad_left(n):
    if length < n then " " * (n - length) + . else . end;

def pad_right(n):
    if length < n then . + " " * (n - length) else . end;

def delta:
    if . > 0 then "(+\(.))" elif . == 0 then "" else "(\(.))" end;

def count_errors:
    # aggregate error count from an array of pytest events
    map(
        select(."$report_type" == "TestReport" and .outcome == "failed")
        | .longrepr.reprcrash.message
        | gsub("\\n"; " ")
        | gsub("\\t"; "    ")
        | gsub("^.*(SparkConnectGrpcException|_InactiveRpcError).*details\\s*=\\s*\"(?<error>.*)\"\\s*debug_error_string.*$"; "\(.error)")
        | gsub("^(\\w+\\.)*(?<error>[\\w]*Error):\\s*(?<reason>.*)$"; "\(.error): \(.reason)")
    )
  | group_by(.)
  | map({error: .[0], count: length});

($ARGS.named.baseline != null) as $has_baseline
| (
    $ARGS.named.baseline // []
    | count_errors
    | map({key: .error, value: {before: .count}})
    | from_entries
) * (
    [inputs]
    | count_errors
    | map({key: .error, value: {after: .count}})
    | from_entries
)
| to_entries
| map(.value.before //= 0)
| map(.value.after //= 0)
| map({error: .key, count: .value.after, diff: (.value.after - .value.before)})
| (map(.count) | add) as $total_count
| (map(.diff) | add) as $total_diff
| sort_by(-.count, .error)
| map({
    diff: (.diff | delta | pad_right(8)),
    count: (.count | tostring | pad_left(4)),
    error: (.error | truncate(100))
})
| . = [
    {
        diff: ($total_diff | delta | pad_right(8)),
        count: ($total_count | tostring | pad_left(4)),
        error: "Total"
    },
    {
        diff: ("-" * 8),
        count: ("-" * 4),
        error: ("-" * 106),
    }
] + .
| if $has_baseline then
    map("\(.diff) \(.count) \(.error)")
else
    map("\(.count) \(.error)")
end
| .[]
