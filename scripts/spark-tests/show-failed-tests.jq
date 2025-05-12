[inputs]
| map(
    select(."$report_type" == "TestReport" and .outcome == "failed" and .when == "call")
)
| sort_by (.nodeid)
| .[]
| .nodeid
