[inputs]
| map(
    select(."$report_type" == "TestReport" and .outcome == "passed" and .when == "call")
)
| sort_by (.nodeid)
| .[]
| .nodeid
