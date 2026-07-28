---
title: dbt
rank: 5
---

# dbt

[`dbt-sail`](https://github.com/lakehq/dbt-sail) is the LakeSail-maintained dbt
adapter for Sail. It is a thin wrapper around `dbt-spark` that connects to Sail
over Spark Connect, so any existing dbt-spark project runs on Sail with only a
profile change.

## Installation

```bash
pip install dbt-sail
```

`dbt-sail` pulls in `dbt-spark[session]` and `pysail` as dependencies. You do
not need to install Sail separately.

## Configuration

Run `dbt init` to generate a profile interactively. It will prompt for the
fields below.

| Field                    | Required     | Default     | Description                                                                                                            |
| ------------------------ | ------------ | ----------- | ---------------------------------------------------------------------------------------------------------------------- |
| `type`                   | yes          |             | Must be `sail`.                                                                                                        |
| `mode`                   | yes          | `embedded`  | `embedded` starts a Sail Spark Connect server in the dbt process. `remote` connects to an already-running Sail server. |
| `schema`                 | yes          |             | Default schema dbt builds objects in.                                                                                  |
| `host`                   | for `remote` | `127.0.0.1` | Hostname of the Sail server. In `embedded` mode this is the bind address of the in-process server.                     |
| `port`                   | no           | `50051`     | Port of the Sail server. In `embedded` mode an unused port is chosen automatically.                                    |
| `database`               | no           | `null`      | Must be omitted or equal to `schema`. Sail, like Spark, treats database and schema as the same thing.                  |
| `server_side_parameters` | no           | `{}`        | Map of string-valued options forwarded to the Spark Connect session.                                                   |
| `threads`                | no           | `1`         | Standard dbt option.                                                                                                   |

## Links

- [`dbt-sail` on GitHub](https://github.com/lakehq/dbt-sail)
- [`dbt-sail` on PyPI](https://pypi.org/project/dbt-sail/)
- [dbt documentation](https://docs.getdbt.com/)
- [Getting Started with Sail](/introduction/getting-started/)
