---
title: LangChain
rank: 6
---

# LangChain

[LangChain](https://www.langchain.com/) provides tools for building applications and agents with language models.
The [`langchain-sail`](https://github.com/lakehq/langchain-sail) package connects LangChain agents to Sail through Spark Connect.
Agents can list tables, inspect schemas and sample rows, check SQL, and execute queries.

## Overview

`SailSQLToolkit` contains four tools for table discovery, schema inspection, query checking, and query execution.
Each tool uses a `SailSQL` connection to Sail over Spark Connect.

## Installation

Install `langchain-sail` with the lightweight Spark Connect client.
The following commands also install LangChain and the OpenAI integration used in this guide.

::: code-group

```bash [pip]
pip install "langchain-sail[spark]" langchain langchain-openai
```

```bash [uv]
uv add "langchain-sail[spark]" langchain langchain-openai
```

:::

The `spark` extra installs `pyspark-client==4.2.0` without the JVM components included in the full PySpark distribution.

Applications that require the full PySpark distribution should install it separately instead of using the `spark` extra.

::: code-group

```bash [pip]
pip install langchain-sail "pyspark[connect]==4.2.0" langchain langchain-openai
```

```bash [uv]
uv add langchain-sail "pyspark[connect]==4.2.0" langchain langchain-openai
```

:::

To run a local Sail server for development or testing, also install `pysail`.

::: code-group

```bash [pip]
pip install pysail
```

```bash [uv]
uv add pysail
```

:::

## Starting or Connecting to Sail

### Connecting to an Existing Sail Server

Connect a Spark session to the Sail server.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://your-sail-host:50051").getOrCreate()
```

See [Deployment](/guide/deployment/) for ways to run Sail outside the application.

### Starting Sail Locally

For local development, start Sail in the application process and connect a Spark session to its listening address.

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

server = SparkConnectServer()
server.start()
_, port = server.listening_address

spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
```

Keep the server running while the integration is in use.
Stop the Spark session with `spark.stop()` before calling `server.stop()`.

### Preparing the Example Data

A local Sail server starts without tables.
Create two temporary views for the remaining examples.

```python
spark.createDataFrame(
    [(1, "Alice"), (2, "Bob")],
    ["customer_id", "name"],
).createOrReplaceTempView("customers")

spark.createDataFrame(
    [(101, 1), (102, 1), (103, 2)],
    ["order_id", "customer_id"],
).createOrReplaceTempView("orders")
```

Temporary views exist only for the current Spark Connect session and do not modify persistent data.

## Creating the Toolkit

Set the OpenAI API key in your environment.

```bash
export OPENAI_API_KEY="your-api-key"
```

`SailSQLToolkit` requires a tool-capable chat model and a `SailSQL` instance.
The query-checking tool uses the model.
The `spark` variable refers to the Spark Connect session created above.

```python
from langchain.chat_models import init_chat_model
from langchain_sail import SailSQL, SailSQLToolkit

model = init_chat_model("openai:gpt-5.5")

sail_sql = SailSQL(
    spark_session=spark,
    sample_rows_in_table_info=3,
)

toolkit = SailSQLToolkit(db=sail_sql, llm=model)
tools = toolkit.get_tools()

print([tool.name for tool in tools])
```

Output:

```text
['query_sql_db', 'schema_sql_db', 'list_tables_sql_db', 'query_checker_sql_db']
```

Pass existing table names through `include_tables` or `ignore_tables` to control which tables the toolkit can access.
Use `sample_rows_in_table_info` to set the number of sample rows included with table schema information.

## Available Tools

`SailSQLToolkit.get_tools()` returns the following tools:

| Tool                   | Input                                 | Output                                             | Purpose                                                  |
| ---------------------- | ------------------------------------- | -------------------------------------------------- | -------------------------------------------------------- |
| `list_tables_sql_db`   | An empty string                       | A comma-separated list of table names              | Lists the tables available to the toolkit                |
| `schema_sql_db`        | A comma-separated list of table names | Table schemas and sample rows, or an error message | Inspects tables before constructing a query              |
| `query_checker_sql_db` | A SQL query                           | The checked SQL query                              | Uses the language model to check for common SQL mistakes |
| `query_sql_db`         | A SQL query                           | Query results or an error message                  | Executes the query against Sail                          |

## Invoking Tools Directly

Select tools by name to invoke them without an agent.
These examples use the `customers` and `orders` temporary views created above.

```python
tools_by_name = {tool.name: tool for tool in tools}
```

### Listing Tables

Invoke the table-listing tool with an empty string.

```python
tables = tools_by_name["list_tables_sql_db"].invoke("")
print(tables)
```

Output:

```text
customers, orders
```

### Inspecting Table Schemas

Provide table names as a comma-separated string.

```python
schema = tools_by_name["schema_sql_db"].invoke("customers, orders")
print(schema)
```

Output:

```text
CREATE TABLE customers (customer_id bigint, name string);

/*
3 rows from customers table:
customer_id	name
1	Alice
2	Bob
*/

CREATE TABLE orders (order_id bigint, customer_id bigint);

/*
3 rows from orders table:
order_id	customer_id
101	1
102	1
103	2
*/
```

### Checking a Query

Check model-generated SQL before execution.

```python
query = """SELECT c.name, COUNT(o.order_id) AS order_count
FROM customers AS c
LEFT JOIN orders AS o ON c.customer_id = o.customer_id
GROUP BY c.name
ORDER BY order_count DESC"""

checked_query = tools_by_name["query_checker_sql_db"].invoke(query)
print(checked_query)
```

Output:

```sql
SELECT c.name, COUNT(o.order_id) AS order_count
FROM customers AS c
LEFT JOIN orders AS o ON c.customer_id = o.customer_id
GROUP BY c.name
ORDER BY order_count DESC
```

### Executing the Query

Pass the checked SQL to the query tool.

```python
result = tools_by_name["query_sql_db"].invoke(checked_query)
print(result)
```

Output:

```text
[('Alice', '2'), ('Bob', '1')]
```

## Using the Toolkit with an Agent

LangChain's `create_agent()` API combines the model, tools, and a system prompt.
The prompt requires the agent to inspect available data and construct read-only queries.

```python
from langchain.agents import create_agent

system_prompt = """
You are an agent designed to interact with Sail using Spark SQL.
Given a question, create a syntactically correct Spark SQL query, execute it,
and use the result to answer the question. Unless the user requests a specific
number of results, limit the query to at most 5 rows.

Only select the columns needed to answer the question. Do not issue statements
that modify data or schema, including INSERT, UPDATE, DELETE, DROP, or ALTER.

Always list the available tables first. Then inspect the schemas of the relevant
tables. Always check a query with query_checker_sql_db before executing it with
query_sql_db. If query execution returns an error, revise the query and try again.
"""

agent = create_agent(
    model,
    tools,
    system_prompt=system_prompt,
)
```

Send a user message to the agent.

```python
response = agent.invoke(
    {
        "messages": [
            {
                "role": "user",
                "content": "Which customer has placed the most orders?",
            }
        ]
    }
)

print(response["messages"][-1].text)
```

Output:

```text
Alice has placed the most orders, with 2 orders.
```

`create_sail_sql_agent()` remains available for applications that use LangChain's classic `AgentExecutor` API.
Use `create_agent()` for new applications.

## Safety Considerations

::: warning
`query_sql_db` can execute arbitrary model-generated SQL against the connected Sail environment.
The query checker asks a language model to review SQL for common mistakes, but it does not enforce access control and is not a security boundary.
:::

When using the toolkit:

- Use a read-only, least-privilege role.
- Limit access to required catalogs and schemas. Use `include_tables` to allowlist tables where possible.
- Set server-side statement timeouts, resource limits, and concurrency limits.
- Log generated queries and monitor their resource usage.
- Require human approval before `query_sql_db` executes queries in sensitive environments.

Client-side timeouts do not guarantee that a running query is cancelled on the server.

## API Reference and Links

`langchain-sail` exports `SailSQL`, `SailSQLToolkit`, the individual Sail SQL tool classes, and `create_sail_sql_agent()`.

- [`langchain-sail` source code](https://github.com/lakehq/langchain-sail)
- [`langchain-sail` on PyPI](https://pypi.org/project/langchain-sail/)
- [Sail Python API reference](/reference/python/)
- [Deploying Sail](/guide/deployment/)
- [LangChain tools documentation](https://docs.langchain.com/oss/python/langchain/tools)
- [LangChain agents documentation](https://docs.langchain.com/oss/python/langchain/agents)
- [LangChain SQL agent guide](https://docs.langchain.com/oss/python/langchain/sql-agent)
- [LangChain human-in-the-loop documentation](https://docs.langchain.com/oss/python/langchain/human-in-the-loop)
