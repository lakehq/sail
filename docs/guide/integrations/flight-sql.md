---
title: Arrow Flight SQL
rank: 3
---

# Arrow Flight SQL

Sail supports the [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) protocol, which allows clients to execute SQL queries and retrieve results using the high-performance Arrow Flight RPC framework.

You can start an Arrow Flight SQL server powered by Sail and then connect to it using any Arrow Flight SQL client.

## Starting the Server

### Using the Sail CLI

You can start the server using the `sail` command.

```bash
sail flight server --ip 127.0.0.1 --port 32010
```

### Using the Sail Python API

You can also start the server programmatically using the Sail Python API.

```python
from pysail.flight import FlightSqlServer

server = FlightSqlServer(ip="127.0.0.1", port=32010)
server.start(background=False)
```

## Connecting to the Server

Once the server is running, you can connect to it using any Arrow Flight SQL client. Here is an example using the `adbc-driver-flightsql` Python package.

```python
from adbc_driver_flightsql import dbapi

conn = dbapi.connect("grpc://127.0.0.1:32010")

cur = conn.cursor()
cur.execute("SELECT 1 + 1")
cur.fetchall()

conn.close()
```
