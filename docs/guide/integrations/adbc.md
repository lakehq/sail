---
title: ADBC
rank: 4
---

# ADBC

[ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) is a vendor-neutral API for accessing databases using Apache Arrow.
Sail supports ADBC through its [Arrow Flight SQL](/guide/integrations/flight-sql) server.
You connect to Sail using the ADBC Flight SQL driver, which speaks the Arrow Flight SQL protocol over gRPC.

## Installation

### Driver

Install the Arrow Flight SQL ADBC driver with [dbc](https://docs.columnar.tech/dbc/):

```bash
dbc install flightsql
```

### Client Libraries

In addition to the ADBC driver, each client needs its language-specific ADBC bindings.
Install the bindings for your language:

::: code-group

```bash [C++]
# Install the ADBC driver manager and Arrow from conda-forge.
# This example uses pixi; you can also use conda, mamba, or a system package manager.
pixi add libadbc-driver-manager libarrow cmake compilers
```

```bash [C#]
dotnet add package Apache.Arrow.Adbc
```

```bash [Go]
go get github.com/apache/arrow-adbc/go/adbc
```

```xml [Java]
<!-- Add these dependencies to your pom.xml -->
<dependencies>
  <dependency>
    <groupId>org.apache.arrow.adbc</groupId>
    <artifactId>adbc-core</artifactId>
  </dependency>
  <dependency>
    <groupId>org.apache.arrow.adbc</groupId>
    <artifactId>adbc-driver-manager</artifactId>
  </dependency>
  <dependency>
    <groupId>org.apache.arrow.adbc</groupId>
    <artifactId>adbc-driver-jni</artifactId>
  </dependency>
  <dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-core</artifactId>
  </dependency>
  <dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-netty</artifactId>
  </dependency>
  <dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
  </dependency>
</dependencies>
```

```bash [JavaScript]
npm install @apache-arrow/adbc-driver-manager apache-arrow
```

```kotlin [Kotlin]
// Add these dependencies to your build.gradle.kts
dependencies {
    implementation("org.apache.arrow.adbc:adbc-core")
    implementation("org.apache.arrow.adbc:adbc-driver-manager")
    implementation("org.apache.arrow.adbc:adbc-driver-jni")
    implementation("org.apache.arrow:arrow-memory-core")
    implementation("org.apache.arrow:arrow-memory-netty")
    implementation("org.apache.arrow:arrow-vector")
}
```

```bash [Python]
pip install adbc-driver-manager pyarrow
```

```r [R]
install.packages(c("adbcdrivermanager", "arrow", "tibble"))
```

```bash [Ruby]
gem install red-adbc
```

```bash [Rust]
cargo add adbc_core adbc_driver_manager arrow arrow-array
```

:::


## Connecting to Sail

The examples below all run `SELECT 1 + 1 AS result` against a server at `grpc://localhost:32010`.
The `uri` scheme should be `grpc://` for plain connections or `grpc+tls://` when the server is configured with TLS.
Adjust the host, port, and query for your environment.

::: code-group

```cpp [C++]
#include <cstdlib>
#include <cstring>
#include <iostream>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

// Error-checking helper for ADBC calls.
// Assumes that there is an AdbcError named `error` in scope.
#define CHECK_ADBC(EXPR)                                                       \
  if (AdbcStatusCode status = (EXPR); status != ADBC_STATUS_OK) {              \
    if (error.message != nullptr) {                                            \
      std::cerr << error.message << std::endl;                                 \
    }                                                                          \
    return EXIT_FAILURE;                                                       \
  }

int main() {
  AdbcError error = {};

  AdbcDatabase database = {};
  CHECK_ADBC(AdbcDatabaseNew(&database, &error));
  CHECK_ADBC(AdbcDatabaseSetOption(&database, "driver", "flightsql", &error));
  CHECK_ADBC(AdbcDatabaseSetOption(&database, "uri",
                                   "grpc://localhost:32010", &error));
  CHECK_ADBC(AdbcDriverManagerDatabaseSetLoadFlags(
      &database, ADBC_LOAD_FLAG_DEFAULT, &error));
  CHECK_ADBC(AdbcDatabaseInit(&database, &error));

  AdbcConnection connection = {};
  CHECK_ADBC(AdbcConnectionNew(&connection, &error));
  CHECK_ADBC(AdbcConnectionInit(&connection, &database, &error));

  AdbcStatement statement = {};
  CHECK_ADBC(AdbcStatementNew(&connection, &statement, &error));

  struct ArrowArrayStream stream = {};
  int64_t rows_affected = -1;
  CHECK_ADBC(AdbcStatementSetSqlQuery(
      &statement, "SELECT 1 + 1 AS result", &error));
  CHECK_ADBC(
      AdbcStatementExecuteQuery(&statement, &stream, &rows_affected, &error));

  auto reader = arrow::ImportRecordBatchReader(&stream).ValueOrDie();
  while (auto batch = reader->Next().ValueOrDie()) {
    std::cout << batch->ToString() << std::endl;
  }

  CHECK_ADBC(AdbcStatementRelease(&statement, &error));
  CHECK_ADBC(AdbcConnectionRelease(&connection, &error));
  CHECK_ADBC(AdbcDatabaseRelease(&database, &error));

  return EXIT_SUCCESS;
}
```

```csharp [C#]
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.DriverManager;
using Apache.Arrow.Ipc;

using AdbcDriver driver = AdbcDriverManager.FindLoadDriver(
    "flightsql",
    loadOptions: AdbcLoadFlags.Default);

using AdbcDatabase db = driver.Open(new Dictionary<string, string>
{
    ["uri"] = "grpc://localhost:32010",
});

using AdbcConnection conn = db.Connect(null);
using AdbcStatement stmt = conn.CreateStatement();

stmt.SqlQuery = "SELECT 1 + 1 AS result";

QueryResult result = stmt.ExecuteQuery();
using IArrowArrayStream stream = result.Stream!;

while (await stream.ReadNextRecordBatchAsync() is { } batch)
{
    using (batch)
    {
        // BatchPrinter is a small helper that formats a record batch for display.
        // See the linked quickstart repository for its source.
        BatchPrinter.Print(batch);
    }
}
```

```go [Go]
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

func main() {
	var drv drivermgr.Driver

	db, err := drv.NewDatabase(map[string]string{
		"driver": "flightsql",
		"uri":    "grpc://localhost:32010",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT 1 + 1 AS result"); err != nil {
		log.Fatal(err)
	}

	stream, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Release()

	for stream.Next() {
		fmt.Println(stream.RecordBatch())
	}
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}
```

```java [Java]
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class Example {
  private static final String DRIVER_FACTORY =
      "org.apache.arrow.adbc.driver.jni.JniDriverFactory";

  public static void main(String[] args) throws Exception {
    Map<String, Object> params = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(params, "flightsql");
    params.put("uri", "grpc://localhost:32010");

    try (BufferAllocator allocator = new RootAllocator();
        AdbcDatabase db =
            AdbcDriverManager.getInstance().connect(DRIVER_FACTORY, allocator, params);
        AdbcConnection conn = db.connect();
        AdbcStatement stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT 1 + 1 AS result");
      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        ArrowReader reader = result.getReader();
        while (reader.loadNextBatch()) {
          System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
        }
      }
    }
  }
}
```

```javascript [JavaScript]
import { AdbcDatabase } from "@apache-arrow/adbc-driver-manager";

const db = new AdbcDatabase({
  driver: "flightsql",
  databaseOptions: {
    uri: "grpc://localhost:32010",
  },
});

let conn;
try {
  conn = await db.connect();
  const table = await conn.query("SELECT 1 + 1 AS result");
  console.log(table.toString());
} finally {
  await conn?.close();
  await db.close();
}
```

```kotlin [Kotlin]
import org.apache.arrow.adbc.driver.jni.JniDriver
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator

private const val DRIVER_FACTORY = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"

fun main() {
    val params = mutableMapOf<String, Any>()
    JniDriver.PARAM_DRIVER.set(params, "flightsql")
    params["uri"] = "grpc://localhost:32010"

    RootAllocator().use { allocator ->
        AdbcDriverManager.getInstance().connect(DRIVER_FACTORY, allocator, params).use { db ->
            db.connect().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.setSqlQuery("SELECT 1 + 1 AS result")
                    stmt.executeQuery().use { result ->
                        val reader = result.reader
                        while (reader.loadNextBatch()) {
                            println(reader.vectorSchemaRoot.contentToTSVString())
                        }
                    }
                }
            }
        }
    }
}
```

```python [Python]
from adbc_driver_manager import dbapi

with (
    dbapi.connect(
        driver="flightsql",
        db_kwargs={
            "uri": "grpc://localhost:32010",
        },
    ) as con,
    con.cursor() as cursor,
):
    cursor.execute("SELECT 1 + 1 AS result")
    table = cursor.fetch_arrow_table()

print(table)
```

```r [R]
library(adbcdrivermanager)

drv <- adbc_driver("flightsql")

db <- adbc_database_init(
  drv,
  uri = "grpc://localhost:32010"
)

con <- adbc_connection_init(db)

con |>
  read_adbc("SELECT 1 + 1 AS result") |>
  tibble::as_tibble()
```

```ruby [Ruby]
require "adbc"

database = ADBC::Database.new

begin
  database.set_option("driver", "flightsql")
  database.set_option("uri", "grpc://localhost:32010")
  database.set_load_flags(ADBC::LoadFlags::DEFAULT)
  database.init

  database.connect do |connection|
    table, = connection.query("SELECT 1 + 1 AS result")
    puts(table)
  end
ensure
  database.release
end
```

```rust [Rust]
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, LOAD_FLAG_DEFAULT, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow::util::pretty;
use arrow_array::RecordBatch;

fn main() {
    let mut driver = ManagedDriver::load_from_name(
        "flightsql",
        None,
        AdbcVersion::default(),
        LOAD_FLAG_DEFAULT,
        None,
    )
    .expect("Failed to load driver");

    let opts = [(OptionDatabase::Uri, "grpc://localhost:32010".into())];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database handle");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query("SELECT 1 + 1 AS result").unwrap();
    let reader = statement.execute().unwrap();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();

    pretty::print_batches(&batches).expect("Failed to print batches");
}
```

:::

## Links

- [ADBC documentation](https://arrow.apache.org/adbc/current/index.html)
- [Flight SQL driver documentation](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
- [Sail ADBC quickstarts](https://github.com/columnar-tech/adbc-quickstarts/tree/by-database/sail)
