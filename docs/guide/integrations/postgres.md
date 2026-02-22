---
title: PostgreSQL
rank: 2
---

# PostgreSQL

::: warning Deprecated
The PostgreSQL-specific datasource (`format("postgres")`) has been removed and replaced by the
generic [JDBC datasource](./jdbc.md). See the [migration guide](#migrating-to-the-jdbc-datasource)
below.
:::

## Migrating to the JDBC Datasource

The legacy PostgreSQL datasource has been replaced by the generic JDBC datasource, which supports
PostgreSQL and other databases through a unified API.

**Installation:**

- Old: `pip install "pysail[postgres]"`
- New: `pip install "pysail[jdbc]"`

**Read API:**

- Old: `spark.read.format("postgres")...`
- New: `spark.read.format("jdbc")...`

All other options (`dbtable`, `user`, `password`, partitioning options, etc.) remain the same.
The JDBC URL format is unchanged (e.g. `jdbc:postgresql://localhost:5432/mydb`).

**Example migration:**

```python
# Before
from pysail.datasources.postgres import PostgresDataSource

spark.dataSource.register(PostgresDataSource)

df = spark.read.format("postgres").options(
    url="jdbc:postgresql://localhost:5432/mydb",
    user="myuser",
    password="mypassword",
    dbtable="users",
).load()

# After
from pysail.datasources.jdbc import JdbcDataSource

spark.dataSource.register(JdbcDataSource)

df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://localhost:5432/mydb",
    user="myuser",
    password="mypassword",
    dbtable="users",
).load()
```

See the [JDBC datasource documentation](./jdbc.md) for full details.

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
