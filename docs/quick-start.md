# LakeSail

## Quick Start

```bash
pip install framework # (LakeSail framework)
pip install pyspark # (if not already installed)
```

```Python
from datetime import datetime, date
from pyspark.sql import SparkSession, Row
from framework.spark.server import SparkConnectServer as LakeSailServer

server = LakeSailServer("127.0.0.1", 0)
server.start(background=True)
(host, port) = server.listening_address

spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
])
df
```

hatch env create notebook
hatch shell notebook
jupyter notebook
