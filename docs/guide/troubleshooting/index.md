---
title: Troubleshooting
rank: 12
---

# Troubleshooting

## System Time Zone Issue on Amazon Linux

When you run Sail on Amazon Linux, you may encounter the following error when creating a session:

> `failed to get system time zone: No such file or directory (os error 2)`

The reason is that `/etc/localtime` is supposed to be a symlink when retrieving the system time zone, but on Amazon Linux it is a regular file.
There is a [GitHub issue](https://github.com/amazonlinux/amazon-linux-2023/issues/526) for this problem, but it has not been resolved yet.

To work around this issue, you can run the following command on the host:

```bash
sudo timedatectl set-timezone UTC
```

## Protobuf Version Mismatch

When you run PySpark 4.0 in Spark Connect mode, you may see a lot of warnings like this:

> `UserWarning: Protobuf gencode version 5.28.3 is exactly one major version older than the runtime version 6.31.1 at spark/connect/base.proto. Please update the gencode to avoid compatibility violations in the next runtime release.`

This is possibly related to upstream issues such as [this](https://github.com/protocolbuffers/protobuf/issues/18096) and [this](https://github.com/grpc/grpc/issues/37609).
You can either ignore these warnings, or install specific versions of Protobuf and gRPC libraries along with PySpark:

```bash
pip install "pyspark[connect]==4.0.0" \
  "protobuf==5.28.3" \
  "grpcio==1.71.2" \
  "grpcio-status==1.71.2"
```

The `pyspark[connect]` package can also be replaced with
its equivalent package `pyspark-connect`, or the lightweight client package `pyspark-client`, depending on your use case.
