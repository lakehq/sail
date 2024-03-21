# LakeSail Helm Chart

## Prerequisites

```shell
# https://kind.sigs.k8s.io/docs/user/quick-start/#installation
kind create cluster
```

## Build

```shell
docker/build-lakesail-docker.sh
```

## Deploy

```shell
kind load docker-image lakesail-framework:latest

cd k8s/lakesail
helm dependency build .
helm install lakesail .
```

## Connect

```shell

```

## Teardown

```shell
helm uninstall lakesail
```