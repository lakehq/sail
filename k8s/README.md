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
helm dependency build . # OR helm dependency update .
helm install lakesail . # --debug

kubectl port-forward svc/lakesail 50051:50051
```

## Connect

```shell
kubectl port-forward svc/lakesail 50051:50051
```

## Misc

```shell
kubectl get pods
kubectl get all
export POD_NAME=$(kubectl get pods --namespace default -l "app.kubernetes.io/name=lakesail,app.kubernetes.io/instance=lakesail" -o jsonpath="{.items[0].metadata.name}")
kubectl describe pods/$POD_NAME
kubectl describe deployment lakesail
kubectl describe deployment lakesail-opentelemetry-operator
kubectl logs svc/lakesail --all-containers=true
kubectl logs svc/lakesail-opentelemetry-operator --all-containers=true
kubectl exec svc/lakesail -- printenv
```

## Teardown

```shell
helm uninstall lakesail
```

```shell
kind delete cluster
```