# Kubernetes Deployment

## Basic Deployment (No Volumes)

```bash
kubectl apply -f k8s/sail.yaml
```

### Clean Up
```bash
kubectl delete -f k8s/sail.yaml
```

## Deployment with Local Volume (hostPath) using Kustomize

```bash
kubectl apply -k k8s/
```

This mounts `/tmp/sail` from the host node to `/tmp/sail` in the container.

### Clean Up

```bash
kubectl delete -k k8s/
```


## Port Forwarding
```bash
kubectl -n sail port-forward service/sail-spark-server 50051:50051
```
